import * as net from "net";
import * as fs from "fs";

interface StorageItem {
    value: string;
    expiryTime?: number;  
}

interface RedisConfig {
    dir: string;
    dbfilename: string;
    replicaof?: {
        host: string;
        port: number;
    };
}

class RDBParser {
    private data: Buffer;
    private position: number = 0;

    constructor(data: Buffer) {
        this.data = data;
    }

    parse(): Map<string, StorageItem> {
        const storage = new Map<string, StorageItem>();
        
        // Validate header (REDIS0011)
        const header = this.data.slice(0, 9).toString();
        if (header !== 'REDIS0011') {
            throw new Error('Invalid RDB file format');
        }
        this.position = 9;

        // Skip metadata section
        while (this.position < this.data.length) {
            const byte = this.data[this.position];
            if (byte === 0xFE || byte === 0xFF) { // Database selector or EOF
                break;
            } else if (byte === 0xFA || byte === 0x64) { // Metadata or AUX field
                this.position++;
                this.skipString(); // Skip key
                this.skipString(); // Skip value
                continue;
            }
            // Skip any other bytes in the metadata section
            this.position++;
        }

        // Parse database section
        if (this.position < this.data.length && this.data[this.position] === 0xFE) {
            this.position++; // Skip FE byte
            this.skipSize(); // Skip database number
            
            if (this.data[this.position] === 0xFB) {
                this.position++; // Skip FB byte
                this.skipSize(); // Skip hash table size
                this.skipSize(); // Skip expires hash table size
            }

            // Read key-value pairs
            while (this.position < this.data.length) {
                const byte = this.data[this.position];
                
                if (byte === 0xFF) { // End of RDB file
                    break;
                }

                let expiryTime: number | undefined;

                // Check for expiry
                if (byte === 0xFC || byte === 0xFD) {
                    this.position++;
                    if (byte === 0xFC) { // Milliseconds timestamp
                        expiryTime = this.readUInt64LE();
                    } else { // Seconds timestamp
                        expiryTime = this.readUInt32LE() * 1000; // Convert to milliseconds
                    }
                }

                // Read value type
                const valueType = this.data[this.position++];
                if (valueType !== 0) { // Only support string values for now
                    throw new Error(`Unsupported value type: ${valueType}`);
                }

                // Read key and value
                const key = this.readString();
                const value = this.readString();

                storage.set(key, { value, expiryTime });
            }
        }

        return storage;
    }

    private skipString(): void {
        const length = this.readSize();
        this.position += length;
    }

    private skipSize(): void {
        const byte = this.data[this.position++];
        const type = (byte & 0xC0) >> 6;
        
        if (type === 0) return;
        else if (type === 1) this.position += 1;
        else if (type === 2) this.position += 4;
        else throw new Error(`Invalid size encoding type: ${type}`);
    }

    private readSize(): number {
        const byte = this.data[this.position++];
        const type = (byte & 0xC0) >> 6;
        
        if (type === 0) return byte & 0x3F;
        else if (type === 1) {
            const next = this.data[this.position++];
            return ((byte & 0x3F) << 8) | next;
        } else if (type === 2) {
            const size = this.data.readUInt32BE(this.position);
            this.position += 4;
            return size;
        } else if (type === 3) {
            // For type 3, we need to handle special string encoding
            // The length is stored in the lower 6 bits
            return byte & 0x3F;
        } else {
            throw new Error(`Invalid size encoding type: ${type}`);
        }
    }

    private readString(): string {
        const length = this.readSize();
        const str = this.data.slice(this.position, this.position + length).toString();
        this.position += length;
        return str;
    }

    private readUInt32LE(): number {
        const val = this.data.readUInt32LE(this.position);
        this.position += 4;
        return val;
    }

    private readUInt64LE(): number {
        // Note: This might lose precision for very large numbers
        const low = this.data.readUInt32LE(this.position);
        const high = this.data.readUInt32LE(this.position + 4);
        this.position += 8;
        return high * 0x100000000 + low;
    }
}

class RedisServer {
    private server: net.Server;
    private storage: Map<string, StorageItem>;
    private config: RedisConfig;
    private role: 'master' | 'slave';
    private masterReplId: string = '8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb';
    private masterReplOffset: number = 0;

    constructor(config: RedisConfig) {
        this.server = net.createServer((connection: net.Socket) => {
            this.handleConnection(connection);
        });
        this.storage = new Map();
        this.config = config;
        this.role = config.replicaof ? 'slave' : 'master';

        // Try to load RDB file if it exists
        const rdbPath = `${this.config.dir}/${this.config.dbfilename}`;
        try {
            if (fs.existsSync(rdbPath)) {
                const rdbData = fs.readFileSync(rdbPath);
                const parser = new RDBParser(rdbData);
                this.storage = parser.parse();
            }
        } catch (error) {
            console.error('Error loading RDB file:', error);
        }

        // If we're a replica, connect to master
        if (this.config.replicaof) {
            this.connectToMaster();
        }
    }

    private handleConnection(connection: net.Socket) {
        connection.on('data', (data: Buffer) => {
            try {
                const parser = new RespParser(data.toString());
                const command = parser.parse();
                const response = this.handleCommand(command);
                connection.write(response);
            } catch (error) {
                console.error('Error processing command:', error);
                connection.write('-ERR Invalid command format\r\n');
            }
        });
    }

    private isExpired(key: string): boolean {
        const item = this.storage.get(key);
        if (!item || !item.expiryTime) {
            return false;
        }
        return Date.now() > item.expiryTime;
    }

    private connectToMaster() {
        if (!this.config.replicaof) return;
    
        const client = new net.Socket();
        client.connect(this.config.replicaof.port, this.config.replicaof.host, () => {
            // Send PING command in RESP format
            client.write('*1\r\n$4\r\nPING\r\n');
        });
    
        let pingReceived = false;
        let firstReplconfSent = false;
    
        client.on('data', (data) => {
            const response = data.toString();
            console.log('Received from master:', response);
    
            if (!pingReceived && response === '+PONG\r\n') {
                pingReceived = true;
                // Send first REPLCONF command (listening-port)
                const address = this.server.address();
                const serverPort = typeof address === 'string' ? '6380' : address?.port?.toString() || '6380';
                // Format: REPLCONF listening-port <PORT>
                client.write(`*3\r\n$8\r\nREPLCONF\r\n$14\r\nlistening-port\r\n$${serverPort.length}\r\n${serverPort}\r\n`);
            } else if (pingReceived && !firstReplconfSent && response === '+OK\r\n') {
                firstReplconfSent = true;
                // Send second REPLCONF command (capabilities)
                client.write('*3\r\n$8\r\nREPLCONF\r\n$4\r\ncapa\r\n$6\r\npsync2\r\n');
            }
        });
    
        client.on('error', (error) => {
            console.error('Error connecting to master:', error);
        });
    }

    private handleCommand(command: string[]): string {
        const commandName = command[0].toUpperCase();
        
        switch (commandName) {
            case 'PING':
                return '+PONG\r\n';
                
            case 'ECHO':
                if (command.length < 2) {
                    return '-ERR wrong number of arguments for \'echo\' command\r\n';
                }
                const arg = command[1];
                return `$${arg.length}\r\n${arg}\r\n`;
                
            case 'SET':
                if (command.length < 3) {
                    return '-ERR wrong number of arguments for \'set\' command\r\n';
                }
                const [_, key, value, ...options] = command;
                let expiryTime: number | undefined;
                
                // Parse options
                for (let i = 0; i < options.length; i += 2) {
                    const option = options[i]?.toUpperCase();
                    const optionValue = options[i + 1];
                    
                    if (option === 'PX') {
                        if (!optionValue || isNaN(Number(optionValue))) {
                            return '-ERR value is not an integer or out of range\r\n';
                        }
                        expiryTime = Date.now() + Number(optionValue);
                    }
                }
                
                this.storage.set(key, {
                    value,
                    expiryTime
                });
                return '+OK\r\n';
                
            case 'GET':
                if (command.length < 2) {
                    return '-ERR wrong number of arguments for \'get\' command\r\n';
                }
                const getKey = command[1];
                
                // Check if key exists and isn't expired
                if (!this.storage.has(getKey) || this.isExpired(getKey)) {
                    if (this.isExpired(getKey)) {
                        this.storage.delete(getKey); // Clean up expired key
                    }
                    return '$-1\r\n';
                }
                
                const item = this.storage.get(getKey)!;
                return `$${item.value.length}\r\n${item.value}\r\n`;
                
            case 'INFO':
                if (command.length > 1 && command[1].toLowerCase() !== 'replication') {
                    return '-ERR unsupported INFO section';
                }
                const response = `role:${this.role}\nmaster_replid:${this.masterReplId}\nmaster_repl_offset:${this.masterReplOffset}`;
                return `$${response.length}\r\n${response}\r\n`;

            case 'CONFIG':
                if (command.length < 3 || command[1].toUpperCase() !== 'GET') {
                    return '-ERR wrong number of arguments for \'config\' command\r\n';
                }
                const param = command[2].toLowerCase();
                if (param === 'dir' || param === 'dbfilename') {
                    return `*2\r\n$${param.length}\r\n${param}\r\n$${this.config[param].length}\r\n${this.config[param]}\r\n`;
                }
                return '$-1\r\n';

            case 'KEYS':
                if (command.length !== 2) {
                    return '-ERR wrong number of arguments for \'keys\' command\r\n';
                }
                const pattern = command[1];
                if (pattern !== '*') {
                    return '-ERR only * pattern is supported\r\n';
                }
                
                // Filter out expired keys and get remaining keys
                const activeKeys = Array.from(this.storage.keys())
                    .filter(key => !this.isExpired(key));
                
                // Clean up expired keys while we're at it
                for (const key of this.storage.keys()) {
                    if (this.isExpired(key)) {
                        this.storage.delete(key);
                    }
                }
                
                // Format response as RESP array
                const respArray = ['*' + activeKeys.length];
                for (const key of activeKeys) {
                    respArray.push(`$${key.length}`);
                    respArray.push(key);
                }
                return respArray.join('\r\n') + '\r\n';
                
            default:
                return `-ERR unknown command '${commandName}'\r\n`;
        }
    }

    listen(port: number, host: string) {
        this.server.listen(port, host);
    }
}

class RespParser {
    private data: string = '';
    private position: number = 0;

    constructor(data: string) {
        this.data = data;
    }

    parse(): any {
        const type = this.data[this.position];
        this.position++;

        switch (type) {
            case '+':
                return this.parseSimpleString();
            case '*':
                return this.parseArray();
            case '$':
                return this.parseBulkString();
            default:
                throw new Error(`Unsupported RESP type: ${type}`);
        }
    }

    private parseArray(): string[] {
        const length = this.parseInteger();
        const result: string[] = [];

        for (let i = 0; i < length; i++) {
            this.consumeCRLF();
            result.push(this.parse());
        }

        return result;
    }

    private parseBulkString(): string {
        const length = this.parseInteger();
        if (length === -1) {
            return '';
        }

        this.consumeCRLF();
        const result = this.data.slice(this.position, this.position + length);
        this.position += length;
        this.consumeCRLF();
        return result;
    }

    private parseSimpleString(): string {
        let result = '';
        while (this.position < this.data.length && this.data[this.position] !== '\r') {
            result += this.data[this.position];
            this.position++;
        }
        this.consumeCRLF();
        return result;
    }

    private parseInteger(): number {
        let numStr = '';
        while (this.position < this.data.length && this.data[this.position] !== '\r') {
            numStr += this.data[this.position];
            this.position++;
        }
        this.consumeCRLF();
        return parseInt(numStr, 10);
    }

    private consumeCRLF() {
        if (this.data[this.position] === '\r' && this.data[this.position + 1] === '\n') {
            this.position += 2;
        }
    }
}

// Parse command line arguments
const args = process.argv.slice(2);
let dir = '/tmp';
let dbfilename = 'dump.rdb';
let port = 6379; // Default port

let replicaof: { host: string; port: number } | undefined;

for (let i = 0; i < args.length; i += 2) {
    const arg = args[i];
    const value = args[i + 1];
    
    if (arg === '--dir') {
        dir = value;
    } else if (arg === '--dbfilename') {
        dbfilename = value;
    } else if (arg === '--port') {
        port = parseInt(value, 10);
    } else if (arg === '--replicaof') {
        const [host, replicaPort] = value.split(' ');
        replicaof = {
            host,
            port: parseInt(replicaPort, 10)
        };
    }
}

// Start the server
const server = new RedisServer({ dir, dbfilename, replicaof });
server.listen(port, "127.0.0.1");