import * as net from "net";
import * as fs from "fs";
import { RDBParserImpl } from "../persistence/RDBParserImpl";
import { RespParser } from "../protocol/RespParser";
import type { RedisConfig } from "../../domain/entities/RedisConfig";
import type { StorageItem } from "../../domain/entities/StorageItem";

export class RedisServer {
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
                const parser = new RDBParserImpl({ data: rdbData });
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
            } else if (firstReplconfSent && response === '+OK\r\n') {
                // Send PSYNC command with replication ID '?' and offset '-1'
                client.write('*3\r\n$5\r\nPSYNC\r\n$1\r\n?\r\n$2\r\n-1\r\n');
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

            case 'REPLCONF':
                // For both REPLCONF commands (listening-port and capa psync2), respond with OK
                return '+OK\r\n';

            case 'PSYNC':
                const fullresyncResponse = Buffer.from(`+FULLRESYNC ${this.masterReplId} ${this.masterReplOffset}\r\n`);
                const emptyRDBFile = Buffer.from('524544495330303131ff00000000000000000000ff', 'hex');
                const rdbLength = Buffer.from(`$${emptyRDBFile.length}\r\n`);
                return Buffer.concat([fullresyncResponse, rdbLength, emptyRDBFile]);
                
            default:
                return `-ERR unknown command '${commandName}'\r\n`;
        }
    }

    listen(port: number, host: string) {
        this.server.listen(port, host);
    }
}