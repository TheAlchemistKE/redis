import * as net from "net";

interface StorageItem {
    value: string;
    expiryTime?: number;  // Optional expiry time in milliseconds since epoch
}

class RedisServer {
    private server: net.Server;
    private storage: Map<string, StorageItem>;

    constructor() {
        this.server = net.createServer((connection: net.Socket) => {
            this.handleConnection(connection);
        });
        this.storage = new Map();
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
                
            default:
                return `-ERR unknown command '${commandName}'\r\n`;
        }
    }

    listen(port: number, host: string) {
        this.server.listen(port, host);
    }
}

// RESP parser remains the same
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

// Start the server
const server = new RedisServer();
server.listen(6379, "127.0.0.1");