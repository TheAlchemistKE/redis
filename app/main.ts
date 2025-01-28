import * as net from "net";

// RESP Types
enum RespType {
    SimpleString = '+',
    Error = '-',
    Integer = ':',
    BulkString = '$',
    Array = '*'
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
            case RespType.Array:
                return this.parseArray();
            case RespType.BulkString:
                return this.parseBulkString();
            case RespType.SimpleString:
                return this.parseSimpleString();
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
            return ''; // Null bulk string
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

class RedisServer {
    private server: net.Server;

    constructor() {
        this.server = net.createServer((connection: net.Socket) => {
            this.handleConnection(connection);
        });
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
            default:
                return `-ERR unknown command '${commandName}'\r\n`;
        }
    }

    listen(port: number, host: string) {
        this.server.listen(port, host);
    }
}

// Start the server
const server = new RedisServer();
server.listen(6379, "127.0.0.1");