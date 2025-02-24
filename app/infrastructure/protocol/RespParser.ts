export class RespParser {
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