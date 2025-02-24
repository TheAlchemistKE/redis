import type { StorageItem } from "../../domain/entities/StorageItem";
import type { RDBParser, RDBParserOptions } from "../../domain/ports/RDBParser";

export class RDBParserImpl implements RDBParser {
    private data: Buffer;
    private position: number = 0;

    constructor(options: RDBParserOptions) {
        this.data = options.data;
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