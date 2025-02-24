import type { StorageItem } from "../domain/entities/StorageItem";
import type { RedisCommandHandler } from "../domain/ports/RedisCommandHandler";

export class RedisCommandHandlerImpl implements RedisCommandHandler {
    private storage: Map<string, StorageItem>;
    private role: 'master' | 'slave';
    private masterReplId: string;
    private masterReplOffset: number;
    private config: { dir: string; dbfilename: string };

    constructor(config: { dir: string; dbfilename: string }, role: 'master' | 'slave', storage: Map<string, StorageItem>) {
        this.storage = storage;
        this.role = role;
        this.config = config;
        this.masterReplId = '8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb';
        this.masterReplOffset = 0;
    }

    handlePing(): string {
        return '+PONG\r\n';
    }

    handleEcho(args: string[]): string {
        if (args.length < 1) {
            return '-ERR wrong number of arguments for \'echo\' command\r\n';
        }
        const arg = args[0];
        return `$${arg.length}\r\n${arg}\r\n`;
    }

    handleSet(key: string, value: string, options?: { px?: number }): string {
        let expiryTime: number | undefined;
        
        if (options?.px) {
            expiryTime = Date.now() + options.px;
        }
        
        this.storage.set(key, {
            value,
            expiryTime
        });
        return '+OK\r\n';
    }

    handleGet(key: string): string {
        if (!this.storage.has(key) || this.isExpired(key)) {
            if (this.isExpired(key)) {
                this.storage.delete(key);
            }
            return '$-1\r\n';
        }
        
        const item = this.storage.get(key)!;
        return `$${item.value.length}\r\n${item.value}\r\n`;
    }

    handleInfo(section?: string): string {
        if (section && section.toLowerCase() !== 'replication') {
            return '-ERR unsupported INFO section';
        }
        const response = `role:${this.role}\nmaster_replid:${this.masterReplId}\nmaster_repl_offset:${this.masterReplOffset}`;
        return `$${response.length}\r\n${response}\r\n`;
    }

    handleConfig(operation: string, param: string): string {
        if (operation.toUpperCase() !== 'GET') {
            return '-ERR wrong number of arguments for \'config\' command\r\n';
        }
        const paramLower = param.toLowerCase();
        if (paramLower === 'dir' || paramLower === 'dbfilename') {
            return `*2\r\n$${paramLower.length}\r\n${paramLower}\r\n$${this.config[paramLower].length}\r\n${this.config[paramLower]}\r\n`;
        }
        return '$-1\r\n';
    }

    handleKeys(pattern: string): string {
        if (pattern !== '*') {
            return '-ERR only * pattern is supported\r\n';
        }
        
        const activeKeys = Array.from(this.storage.keys())
            .filter(key => !this.isExpired(key));
        
        for (const key of this.storage.keys()) {
            if (this.isExpired(key)) {
                this.storage.delete(key);
            }
        }
        
        const respArray = ['*' + activeKeys.length];
        for (const key of activeKeys) {
            respArray.push(`$${key.length}`);
            respArray.push(key);
        }
        return respArray.join('\r\n') + '\r\n';
    }

    getStorage(): Map<string, StorageItem> {
        return this.storage;
    }

    private isExpired(key: string): boolean {
        const item = this.storage.get(key);
        if (!item || !item.expiryTime) {
            return false;
        }
        return Date.now() > item.expiryTime;
    }
}