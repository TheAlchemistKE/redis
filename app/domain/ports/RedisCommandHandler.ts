import type { StorageItem } from "../entities/StorageItem";

export interface RedisCommandHandler {
    handlePing(): string;
    handleEcho(args: string[]): string;
    handleSet(key: string, value: string, options?: { px?: number }): string;
    handleGet(key: string): string;
    handleInfo(section?: string): string;
    handleConfig(operation: string, param: string): string;
    handleKeys(pattern: string): string;
    getStorage(): Map<string, StorageItem>;
}