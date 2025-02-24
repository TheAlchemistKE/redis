import type { StorageItem } from "../entities/StorageItem";

export interface RDBParser {
    parse(): Map<string, StorageItem>;
}

export interface RDBParserOptions {
    data: Buffer;
}