export interface RedisConfig {
    dir: string;
    dbfilename: string;
    replicaof?: {
        host: string;
        port: number;
    };
}