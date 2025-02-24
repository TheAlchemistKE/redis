import { RedisServer } from "./infrastructure/server/RedisServer";

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