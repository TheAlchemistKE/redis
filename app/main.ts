import * as net from "net";

// You can use print statements as follows for debugging, they'll be visible when running tests.
console.log("Logs from your program will appear here!");

const server: net.Server = net.createServer((connection: net.Socket) => {
    // Response for PING command in RESP format
    const response = "+PONG\r\n";
    
    // Send response regardless of input (as per requirements)
    connection.write(response);
});

server.listen(6379, "127.0.0.1");