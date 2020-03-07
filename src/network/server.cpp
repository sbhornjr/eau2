// lang::CwC

#include "server.h"

Server* server;

/**
    Upon receiving a control-c event, tear down the networking infrastructure.
    Only necessary on the server, since it will take down all clients as well.
**/
void terminate(int signum) {
    server->teardown();
    delete server;
    exit(signum);
}

/**
    Starts a server with arguments passed in for ip and port.
**/
int main(int argc, char** argv) {
    if (argc != 5 || strcmp(argv[1], "-ip") != 0 || strcmp(argv[3], "-port")
        != 0) {
        printf("\"./server -ip [ip address] -port [port]\" required\n");
        exit(1);
    }

    String* ip = new String(argv[2]);
    String* port = new String(argv[4]);

    server = new Server(ip->c_str(), port->c_str());

    // Listen for a control-c event.
    signal(SIGINT, terminate);

    server->run();

    delete server;
    delete ip;
    delete port;

    return 0;
}
