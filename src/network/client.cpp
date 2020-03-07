// lang::CwC
#include "client.h"
#include <sys/time.h>

/**
    Starts a client with arguments passed in for ip, port, and serverip.
**/
int main(int argc, char** argv) {
    if (argc != 7 || strcmp(argv[1], "-ip") != 0 || strcmp(argv[3], "-port")
        != 0 || strcmp(argv[5], "-serverip") != 0) {
        printf("\"./client -ip [ip address] -port [port] -serverip [sip]\" required\n");
        exit(1);
    }

    // We want to wait a second to make sure server starts before clients.
    sleep(1);

    /** Random number generator allows us to
        have clients sending messages
        back and forth at a random interval.
        This is useful for demonstrating the
        functionality of our network for this
        assignment. **/
    //time_t t;
    timeval t1;
    gettimeofday(&t1, NULL);
    srand(t1.tv_usec * t1.tv_sec);
    int randNum = rand()%(10-5 + 1) + 5;

    String* ip = new String(argv[2]);
    String* port = new String(argv[4]);
    String* serverip = new String(argv[6]);
    Client* client = new Client(ip->c_str(), port->c_str(), serverip, randNum);

    client->run();

    delete client;
    delete ip;
    delete port;
    delete serverip;

    return 0;
}
