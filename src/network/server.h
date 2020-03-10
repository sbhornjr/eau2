// lang:: CwC

#pragma once
#include <unistd.h>
#include <stdio.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <stdlib.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <netdb.h>
#include <string.h>
#include <fcntl.h>
#include "../object.h"
#include "../string.h"
#include "message.h"
#include "../array.h"
#include "../serial.h"
#include "socket.h"

#define MAX_TIME 30

// get sockaddr, IPv4 or IPv6: FROM BEEJ'S GUIDE
void *get_in_addr(struct sockaddr *sa) {
    if (sa->sa_family == AF_INET) {
        return &(((struct sockaddr_in*)sa)->sin_addr);
    }

    return &(((struct sockaddr_in6*)sa)->sin6_addr);
}

/**
 * Server class that wraps operations such as registering clients,
 * updating clients with information about other clients,
 * and receiving messages from clients.
 * @authors horn.s@husky.neu.edu, armani.a@husky.neu.edu
 */
class Server : public Socket {
public:

    IntArray* socks;               // the sockets of all registered clients

    // default constructor
    Server(): Socket(), socks(new IntArray()) { setup(); }

    // constructor that takes in an ip address for this server
    Server(char* ip): Socket(ip), socks(new IntArray()) { setup(); }

    // constructor that takes in an ip address and a port
    Server(char* ip, char* port): Socket(ip, port), socks(new IntArray()) { setup(); }

    /**
     * sets up this server. creates a listening socket and binds it.
     */
    void setup() {
      struct addrinfo hints, *res;    // hints and res addrinfos
      int status;                     // general status code holder

      // set some values of hints
      memset(&hints, 0, sizeof hints);  // make sure hints is empty
      hints.ai_family = AF_INET;        // ipv4
      hints.ai_socktype = SOCK_STREAM;  // sock stream
      hints.ai_flags = AI_PASSIVE;      // use my ip

      // get the addrinfo struct given hints
      status = getaddrinfo(NULL, port, &hints, &res);
      if (status < 0) {
        printf("server: exiting - setup - getting address info failed\n");
        exit(1);
      }

      // set up the socket
      sockfd = socket(res->ai_family, res->ai_socktype, res->ai_protocol);
      if (sockfd < 0) {
        printf("server: exiting - setup - socket creation failed\n");
        exit(1);
      }

      // set sock option so we can reuse the address later
      int i = 1;
      status = setsockopt(sockfd, SOL_SOCKET, SO_REUSEADDR, &i, sizeof(int));
      if (status < 0) {
        printf("server: exiting - setup - setting socket option\n");
        teardown();
      }

      // bind the socket to the address
      bind(sockfd, res->ai_addr, res->ai_addrlen);

      freeaddrinfo(res);    // free the addrinfo struct

      // set the socket up to listen
      status = listen(sockfd, 10);
      if (status < 0) {
        printf("server: exiting - listening error\n");
        teardown();
      }
    }

    /**
     * main looping function. uses select to receive incoming connections.
     */
    void run() {
      if (dead) return ;

      printf("\n\nserver: waiting for connections\n");
      int status;             // general status code holder

      time_t total_time = time(NULL);

      // timeout for select
      struct timeval tv;
      tv.tv_sec = 0;
      tv.tv_usec = 0;

      // main for loop
      while(1) {
        if (dead) return ;

        // zero out read_fds, set listener, set fdmax
        FD_ZERO(&read_fds);
        FD_SET(sockfd, &read_fds);
        fdmax = sockfd;
        for (size_t i = 0; i < registered; ++i) {
          if (socks->get(i) > fdmax) fdmax = socks->get(i);
          FD_SET(socks->get(i), &read_fds);
        }

        // select to see if we have a connection
        status = select(fdmax + 1, &read_fds, NULL, NULL, &tv);
        if (status < 0) {
          printf("server: exiting - select error\n");
          teardown();
        }

        // for loop - do we have a connection?
        for (size_t i = 0; i <= fdmax; ++i) {
          // we have a connection
          if (FD_ISSET(i, &read_fds)) {
            // the connection is from a new user - register
            if (i == sockfd) {
              register_client();
            // this user is already registered
            } else {
              // find which registered user is contacting
              for (size_t j = 0; j < registered; ++j) {
                int sock = socks->get(j);
                // we found the user
                if (i == sock) {
                  char msg[1024];
                  memset(msg, 0, 1024);
                  String* user_ip = client_ips->get(j);

                  // receive data from client
                  status = recv(sock, msg, 1024, 0);
                  if (status < 0) {
                    printf("server: exiting - error receiving message\n");
                    printf("FAILED: %s\n", std::strerror(errno));
                    teardown();
                  }

                  // received message
                  if (status > 0) {
                    printf("server: message received from %s:\n\"%s\"\n",
                           user_ip->c_str(), msg);
                  }
                  // received loss of connection message
                  else {
                    printf("server: client %s lost connection\n", user_ip->c_str());
                    kill_client(j);
                  }
                }
                break;
              }
            }
          }
        }
        // if it's max time then shutdown
        if ((double)(time(NULL) - total_time) >= MAX_TIME) {
          teardown();
          return ;
        }
      }
    }

    /**
     * there is a new client waiting to be accepted and registered.
     * use accept on the listening socket to register the new client
     * with a new socket and their ip address.
     * also updates all clients with a new Directory message.
     */
    int register_client() {
      int status;                     // general status code holder

      FD_CLR(sockfd, &read_fds);

      // accept incoming connection
      int sock = accept(sockfd, NULL, NULL);
      if (sock < 0) {
        printf("server: accepting error. not registering client\n");
        return 1;
      }

      char ser_reg[100];
      memset(ser_reg, 0, 100);

      // receive registration data
      status = recv(sock, ser_reg, 100, 0);
      if (status < 0) {
        printf("server: error receiving registration msg. not registering client\n");
        return 1;
      }

      // received empty message
      if (status == 0) {
        printf("server: received empty registration message. not registering client\n");
        return 1;
      }

      // get the Register message
      Message* reg_msg = s->get_message(ser_reg);
      if (reg_msg->kind_ != MsgKind::Register) {
        printf("server: incorrect message received. expected Register and got %c\n", reg_msg->kind_);
        return 1;
      }
      Register* reg = dynamic_cast<Register*>(reg_msg);

      // get client ip
      char ip_str[INET6_ADDRSTRLEN];
      struct sockaddr_in client_addr = reg->client_;
      inet_ntop(client_addr.sin_family, get_in_addr((struct sockaddr *)&client_addr),
                ip_str, INET6_ADDRSTRLEN);

      // this client already registered with me
      for (size_t i = 0; i < registered; ++i) {
        if (strcmp(ip_str, client_ips->get(i)->c_str()) == 0) {
          printf("server: client %s tried to register again\n", ip_str);
          return 1;
        }
      }

      delete reg;

      String* their_ip = new String(ip_str);  // their ip as a String*

      // ACK message
      Ack* ack = new Ack(registered, ++id);
      const char* ser_ack = s->serialize(ack);

      // send registration confirmation to client
      status = send(sock, ser_ack, strlen(ser_ack), 0);
      if (status < 0) {
        printf("server: error sending confirmation of registration. not registering client\n");
        exit(1);
      }

      printf("\e[1;34mserver: %s registered\033[0m\n", ip_str);

      // add ip and sock to lists
      socks->push_back(sock);
      client_ips->push_back(their_ip);
      ports->push_back(client_addr.sin_port);

      ++registered;

      delete their_ip;

      delete ack;
      delete[] ser_ack;

      send_directory();

      return 0;
    }

    /** send the directory to all clients */
    void send_directory() {
      int status;

      // set up ports array
      size_t ports_[registered];
      for (size_t i = 0; i < registered; ++i) {
        int iport = ports->get(i);
        assert(iport >= 0);
        ports_[i] = (size_t)iport;
      }

      // send directory message to all existing clients
      for (size_t i = 0; i < registered; ++i) {
        String* client_ip = client_ips->get(i);

        printf("server: sending directory to client %s\n", client_ip->c_str());

        // DIRECTORY message
        Directory* dir = new Directory(i + 1, ++id, registered, ports_, client_ips);
        const char* ser_dir = s->serialize(dir);

        status = send(socks->get(i), ser_dir, strlen(ser_dir), 0);
        if (status < 0) {
          status = send(socks->get(i), ser_dir, strlen(ser_dir), 0);
          if (status < 0) {
            printf("server: killing client\n");
            kill_client(i);
          }
        }

        delete dir;
        delete[] ser_dir;
      }
    }

    /** kills the given client and sends new directory messages */
    void kill_client(size_t i) {
      ports->remove(i);
      socks->remove(i);
      client_ips->remove(i);
      --registered;

      send_directory();
    }

    /** exit and close all sockets */
    void teardown() {
      int status;

      dead = true;

      // send kill messages
      for (size_t i = 0; i < registered; ++i) {
        Kill* kill = new Kill(i + 1, ++id);
        const char* ser_kill = s->serialize(kill);

        status = send(socks->get(i), ser_kill, strlen(ser_kill), 0);
        if (status < 0) {
          status = send(socks->get(i), ser_kill, strlen(ser_kill), 0);
          if (status < 0) {
            printf("server: exiting - error sending kill message\n");
          }
        }
      }

      close(sockfd);
      for (size_t i = 0; i < registered; ++i) {
        close(socks->get(i));
      }

      printf("\033[0;31mserver shutting down \033[0m\n");
    }
};
