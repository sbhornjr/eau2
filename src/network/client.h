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
#include "object.h"
#include "string.h"
#include "message.h"
#include "array.h"
#include "serial.h"
#include "socket.h"

/**
 * Client class that wraps operations such as registering with a server,
 * sending messages to that server and sending messages to other clients.
 * @authors horn.s@husky.neu.edu, armani.a@husky.neu.edu
 */
class Client : public Socket {
public:

    String* server_ip;          // ip of server
    int servfd;                 // socket to server
    int seconds;                // amount of time between sending random msgs
    bool confirmed;             // am i registered

    // constructor that takes just the ip address of this client
    Client(char* ip)
    : Socket(ip),
      server_ip(new String("127.0.0.1")),
      seconds(-1),
      confirmed(false) { setup(); }

    // constructor that takes the ip address and the amount of seconds
    // between sending random messages
    Client(char* ip, int seconds)
    : Socket(ip),
      server_ip(new String("127.0.0.1")),
      seconds(seconds),
      confirmed(false) { setup(); }

    // constructor that takes the ip address of this client and the port
    Client(char* ip, char* port)
    : Socket(ip, port),
      server_ip(new String("127.0.0.1")),
      seconds(-1),
      confirmed(false) { setup(); }

    // constructor that takes the ip address, the port, and
    // the amount of seconds between sending random messages
    Client(char* ip, char* port, int seconds)
    : Socket(ip, port),
      server_ip(new String("127.0.0.1")),
      seconds(seconds),
      confirmed(false) { setup(); }

    // constructor that takes the ip address, the port, the serverip, and
    // the amount of seconds between sending random messages
    Client(char* ip, char* port, String* serverip, int seconds)
    : Socket(ip, port),
      server_ip(serverip),
      seconds(seconds),
      confirmed(false) { setup(); }

    // sets up the client with a socket for sending to server
    void setup() {
      struct addrinfo hints, *res;    // hints and res addrinfos
      int status;                     // general status code holder

      // set some values of hints
      memset(&hints, 0, sizeof hints);  // make sure hints is empty
      hints.ai_family = AF_INET;        // ipv4
      hints.ai_socktype = SOCK_STREAM;  // sock stream

      // get the addrinfo struct given hints
      status = getaddrinfo(ip->c_str(), port, &hints, &res);
      if (status < 0) {
        printf("client %s: exiting - setup - getting address info failed\n", ip->c_str());
        exit(1);
      }

      // set up the socket
      sockfd = socket(res->ai_family, res->ai_socktype, res->ai_protocol);
      if (sockfd < 0) {
        printf("client %s: exiting - setup - socket creation failed\n", ip->c_str());
        exit(1);
      }

      // set sock option so we can reuse the address later
      int i = 1;
      status = setsockopt(sockfd, SOL_SOCKET, SO_REUSEADDR, &i, sizeof(int));
      if (status < 0) {
        printf("client %s: exiting - setup - setting socket option\n", ip->c_str());
        teardown();
      }

      // bind the socket
      bind(sockfd, res->ai_addr, res->ai_addrlen);

      // set the socket up to listen
      status = listen(sockfd, 10);
      if (status < 0) {
        printf("client %s: exiting - listening error\n", ip->c_str());
        teardown();
      }

      freeaddrinfo(res);    // free the addrinfo struct

      register_client();    // register with the server
    }

    // registers this client with the server
    // also receives info about other clients from server
    int register_client() {
      int status;     // status code holder

      printf("\033[1;36mclient %s: registering with server\033[0m\n", ip->c_str());

      servfd = socket(AF_INET, SOCK_STREAM, 0);
      if (servfd < 0) {
        printf("client %s: exiting - socket creation failed\n", ip->c_str());
        teardown();
      }

      struct addrinfo hints, *res;    // hints and res addrinfos

      // set some values of hints
      memset(&hints, 0, sizeof hints);  // make sure hints is empty
      hints.ai_family = AF_INET;        // ipv4
      hints.ai_socktype = SOCK_STREAM;  // sock stream

      // get the addrinfo struct given hints
      status = getaddrinfo(server_ip->c_str(), port, &hints, &res);
      if (status < 0) {
        printf("client %s: exiting - getting address info failed\n", ip->c_str());
        teardown();
      }

      // connect the socket to the address
      status = connect(servfd, res->ai_addr, res->ai_addrlen);
      if (status < 0) {
        printf("client %s: exiting - connecting socket\n", ip->c_str());
        printf("FAILED: %s\n", std::strerror(errno));
        teardown();
      }

      // Registration message
      struct sockaddr_in my_addr;
      my_addr.sin_family = AF_INET;
      my_addr.sin_addr.s_addr = inet_addr(ip->c_str());
      size_t sz = atoi(port);       // get the port as a size_t
      my_addr.sin_port = sz;

      Register* reg = new Register(1, ++id, my_addr, sz);
      const char* ser_reg = s->serialize(reg);

      // send registration message to server
      status = send(servfd, ser_reg, strlen(ser_reg), 0);
      if (status < 0) {
        printf("client %s: exiting - error sending registration\n", ip->c_str());
        teardown();
      }

      delete reg;
      delete[] ser_reg;

      // receive the confirmation of registration
      char buff[50];
      status = recv(servfd, buff, 50, 0);
      if (status < 0) {
        printf("client %s: exiting - error receiving registration confirmation\n", ip->c_str());
        teardown();
      }

      // make sure this is an Ack message
      Message* ack_msg = s->get_message(buff);
      if (ack_msg->kind_ != MsgKind::Ack) {
        printf("client %s: expected ack from server but got message of type %c\n",
               ip->c_str(), ack_msg->kind_);
        teardown();
      }

      // confirmation received
      printf("client %s: confirmed by server\n", ip->c_str());
      confirmed = true;

      return 0;
    }

    // Directory message received from server - update current directory.
    void get_directory(Directory* dir) {
      // clear current information
      delete ports;
      delete client_ips;
      ports = new IntArray();
      client_ips = new StringArray();

      // add new information
      registered = dir->clients_ - 1;
      for (size_t i = 0; i <= registered; ++i) {
        if (dir->addresses_->get(i)->equals(ip)) continue;
        ports->push_back(dir->ports_[i]);
        client_ips->push_back(dir->addresses_->get(i));
      }
    }

    // there is a message from the server
    int serv_message() {
      int status;                               // general status code holder
      char client_ip[16];                       // client ip buffer (max 15 chars)

      memset(client_ip, 0, 16);

      char serv_msg[512];

      // receive msg from server
      status = recv(servfd, serv_msg, 512, 0);
      if (status < 0) {
        printf("client %s: exiting - error receiving message from server\n", ip->c_str());
        teardown();
      }

      // connection to server lost
      if (status == 0) {
        printf("client %s: exiting - connection to server lost\n", ip->c_str());
        teardown();
      }

      // Handle different kinds of messages.
      Message* msg = s->get_message(serv_msg);
      if (msg->kind_ == MsgKind::Directory) {
        printf("client %s: new directory received from server. updating info\n", ip->c_str());
        get_directory(dynamic_cast<Directory*>(msg));
      } else if (msg->kind_ == MsgKind::Kill) {
        teardown();
      } else {
        printf("client %s: MESSAGE TYPE NOT RECOGNIZED: %c\n", ip->c_str(), (char)msg->kind_);
      }

      delete msg;

      return 0;
    }

    // connection from new client - accept and print message
    Text* get_client_msg() {
      int status;                     // general status code holder

      // accept incoming connection
      int sock = accept(sockfd, NULL, NULL);
      if (sock < 0) {
        printf("client %s: exiting - accepting error\n", ip->c_str());
        return nullptr;
      }

      char msg[1024];
      memset(msg, 0, 1024);

      // receive msg from client
      status = recv(sock, msg, 1024, 0);
      if (status < 0) {
        // One more attempt before closing connection.
        if(recv(sock, msg, 1024, 0) < 0) {
          printf("client %s: exiting - error receiving msg\n", ip->c_str());
          close(sock);
          return nullptr;
        }
      }

      Message* defaultMessage = s->get_message(msg);
      if (defaultMessage->kind_ == MsgKind::Text) {
        Text* text = dynamic_cast<Text*>(defaultMessage);
        printf("\033[0;32mclient %s: message received from %s: \033[0m \t\"%s\"\n",
          ip->c_str(), text->senderip_->c_str(), text->msg_->c_str());
        close(sock);
        return text;
      } else {
        printf("\033[0;31mclient %s: message received of UNEXPECTED TYPE %d\033[0m ",
         ip->c_str(), defaultMessage->kind_);
         close(sock);
        return nullptr;
      }
    }

    // main looping function to receive data from sockets
    void run() {
      if (dead) return ;

      printf("client %s: \033[0;33m waiting for messages \033[0m\n", ip->c_str());
      int status;             // general status code holder

      struct timeval tv;
      tv.tv_sec = 0;
      tv.tv_usec = 0;

      time_t since_last_msg_sent = time(NULL);

      // main for loop
      while(1) {
        if (dead) return ;
        // zero out read_fds, add sockets, set fdmax
        FD_ZERO(&read_fds);
        FD_SET(sockfd, &read_fds);
        FD_SET(servfd, &read_fds);
        if (sockfd > servfd) fdmax = sockfd;
        else fdmax = servfd;

        // select to see if we have a connection
        status = select(fdmax + 1, &read_fds, NULL, NULL, &tv);
        if (status < 0) {
          printf("client %s: exiting - select error\n", ip->c_str());
          teardown();
        }

        // RECEIVE -
        // for loop - do we have a connection?
        for (size_t i = 0; i <= fdmax; ++i) {
          // we have a connection
          if (FD_ISSET(i, &read_fds)) {
            // the connection is from another client
            if (i == sockfd) {
              Text* text = get_client_msg();  // client connection
              // do stuff with the text here, we will do nothing
              if (text) {
                delete text;
              }
              continue;
            }
            // the connection is from the server
            else if (i == servfd) {
              serv_message();           // new directory from server
              continue;
            } else {
              printf("client %s: connection from unknown socket\n", ip->c_str());
              continue;
            }
          }
        }

        // SEND -
        // send random messages if we have been registered
        if (seconds >= 0 && confirmed) {
          // time to send a new random message
          if (registered > 0 &&
              (double)(time(NULL) - since_last_msg_sent) >= (double)seconds) {
            send_random("Hello!");
            since_last_msg_sent = time(NULL);
          }
        }
      }
    }

    // Remove the specified client from this clients directory.
    void remove_from_directory(size_t idx) {
          registered--;
          client_ips->remove(idx);
          ports->remove(idx);
    }

    // send the given message to the given client
    // returns 1 on failure, 0 on successful transmission.
    int send_to(size_t idx, Text* text) {
      int status;     // status code holder

      const char* msg = s->serialize(text);

      // send to client
      char client_port[6];
      sprintf(client_port, "%d", ports->get(idx));
      String* client_ip = client_ips->get(idx);

      // create socket to client
      struct addrinfo hints, *res;

      memset(&hints, 0, sizeof hints);
      hints.ai_family = AF_INET;
      hints.ai_socktype = SOCK_STREAM;

      // get client info
      status = getaddrinfo(client_ip->c_str(), client_port, &hints, &res);
      if (status < 0 || res == NULL) {
        printf("client %s: error getting info of client %s\n", ip->c_str(), client_ip->c_str());
        remove_from_directory(idx);
        return 1;
      }

      // get client sock
      int sock = socket(res->ai_family, res->ai_socktype, res->ai_protocol);
      if (sock < 0) {
        printf("client %s: error creating socket to client %s\n", ip->c_str(), client_ip->c_str());
        remove_from_directory(idx);
      }

      // connect sock to client
      status = connect(sock, res->ai_addr, res->ai_addrlen);
      if (status < 0) {
        printf("client %s: error connecting to client %s\n", ip->c_str(), client_ip->c_str());
        remove_from_directory(idx);
      }

      printf("\u001b[36;1mclient %s: sending message to client %s: \033[0m\t\"%s\"\n",
             ip->c_str(), client_ip->c_str(), text->msg_->c_str());
      status = send(sock, msg, strlen(msg), 0);
      if (status < 0) {
        printf("client %s: error sending message\n", ip->c_str());
        close(sock);
        remove_from_directory(idx);
      }
      if (status != strlen(msg)) {
        printf("client %s: not all bytes sent. expected to send %zu, sent %i\n",
               ip->c_str(), strlen(msg), status);
      }

      close(sock);

      return 0;
    }

    // send the given message to a random known ip
    int send_random(const char* msg) {
      time_t t;
      srand((unsigned)time(&t));

      size_t i = rand() % registered;

      send_to(i, createTextMessage(msg, i));

      return 0;
    }

    /**
    Create a message that can be sent with given contents and receiver.
    All messages will have the origin added in.
    **/
    Text* createTextMessage(const char* contents, int receiver) {
      Text* text = new Text(receiver, ++id, contents, ip->c_str());
      return text;
    }

    // teardown - close sockets
    void teardown() {
      close(sockfd);
      if (servfd) close(servfd);

      dead = true;

      printf("\033[0;31mclient %s shutting down \033[0m\n", ip->c_str());
    }
};
