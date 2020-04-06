#pragma once
#include "object.h"
#include "message.h"
#include <unistd.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>

/* NodeInfo
 * Each node is identified by its node id and socket address.
 * authors: vitek@me.com, horn.s@husky.neu.edu, armani.a@husky.neu.edu
 */
class NodeInfo : public Object {
public:
  unsigned id;
  sockaddr_in address;
};

/* IP based network communication layer.
 * Each node has an index between 0 and num_nodes-1.
 * Each node has a socket and IP address.
 * authors: vitek@me.com, horn.s@husky.neu.edu, armani.a@husky.neu.edu
 */

class Network : public Object {
public:
  NodeInfo* nodes_;  // All nodes in the system.
  size_t this_node_; // Node index of this node.
  int sock_;         // Socket of this node.
  sockaddr_in ip_;   // IP address of this node.
  size_t num_nodes_;  // Number of nodes in the network.
  size_t msg_id_;    // Unique message id that will increment each time.

  // Close the socket of this node when we terminate.
  ~Network() {
    close(sock_);
  }

  Network(NodeInfo* n, size_t num_nodes, size_t this_node) {
    msg_id_ = 0;
    num_nodes_ = num_nodes;
    this_node_ = this_node;
    ip_ = n->address;
  }

  // Returns index of the node.
  virtual size_t index() { return this_node_; }

  // Start the master node 0.
  // Receive register messages from all of the clients.
  // When you have all of them registered, you have a directory to send to the
  // clients.
  void server_init(unsigned idx, unsigned port) {
    this_node_ = idx;
        printf("server tried binding\n");
    init_sock_(port);
    printf("server succeeded\n");
    nodes_ = new NodeInfo[num_nodes_];
    for(size_t i = 0; i < num_nodes_; ++i) nodes_[i].id = 0;
    nodes_[0].address = ip_;
    nodes_[0].id = 0;

    // Loop over nodes, cast their messages to registration messgaes.
    for(size_t i = 2; i <= num_nodes_; ++i) {
      Register* msg = dynamic_cast<Register*>(recv_m());
      nodes_[msg->sender()].id = msg->sender();
      nodes_[msg->sender()].address.sin_family = AF_INET;
      nodes_[msg->sender()].address.sin_addr = msg->client().sin_addr;
      nodes_[msg->sender()].address.sin_port = htons(msg->port());
    }

    // Create their ports and addresses to be sent off.
    size_t* ports = new size_t[num_nodes_ - 1];
    StringArray* addresses = new StringArray();
    for (size_t i = 0; i< num_nodes_ - 1; ++i) {
      ports[i] = ntohs(nodes_[i + 1].address.sin_port);
      addresses->push_back(new String(inet_ntoa(nodes_[i + 1].address.sin_addr)));
    }

    // Send directory to all clients.
    Directory ipd(index(), 0, msg_id_++, num_nodes_ - 1, ports, addresses);
    for (size_t i = 1; i < num_nodes_; ++i) {
      // Reset target to actual destination.
      ipd.target_ = i;
      send_m(&ipd);
      printf("Sent Directory to %zu\n", i);
    }
  }

  // Initialize a client node.
  void client_init(size_t idx, size_t port, const char* server_adr,
                   size_t server_port) {
    this_node_ = idx;
        printf("client %zu tried binding\n", idx);
    init_sock_(port);
            printf("client %zu succeeded\n", idx);
    nodes_ = new NodeInfo[1];
    nodes_[0].id = 0;
    nodes_[0].address.sin_family = AF_INET;
    nodes_[0].address.sin_port = htons(server_port);
    if(inet_pton(AF_INET,server_adr, &nodes_[0].address.sin_addr) <= 0)
      assert(false && "Invalid server IP address format");

    // Send a registration message.
    Register msg(idx, 0, msg_id_++, nodes_[0].address, port);
    send_m(&msg);

    // Receive a directory from server node.
    Directory* ipd = dynamic_cast<Directory*>(recv_m());
    //ipd->log(); // TODO!!!
    NodeInfo * nodes = new NodeInfo[num_nodes_];
    nodes[0] = nodes_[0];
    for (size_t i = 0; i < ipd->clients(); ++i) {
      nodes[i+1].id = i+1;
      nodes[i+1].address.sin_family = AF_INET;
      nodes[i+1].address.sin_port = htons(ipd->ports()[i]);
      if (inet_pton(AF_INET, ipd->addresses()->get(i)->c_str(),
                    &nodes[i+1].address.sin_addr) <= 0) {
        printf("Invalid IP directory-address found for node %zu", i+1);
        exit(1); // Teardown? TODO
      }
    }
    delete [] nodes_;
    nodes_ = nodes; // replace the existing nodes with new nodes.
    delete ipd;
  }

  // Create a socket and bind it.
  void init_sock_(size_t port) {
    std::cout << "port was" << port << endl;;
    assert((sock_ = socket(AF_INET, SOCK_STREAM, 0)) >=0);
    int opt = 1;
    assert(setsockopt(sock_,
                      SOL_SOCKET, SO_REUSEADDR,
                      &opt, sizeof(opt)) == 0);
    ip_.sin_family = AF_INET;
    ip_.sin_addr.s_addr = INADDR_ANY;
    ip_.sin_port = htons(port);
    assert(bind(sock_, (sockaddr*)&ip_, sizeof(ip_)) >= 0);
    assert(listen(sock_, 100) >= 0); // We can have 100 connections queued.
  }

  // Based on message target, create connection to appropriate server.
  // Then, serializes the message.
  void send_m(Message * msg) {
    NodeInfo & tgt = nodes_[msg->target()];
    int conn = socket(AF_INET, SOCK_STREAM, 0);
    assert(conn >= 0 && "Unable to make client socket.");
    if (connect(conn, (sockaddr*)&tgt.address, sizeof(tgt.address)) < 0) {
      printf("Unable to connect to remote node.");
      exit(1); // Teardown? TODO
    }
    MessageSerializer s;
    const char* buf = s.serialize(msg);
    size_t size = strlen(buf);
    send(conn, &size, sizeof(size_t), 0);
    send(conn, buf, size, 0);
    close(conn);
  }

  // Listens on the socket and when a message is available - reads it.
  // Message is deserialized and returned.
  Message* recv_m() {
    sockaddr_in sender;
    socklen_t addrlen = sizeof(sender);
    int req = accept(sock_, (sockaddr*) &sender, &addrlen);
    int size = 0;
    if(read(req, &size, sizeof(size_t)) == 0) {
      printf("Unable to read");
      exit(1); // Teardown? TODO
    }
    char* buf = new char[size];
    int rd = 0;
    while (rd != size) rd += read(req, buf + rd, size - rd);
    MessageSerializer s;
    Message* msg = s.get_message(buf);
    close(req);
    return msg;
  }
};
