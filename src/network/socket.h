/**
  * An object that is part of networking infrastructure.
  * Can be extended to serve some function such as client/server.
  *
  * @authors armani.a@husky.neu.edu, horn.s@husky.neu.edu
**/
class Socket : public Object {
public:

    String* ip;               // my ip
    const char* port;         // the port i am listening on
    StringArray* client_ips;  // the ips of all registered clients
    IntArray* ports;          // client ports
    int sockfd;               // the socket to listen on
    size_t registered;        // number of registered clients
    fd_set read_fds;          // set of read_fds
    int fdmax;                // max fd for select
    Serializer* s;            // basic serializer
    size_t id;                // message id tracker
    bool dead;                // did we teardown

    // default constructor
    Socket()
    : ip(new String("127.0.0.1")),
      port("8080"),
      client_ips(new StringArray()),
      ports(new IntArray()),
      registered(0),
      s(new Serializer()),
      id(0),
      dead(false) {}

    // constructor that takes in an ip address for this server
    Socket(char* ip)
    : ip(new String(ip)),
      port("8080"),
      client_ips(new StringArray()),
      ports(new IntArray()),
      registered(0),
      s(new Serializer()),
      id(0),
      dead(false) {}

    // constructor that takes in an ip address and a port
    Socket(char* ip, char* port)
    : ip(new String(ip)),
      port(port),
      client_ips(new StringArray()),
      ports(new IntArray()),
      registered(0),
      s(new Serializer()),
      id(0),
      dead(false) {}

    // deconstructor. delete all relevant values
    ~Socket() {
      delete ip;
      delete client_ips;
      delete ports;
      delete s;
    }

    virtual void setup() = 0;
    virtual void run() = 0;
    virtual int register_client() = 0;
    virtual void teardown() = 0;
};
