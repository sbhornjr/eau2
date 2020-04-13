// lang: CwC
#pragma once

#include "object.h"
#include "string.h"
#include "chunk.h"
#include <chrono>
#include <cstdio>
#include "message.h"
#include "key.h"
#include <unistd.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>

using namespace std;

class DataFrame;

/* NodeInfo
 * Each node is identified by its node id and socket address.
 * authors: vitek@me.com, horn.s@husky.neu.edu, armani.a@husky.neu.edu
 */
class NodeInfo : public Object {
public:
  unsigned id;
  sockaddr_in address;
};

/**
 * Represents a KVStore - a map containing Key-Object key-value pairs,
 * with Network functionality wrapped in it.
 * @authors: horn.s@husky.neu.edu, armani.a@husky.neu.edu
 */
class KVStore : public Object {
	public:

    NodeInfo* me_;
		KeyArray* keys_;
    StringArray* values_;
    size_t num_nodes_;
		size_t next_node_;
		size_t size_;
    Serializer s_;
    ChunkSerializer cs_;

    NodeInfo** nodes_;  // All nodes in the system.
    int sock_;         // Socket of this node.
    size_t msg_id_;    // Unique message id that will increment each time.

		KVStore() {
			keys_ = new KeyArray();
			size_ = 0;
      num_nodes_ = 1;
			next_node_ = 0;
			values_ = new StringArray();
      NodeInfo* ni = new NodeInfo();
      ni->id = 0;
      me_ = ni;
		}

    KVStore(NodeInfo* n, size_t num_nodes, size_t this_node,
            const char* server_adr, size_t server_port) {
			assert(num_nodes != 0);
      keys_ = new KeyArray();
			values_ = new StringArray();
			num_nodes_ = num_nodes;
			next_node_ = 0;

      me_ = n;
      msg_id_ = 0;

      if (n->id == 0) server_init();
      else client_init(server_adr, server_port);
		}

		// Destructor for Map
		~KVStore() {
			delete keys_;
      delete values_;
      delete me_;
		}

    size_t get_next_node() {
      return next_node_;
    }

    /** ----------------- MAP FUNCTIONALITY ----------------- **/

		// Returns the amount of entries in this map
		size_t size() {
			return size_;
		}

    size_t get_id() {
			size_t ms = chrono::duration_cast<chrono::nanoseconds>(chrono::system_clock::now().time_since_epoch()).count() % 86400000000000;
			return ms;
		}

		void kill(size_t col_id) {
			for (size_t i = 0; i < keys_->size(); ++i) {
				if (keys_->get(i)->getCreatorID() == col_id) {
					keys_->remove(i);
					values_->remove(i);
					--i;
				}
			}
		}

		/**
		 * Gets the chunk at a specific key.
		 * @param key: the key whose value we want to get
		 * @returns the value that corresponds with the given key
		 */
		Chunk* get_chunk(Key* key) {
			// this chunk is stored here
			if (key->getHomeNode() == index()) {
				int ind = -1;
				for (size_t i = 0; i < size_; ++i) {
					if (key->equals(keys_->get(i))) {
						ind = i;
						break;
					}
				}
				if (ind == -1) {
					return nullptr;
				}
				Chunk* chunk = cs_.get_chunk(values_->get(ind)->c_str());
				return chunk;
			}
			// TODO else request from network
			return nullptr;
		}

    /**
		 * Gets the dataframe at a specific key.
		 * @param key: the key whose value we want to get
		 * @returns the value that corresponds with the given key
		 */
		DataFrame* get_df(Key* key);

		/**
		 * Gets the value at a specific key. Blocking.
		 * @param key: the key whose value we want to get
		 * @returns the serialized value that corresponds with the given key
		 */
		const char* getAndWait(Key* k) {
      printf("GET AND WAIT CALLED by %zu\n", index());
      size_t to_node = k->getHomeNode();
      bool found = false;
      // No need for networking if key is in this node.
      if (to_node == index()) {
        while(!found) {
          for (size_t i = 0; i < keys_->size(); i++) {
            if (keys_->get(i)->equals(k)) {
              found = true;
              return values_->get(i)->c_str();
              printf("GET AND WAIT RETURNED FOR %zu\n", index());
            }
          }
        }
      } else {
        // Need to request from a different node.
        // TODO FIX BLOCKING
          Get g(index(), to_node, msg_id_++, k);
          send_m(&g);
          Reply* r = dynamic_cast<Reply*>(recv_m());
          return r->value_;
      }
		}

		/**
		 * Sets the value at the specified key to the value.
		 * If the key already exists, its value is replaced.
		 * If the key does not exist, a key-value pair is created.
		 * @param key: the key whose value we want to set
		 * @param value: the value we want associated with the key
		 */
		void put(Key* key, Chunk* value) {

      cout << "FAILED HERE 1" << endl;
			// choose which node this chunk will go to
			key->setHomeNode(get_next_node());
			++next_node_;

			if (next_node_ >= num_nodes_) next_node_ = 0;
			// does this chunk belong here
			if (key->getHomeNode() == index()) {
				// yes - add to map
				keys_->push_back(key);
        cout << "FAILED HERE 3" << endl;

        String* s =new String(cs_.serialize(value));
        cout << "FAILED HERE 4" << endl;

				values_->push_back(s);

				++size_;
			} else {
        cout << "FAILED HERE 2" << endl;
        Put p(index(), key->getHomeNode(), msg_id_++, key, cs_.serialize(value));
        send_m(&p);
			}
		}



    /**
		 * Sets the value at the specified key to the value.
		 * If the key already exists, its value is replaced.
		 * If the key does not exist, a key-value pair is created.
		 * @param key: the key whose value we want to set
		 * @param value: the value we want associated with the key
		 */
		void put(Key* key, DataFrame* value);

		/**
		 * Gets all the keys of this map
		 * @returns the array of keys
		 */
		KeyArray* getKeys() {
			return keys_;
		}

		/**
		 * Gets all the values of this map
		 * @returns the array of values
		 */
		StringArray* getValues() {
			return values_;
		}

    /** ----------------- NETWORK FUNCTIONALITY ----------------- **/

    // Returns index of the node.
    size_t index() { return me_->id; }

    // Returns the IP of this node.
    struct sockaddr_in getMyIP() { return me_->address; };

    // Returns the port of this node.
    size_t port() { return ntohs(me_->address.sin_port); }

    // Start the master node 0.
    // Receive register messages from all of the clients.
    // When you have all of them registered, you have a directory to send to the
    // clients.
    void server_init() {
      init_sock_();
      nodes_ = new NodeInfo*[num_nodes_];
      nodes_[0] = me_;
      for(size_t i = 1; i < num_nodes_; ++i) nodes_[i] = new NodeInfo(); //->id = 0;

      // Loop over nodes, cast their messages to registration messgaes.
      for(size_t i = 1; i < num_nodes_; ++i) {
        Register* msg = dynamic_cast<Register*>(recv_m());
        nodes_[msg->sender()]->id = msg->sender();
        nodes_[msg->sender()]->address.sin_family = AF_INET;
        nodes_[msg->sender()]->address.sin_addr = msg->client().sin_addr;
        nodes_[msg->sender()]->address.sin_port = msg->client().sin_port;
      }

      // Create their ports and addresses to be sent off.
      size_t* ports = new size_t[num_nodes_ - 1];
      StringArray* addresses = new StringArray();
      for (size_t i = 1; i < num_nodes_; ++i) {
        ports[i - 1] = ntohs(nodes_[i]->address.sin_port);
        addresses->push_back(new String(inet_ntoa(nodes_[i]->address.sin_addr)));
      }

      // Send directory to all clients.
      for (size_t i = 1; i < num_nodes_; ++i) {
        Directory* ipd = new Directory(index(), i, msg_id_++, num_nodes_ - 1, ports, addresses);
        send_m(ipd);
        delete ipd;
      }

      printf("Completed Server Initialization\n");
    }

    // Initialize a client node.
    void client_init(const char* server_adr, size_t server_port) {
      sleep(3); // Have clients wait until server is ready.
      init_sock_();

      nodes_ = new NodeInfo*[1];
      nodes_[0] = new NodeInfo();
      nodes_[0]->id = 0;
      nodes_[0]->address.sin_family = AF_INET;
      nodes_[0]->address.sin_port = htons(server_port);
      if(inet_pton(AF_INET, server_adr, &nodes_[0]->address.sin_addr) <= 0)
        assert(false && "Invalid server IP address format");

      // Send a registration message.
      Register msg(index(), 0, msg_id_++, getMyIP(), port());
      send_m(&msg);

      // Receive a directory from server node.
      Directory* ipd = dynamic_cast<Directory*>(recv_m());
      NodeInfo** nodes = new NodeInfo*[num_nodes_];
      nodes[0] = nodes_[0];
      for (size_t i = 0; i < ipd->clients(); ++i) {
        nodes[i+1] = new NodeInfo();
        nodes[i+1]->id = i+1;
        nodes[i+1]->address.sin_family = AF_INET;
        nodes[i+1]->address.sin_port = htons(ipd->ports()[i]);
        if (inet_pton(AF_INET, ipd->addresses()->get(i)->c_str(),
                      &nodes[i+1]->address.sin_addr) <= 0) {
          printf("Invalid IP directory-address found for node %zu", i+1);
          exit(1); // Teardown? TODO
        }
      }
      delete[] nodes_;
      nodes_ = nodes; // replace the existing nodes with new nodes.
      delete ipd;

      printf("Completed Client %zu Initialization\n", index());
    }

    // Create a socket and bind it.
    void init_sock_() {
      assert((sock_ = socket(AF_INET, SOCK_STREAM, 0)) >=0);
      int opt = 1;
      struct sockaddr_in addr;
      assert(setsockopt(sock_,
                        SOL_SOCKET, SO_REUSEADDR,
                        &opt, sizeof(opt)) == 0);
      addr.sin_family = AF_INET;
      addr.sin_addr.s_addr = INADDR_ANY;
      addr.sin_port = me_->address.sin_port;
      me_->address.sin_family = AF_INET;
      assert(bind(sock_, (sockaddr*)&addr, sizeof(addr)) >= 0);
      assert(listen(sock_, 100) >= 0); // We can have 100 connections queued.
    }

    // Based on message target, create connection to appropriate server.
    // Then, serializes the message.
    void send_m(Message * msg) {
      MessageSerializer s;
      NodeInfo* tgt = nodes_[msg->target()];
      int conn = socket(AF_INET, SOCK_STREAM, 0);
      assert(conn >= 0 && "Unable to make client socket.\n");

      if (connect(conn, (sockaddr*)&tgt->address, sizeof(tgt->address)) < 0) {
        printf("Error conecting: %s\n", strerror( errno ) );
        printf("Unable to connect to remote node.\n");
        exit(1); // Teardown? TODO
      }
      const char* buf = s.serialize(msg);
      size_t size = strlen(buf);
      printf("SIZE OF SENT MESSAGE WAS %zu\n", size);
      printf("\033[0;33mNode %zu Sent:\n%s\033[0m\n", index(), buf);
      send(conn, &size, sizeof(size_t), 0);
      send(conn, buf, size, 0);
      close(conn);
      delete[] buf;
    }

    // Listens on the socket and when a message is available - reads it.
    // Message is deserialized and returned.
    Message* recv_m() {
      sockaddr_in sender;
      socklen_t addrlen = sizeof(sender);
      int req = accept(sock_, (sockaddr*) &sender, &addrlen);
      size_t size = 0;
      if(read(req, &size, sizeof(size_t)) == 0) {
        printf("Unable to read");
        exit(1);
      }
      char buf[360000];
      size_t rd = 0;
      while (rd != size) rd += read(req, buf + rd, size - rd);
      MessageSerializer s;
      printf("\033[0;34mNode %zu Received:\n%s\033[0m\n", index(), buf);
      Message* msg = s.get_message(buf);
      close(req);
      return msg;
    }

    /**
      * Inserts into KV store. If on the same node, no need to contact the
      * network. Otherwise, send a Put message to the right node.
      */
    void put(Key* k, const char* value) {
      size_t to_node = k->getHomeNode();
      if (to_node == index()) {
        keys_->push_back(k);
        values_->push_back(new String(value));
      } else {
        Put p(index(), to_node, msg_id_++, k, value);
        send_m(&p);
      }
    }

    /* Returns the value stored for a key. If key does not belong to this
       node, contact the correct node via the network. */
    const char* get(Key* k) {
      cout << (k->getName()->c_str()) << endl;
      cout << k->getHomeNode() << endl;
      cout << k->getCreatorID() << endl;


      size_t to_node = k->getHomeNode();
      // No need for networking if key is in this node.
      if (to_node == index()) {
        for (size_t i = 0; i < keys_->size(); i++) {
          if (keys_->get(i)->equals(k)) {
            return values_->get(i)->c_str();
          }
        }
        return nullptr;
      } else {
        // Need to request from a different node.
        Get g(index(), to_node, msg_id_++, k);
        send_m(&g);
        Reply* r = dynamic_cast<Reply*>(recv_m());
              cout << "got 786" << endl;
        return r->value_;
      }
    }

    /**
      * In response to a get message, send a reply with the value for the
      * given key.
      */
    void reply(Key* k, size_t tgt) {
      const char* value = getAndWait(k);
      Reply r(index(), tgt, ++msg_id_, value);
      send_m(&r);
    }


    /**
      * Infinitely loops while looking for messages that arrive.
      * Depending on the message kind, perform some action.
      */
    void begin_receiving() {
      struct timeval tv;
      tv.tv_sec = 0;
      tv.tv_usec = 0;
      fd_set read_fds;

      int status;

      while(1) {
        FD_ZERO(&read_fds);
        FD_SET(sock_, &read_fds);
        status = select(sock_ + 1, &read_fds, NULL, NULL, &tv);
        if (status < 0) {
          printf("Error in select.");
          close(sock_);
          exit(1);
        }
        for (int i = 0; i <= sock_; ++i) {
          Message* received = recv_m();
          MsgKind kind = received->get_kind();
          if (kind == MsgKind::Get) {
            Get* g_received = dynamic_cast<Get*>(received);
            reply(g_received->get_key(), g_received->sender());
          } else if (kind == MsgKind::Put)
          {
            Put* p_received = dynamic_cast<Put*>(received);
            put(p_received->get_key(), p_received->get_value());
          }

        }
      }
    }
};
