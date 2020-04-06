#include "helper.h"
#include "map.h"
#include "dataframe.h"
#include "sorer.h"
#include "network_impl.h"
#include <string>
#include <iostream>
#include <stdlib.h>

using namespace std;

/**
  * This file runs the Milestone 3 Demo with a Network Implementation.
  */
void producer(KChunkMap* kc, NodeInfo* node_info,
                size_t num_nodes, size_t this_node, size_t port) {
    Network* n = new Network(node_info, num_nodes, this_node);
    n->server_init(this_node, port);
    DoubleColumn* dc = new DoubleColumn(kc);
    for (size_t i = 100 * 1000; i > 0; --i) {
      dc->push_back((double)i);
    }
    dc->finalize();
    cout << "Double column created." << endl;

    Schema scm;
    DataFrame* df = new DataFrame(scm, kc);
    df->add_column(dc);

    const char* ser_df = df->serialize(df);

    delete df;
    delete[] ser_df;
    delete kc;
}

void counter(KChunkMap* kc, NodeInfo* node_info,
                size_t num_nodes, size_t this_node, size_t port,
                const char* server_adr, size_t server_port) {
  Network* n = new Network(node_info, num_nodes, this_node);
  n->client_init(this_node, port, server_adr, server_port);
}

void summarizer(KChunkMap* kc, NodeInfo* node_info,
                size_t num_nodes, size_t this_node, size_t port,
                const char* server_adr, size_t server_port) {
  Network* n = new Network(node_info, num_nodes, this_node);
  n->client_init(this_node, port, server_adr, server_port);
}

/**
  * Called in the event that the correct function/job
  * for this node could not be found.
  */
void unspecified(size_t num) {
  cout << "Cannot determine what to do for this node #" + num << endl;
}

int main(int argc, const char** argv) {
    if (argc != 7) {
        cout << "please run ./milestone3 [node_id] [# nodes] [node_ip] [port] [server_ip] [server_port]" << endl;
        exit(1);
    }

    size_t this_node = stoi(argv[1]);
    size_t num_nodes = stoi(argv[2]);
    sockaddr_in my_ip;// = stoi(argv[3]);
    size_t port = stoi(argv[4]);
    sockaddr_in server_ip;// = aton(argv[5]);
    size_t server_port = stoi(argv[6]);

    inet_pton(AF_INET, argv[3], &(my_ip.sin_addr));
    inet_pton(AF_INET, argv[5], &(server_ip.sin_addr));

    KChunkMap* kc = new KChunkMap(num_nodes, this_node);

    NodeInfo* node_info = new NodeInfo();
    node_info->id = this_node;
    node_info->address = my_ip;

    switch (this_node) {
      case 0:
        producer(kc, node_info, num_nodes, this_node, port);
        break;
      case 1:
        counter(kc, node_info, num_nodes, this_node, port, argv[5], server_port);
        break;
      case 2:
        summarizer(kc, node_info, num_nodes, this_node, port, argv[5], server_port);
        break;
      default:
        unspecified(this_node);
        break;
    }

}
