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
  * The previous submission demonstrated this with a Threaded Implementation.
  * In its current state, we were only able to get the server/client nodes
  * initialized without linking in a KV Store.
  * @authors armani.a@husky.neu.edu, horn.s@husky.neu.edu
  */

String* m = new String("main");
String* v = new String("verif");
String* c = new String("ck");
Key* mainK = new Key(m,0);
Key* verify = new Key(v,0);
Key* check = new Key(c,0);

void producer(KChunkMap* kc, NodeInfo* node_info, size_t num_nodes) {

    // Start network with server.
    Network* n = new Network(node_info, num_nodes);
    n->server_init();

/**
    // Store sum in a variable.
    double sum = 0;

    // Create Double Column
    DoubleColumn* dc = new DoubleColumn(kc);
    for (double i = 100 * 1000; i > 0; --i) {
      dc->push_back(i);
      sum += i;
    }
    dc->finalize();

    // Create first dataframe.
    Schema scm;
    DataFrame* df = new DataFrame(scm, kc);
    df->add_column(dc);

    // Create Double Column that stores the sum.
    DoubleColumn sumStorage = new DoubleColumn(kc);
    sumStorage->push_back(sum);

    // Create second dataframe.
    Schema scm2;
    DataFrame* df2 = new DataFrame(scm2, kc);
    df2->add_column(sumStorage);

    const char* ser_df = df->serialize(df);

    delete df;
    delete[] ser_df;
    delete kc;
    */
    delete n;
}

void counter(KChunkMap* kc, NodeInfo* node_info, size_t num_nodes,
                const char* server_adr, size_t server_port) {

  // Start client in network.
  Network* n = new Network(node_info, num_nodes);
  n->client_init(server_adr, server_port);
  //n->begin_receiving();
/*
  size_t SZ = 100*1000;
  DataFrame* v = getKVStore()->getAndWait(mainK);
  double sum = 0;
  for (size_t i = 0; i < SZ; ++i) {
    sum += v->get_double(0,i);
  }
  printf("The sum is %f", sum);
  delete v;
  df3 = df3->from_scalar(verify, getKVStore(), kc_, sum);
  */

  delete n;
}

void summarizer(KChunkMap* kc, NodeInfo* node_info, size_t num_nodes,
                const char* server_adr, size_t server_port) {


  // Start client in network.
  Network* n = new Network(node_info, num_nodes);
  n->client_init(server_adr, server_port);
  //n->begin_receiving();
                  /*
  DataFrame* result = getKVStore()->getAndWait(verify);
  DataFrame* expected = getKVStore()->getAndWait(check);
  printf(expected->get_double(0,0)==result->get_double(0,0) ? "SUCCESS\n":"FAILURE\n");
  delete result;
  delete expected;
  */
  delete n;
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
        cout << "Please run ./milestone3 [node_id] [# nodes] [node_ip] [port] [server_ip] [server_port]" << endl;
        exit(1);
    }

    size_t this_node = stoi(argv[1]);
    size_t num_nodes = stoi(argv[2]);
    sockaddr_in my_ip;
    size_t port = stoi(argv[4]);
    sockaddr_in server_ip;
    size_t server_port = stoi(argv[6]);

    inet_pton(AF_INET, argv[3], &my_ip.sin_addr);
    inet_pton(AF_INET, argv[5], &server_ip.sin_addr);

    NodeInfo* node_info = new NodeInfo();
    node_info->id = this_node;
    node_info->address = my_ip;
    node_info->address.sin_port = htons(port);

    NetworkThread nt(node_info, num_nodes, 100, argv[5], server_port);

  //  KChunkMap* kc = new KChunkMap(num_nodes, this_node);
  //  KDFMap* kv = new KDFMap(this_node, kc);

  //  DemoThread dt(this_node, 200, kv, kc);

    delete node_info;

    cout << "DONE" << endl;
}
