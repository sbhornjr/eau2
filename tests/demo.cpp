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
void producer(KChunkMap* kc) {

    DoubleColumn* dc = new DoubleColumn(kc);
    for (size_t i = 100 * 1000; i > 0; --i) {
      dc->push_back((double)i);
    }
    dc->finalize();
    cout << "Double column created." << endl;

}

void counter(KChunkMap* kc) {

}

void summarizer(KChunkMap* kc) {

}

/**
  * Called in the event that the correct function/job
  * for this node could not be found.
  */
void unspecified(size_t num) {
  cout << "Cannot determine what to do for this node #" + this_node << endl;
}

int main(int argc, const char** argv) {
    // args: 0=./demo, 1=this_node, 2=num_nodes, 3=ip, 4=port 5=server_ip
    // if (argc != 4) {
    //    cout << "please run ./demo [node_id] [# nodes] [node_ip] [port] [server_ip]" << endl;
    //    exit(1);
    //}

    size_t this_node = stoi(argv[1]);
    size_t num_nodes = stoi(argv[2]);
    sockaddr_in my_ip = stoi(argv[3]);
    size_t port = stoi(argv[4]);
    sockaddr_in server_ip = aton(argv[5]);

    KChunkMap* kc = new KChunkMap(num_nodes, this_node);

    switch (this_node) {
      case 0: producer(kc); break;
      case 1: counter(kc); break;
      case 2: summarizer(kc); break;
      default: unspecified(this_node); break;
    }

    Schema scm;
    DataFrame* df = new DataFrame(scm, kc);
    df->add_column(dc);

    const char* ser_df = df->serialize(df);

    delete df;
    delete[] ser_df;
    delete kc;
}
