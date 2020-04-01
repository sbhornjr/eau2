#include "demo.h"
#include "dataframe.h"
#include "helper.h"
#include "sorer.h"
#include "serial.h"
#include "message.h"
#include "map.h"
#include <string>
#include <iostream>

using namespace std;

int main(int argc, const char** argv) {
    // args: 0=./demo, 1=this_node, 2=num_nodes, 3=ip, 4=port 5=server_ip
    //if (argc != 4) {
    //    cout << "please run ./demo [node_id] [# nodes] [node_ip] [port] [server_ip]" << endl;
    //    exit(1);
    //}
    size_t this_node = stoi(argv[1]);
    size_t num_nodes = stoi(argv[2]);

    KChunkMap* kc = new KChunkMap(num_nodes, this_node);
    IntColumn* ic = new IntColumn(kc);
    for (size_t i = 0; i < 500 * 256; ++i) {
      ic->push_back(i);
    }
    ic->finalize();

    cout << "column created" << endl;

    IntColumn* ic2 = new IntColumn(kc);
    for (size_t i = 500 * 256; i > 0; --i) {
      ic2->push_back(i);
    }
    ic2->finalize();

    Schema scm;
    DataFrame* df = new DataFrame(scm, kc);
    df->add_column(ic);
    df->add_column(ic2);

  //  Serializer s;
  //  cout << s.serialize(df) << endl;
}
