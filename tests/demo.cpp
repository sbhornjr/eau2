#include "demo.h"
#include "map.h"
#include "dataframe.h"
#include "helper.h"
#include "sorer.h"
#include "serial.h"
#include "message.h"
#include <string>
#include <iostream>
#include <stdlib.h>

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

    cout << "int column created" << endl;

    DoubleColumn* dc = new DoubleColumn(kc);
    for (size_t i = 500 * 256; i > 0; --i) {
      dc->push_back((double)i);
    }
    dc->finalize();

    cout << "double column created" << endl;

    StringColumn* sc = new StringColumn(kc);
    for (size_t i = 500 * 256; i > 0; --i) {
      sc->push_back(new String(to_string((int)i).c_str()));
    }
    sc->finalize();

    cout << "string column created" << endl;

    BoolColumn* bc = new BoolColumn(kc);
    bool b = false;
    for (size_t i = 500 * 256; i > 0; --i) {
      bc->push_back(b);
      b = !b;
    }
    bc->finalize();

    cout << "bool column created" << endl;

    Schema scm;
    DataFrame* df = new DataFrame(scm, kc);
    df->add_column(ic);
    df->add_column(dc);
    df->add_column(sc);
    df->add_column(bc);

    //for (size_t i = 0; i < df->nrows(); ++i) {
    //  cout << df->get_int(0, i) << '\t' << df->get_double(1, i) << '\t' << df->get_string(2, i)->c_str() << '\t' << df->get_bool(3, i) << endl;
    //}

    //Serializer s;
    const char* ser_df = df->serialize(df);

    DataFrame* df2 = df->get_dataframe(ser_df);

    //for (size_t i = 0; i < df2->nrows(); ++i) {
    //  cout << df2->get_int(0, i) << '\t' << df2->get_double(1, i) << '\t' << df2->get_string(2, i)->c_str() << '\t' << df2->get_bool(3, i) << endl;
    //}

    delete df;
    delete[] ser_df;
    delete df2;
    delete kc;
}
