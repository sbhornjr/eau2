#include "helper.h"
#include "dataframe.h"
#include "sorer.h"
#include "kvstore.h"
#include "thread.h"
#include <string>
#include <iostream>
#include <stdlib.h>

using namespace std;

/**
  * This file runs the Milestone 3 Demo with a Network Implementation.
  * The previous submission demonstrated this with a Threaded Implementation.
  *
  * @authors armani.a@husky.neu.edu, horn.s@husky.neu.edu
  */

String* m = new String("main");
String* v = new String("verif");
String* c = new String("ck");
Key* mainK = new Key(m,0);
Key* verify = new Key(v,0);
Key* check = new Key(c,0);


// Variable declarations (to avoid passing many parameters around)
size_t this_node;
size_t num_nodes;
sockaddr_in my_ip;
size_t port;
sockaddr_in server_ip;
size_t server_port;
NodeInfo* node_info;
const char* server_ip_str;
KVStore* kv;

void producer() {
  cout << "Ran Producer" << endl;

  // Store sum in a variable.
  double sum = 0;
  double SZ = 100 * 1000;

  // Create Double Column
  DoubleColumn* dc = new DoubleColumn(kv);
  for (double i = SZ; i > 0; --i) {
    dc->push_back(i);
    sum += i;
  }

  cout << "Ran Producer 0.75 " << endl;
  dc->finalize();

  cout << "Ran Producer 1" << endl;


  // Create first dataframe.
  Schema scm;
  DataFrame* df = new DataFrame(scm, kv);
  df->add_column(dc);

  // Create Double Column that stores the sum.
  DoubleColumn* sumStorage = new DoubleColumn(kv);
  sumStorage->push_back(sum);
  sumStorage->finalize();

  // Create second dataframe.
  Schema scm2;
  DataFrame* df2 = new DataFrame(scm2, kv);
  df2->add_column(sumStorage);

  // Put serialized DF's into the KV store.
  const char* ser_df = df->serialize(df);
  const char* ser_df2 = df2->serialize(df2);
  kv->put(mainK, ser_df);
  kv->put(check, ser_df2);

  delete df;
  delete df2;
  delete kv;

  cout << "Finished Producer" << endl;

  delete ser_df;
  delete ser_df2;
}

void counter() {
  cout << "Ran Counter" << endl;

  size_t SZ = 100 * 1000;
  double sum = 0;

  // Grab dataframe belonging to mainK and do another summation.
  DataFrame* v = v->get_dataframe(kv->getAndWait(mainK));
  for (size_t i = 0; i < SZ; ++i) sum += v->get_double(0, i);
  printf("The sum is %f", sum);
  delete v;

  // Create Double Column that stores the sum.
  DoubleColumn* sumStorage = new DoubleColumn(kv);
  sumStorage->push_back(sum);
  sumStorage->finalize();

  // Create verify dataframe.
  Schema scm;
  DataFrame* df3 = new DataFrame(scm, kv);
  df3->add_column(sumStorage);

  // Put serialized dataframe into KV.
  const char* ser_df3 = df3->serialize(df3);
  kv->put(verify, ser_df3);

  delete ser_df3;
  delete df3;
  delete kv;
  cout << "Finished Counter" << endl;
}

void summarizer() {
  cout << "Ran Summarizer" << endl;

  // Pull out two dataframes from the KV.
  DataFrame* result = result->get_dataframe(kv->getAndWait(verify));
  DataFrame* expected = expected->get_dataframe(kv->getAndWait(check));
  printf(expected->get_double(0,0)==result->get_double(0,0) ? "SUCCESS\n":"FAILURE\n");

  delete result;
  delete expected;
  delete kv;
  cout << "Finished Summarizer, Press Ctrl-C" << endl;
}

void run(size_t this_node) {
  switch(this_node) {
    case 0:   producer();     break;
    case 1:   counter();      break;
    case 2:   summarizer();
   }
}

int main(int argc, const char** argv) {
    if (argc != 7) {
        cout << "Please run ./milestone3 [node_id] [# nodes] [node_ip] [port] [server_ip] [server_port]" << endl;
        exit(1);
    }

    // Read command line args and set values.
    this_node = stoi(argv[1]);
    num_nodes = stoi(argv[2]);
    inet_pton(AF_INET, argv[3], &my_ip.sin_addr);
    port = stoi(argv[4]);
    inet_pton(AF_INET, argv[5], &server_ip.sin_addr);
    server_ip_str = argv[5];
    server_port = stoi(argv[6]);

    node_info = new NodeInfo();
    node_info->id = this_node;
    node_info->address = my_ip;
    node_info->address.sin_port = htons(port);

    kv = new KVStore(node_info, num_nodes, this_node, server_ip_str, server_port);
    NetworkThread n1(kv);
    run(this_node);

    n1.join();
    cout << "DONE" << endl;
}
