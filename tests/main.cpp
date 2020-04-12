#include "trivial.h"
#include "dataframe.h"
#include "row.h"
#include "helper.h"
#include "sorer.h"
#include "serial.h"
#include "message.h"
#include <string>
#include <iostream>

using namespace std;

/**
 * creates a dataframe from an input test file using sorer,
 * sums all ints, and reverses all strings in the df.
 */
void milestone1(string filename) {

    KVStore* kv = new KVStore();
    cout << "Creating dataframe from file " << filename << "." << endl;
    Sorer s(filename, kv);
    DataFrame* df = s.generate_dataframe();
    cout << "Dataframe created." << endl;

    cout << "Summing all ints in the df: ";
    SumRower sr;
    df->map(sr);
    cout << sr.getSum() << "." << endl << endl;

    delete df;
    delete kv;
}

/**
 * tests the trivial example.
 */
void milestone2() {
    KVStore* masterKV = new KVStore();
    Trivial t(0, masterKV);
    delete masterKV;
}

/**
  * tests that key operations work as intended.
  */
void test_key() {
    String* tester = new String("TESTER");
    String* tester2 = new String("TESTER2");
    Key* k = new Key(tester, 0);

    cout << "Checking that we can retrieve key values." << endl;
    assert(k->getName()->equals(tester));
    assert(k->getHomeNode() == 0);

    cout << "Checking that we can set key values." << endl;
    k->setHomeNode(100);
    assert(k->getHomeNode() == 100);

    cout << "Checking that hashing works for key values.\n\n";
    Key* k2 = new Key(tester2, 0);
    assert(k->hash() != k2->hash());

    delete k;
    delete k2;
}

/**
 * tests serial and deserialization
 */
void test_serial() {
    String* adr1 = new String("127.0.0.1");
    String* adr2 = new String("127.0.0.2");
    String* adr3 = new String("127.0.0.3");
    String* adr4 = new String("127.0.0.4");
    String* blue = new String("blue");
    String* red = new String("red");
    String* green = new String("green");
    String* yellow = new String("yellow");
    String* purple = new String("purple");
    StringArray* colors = new StringArray(5, blue, red, green, yellow, purple);

    KVStore* kv = new KVStore();
    Serializer s;
    MessageSerializer msgs;
    ChunkSerializer chunks;
    ColumnSerializer cols(kv);

    cout << "Checking serialization and deserialization of StringArray." << endl;

    const char* serial_colors = s.serialize(colors);
    StringArray* deserial_colors = s.get_string_array(serial_colors);
    assert(colors->equals(deserial_colors));

    cout << "Checking serialization and deserialization of DoubleArray." << endl;

    DoubleArray* dubs = new DoubleArray(25, 1.1, 2.2, 3.3, 4.4, 5.5, 6.6, 7.7, 8.8,
                                        9.9, 10.0, 11.1, 12.2, 13.3, 14.4, 15.5,
                                        16.6, 17.7, 18.8, 19.9, 20.0, 21.1, 22.2,
                                        23.3, 24.4, 25.5);
    const char* serial_dubs = s.serialize(dubs);
    DoubleArray* deserial_dubs = s.get_double_array(serial_dubs);
    assert(dubs->equals(deserial_dubs));

    cout << "Checking serialization and deserialization of Ack Message." << endl;

    Ack* ack = new Ack(0, 2, 47);
    const char* serial_ack = msgs.serialize(ack);
    Message* des_msg1 = msgs.get_message(serial_ack);
    assert(des_msg1->kind_ == MsgKind::Ack);
    Ack* des_ack = dynamic_cast<Ack*>(des_msg1);
    assert(des_ack != nullptr);

    cout << "Checking serialization and deserialization of Register Message." << endl;

    struct sockaddr_in addr;
    struct in_addr adr;
    adr.s_addr = 287654;
    addr.sin_family = AF_INET;
    addr.sin_port = 80;
    addr.sin_addr = adr;
    memset(addr.sin_zero, 0, sizeof addr.sin_zero);
    Register* reg = new Register(0, 2, 47, addr, 80);
    const char* serial_reg = msgs.serialize(reg);
    Message* des_msg3 = msgs.get_message(serial_reg);
    assert(des_msg3->kind_ == MsgKind::Register);
    Register* des_reg = dynamic_cast<Register*>(des_msg3);
    assert(des_reg != nullptr);

    cout << "Checking serialization and deserialization of Directory Message." << endl;

    size_t ports[4];
    for (size_t i = 0; i < 4; ++i) {
        ports[i] = i + 80;
    }
    StringArray* addrs = new StringArray(4, adr1, adr2, adr3, adr4);
    Directory* dir = new Directory(0, 2, 47, 4, ports, addrs);
    const char* serial_dir = msgs.serialize(dir);
    Message* des_msg4 = msgs.get_message(serial_dir);
    assert(des_msg4->kind_ == MsgKind::Directory);
    Directory* des_dir = dynamic_cast<Directory*>(des_msg4);
    assert(des_dir != nullptr);

    cout << "Checking serialization and deserialization of Text Message." << endl;

    Text* text = new Text(0, 2, 47, "hello!", "127.0.0.2");
    const char* serial_text = msgs.serialize(text);
    Message* des_msg5 = msgs.get_message(serial_text);
    assert(des_msg5->kind_ == MsgKind::Text);
    Text* des_text = dynamic_cast<Text*>(des_msg5);
    assert(des_text != nullptr);

    cout << "Checking serialization and deserialization of Int Chunk." << endl;

    IntChunk* ichunk = new IntChunk();
    for (size_t i = 0; i < 100 * 256; ++i) {
      ichunk->push_back((int)i);
    }
    const char* serial_ichunk = chunks.serialize(ichunk);
    Chunk* des_chunk1 = chunks.get_chunk(serial_ichunk);
    assert(des_chunk1->type_ == 'I');
    IntChunk* des_ichunk = des_chunk1->as_int();
    assert(des_ichunk != nullptr);

    cout << "Checking serialization and deserialization of Double Chunk." << endl;

    DoubleChunk* dchunk = new DoubleChunk();
    for (size_t i = 0; i < 100 * 256; ++i) {
      dchunk->push_back((double)i);
    }
    const char* serial_dchunk = chunks.serialize(dchunk);
    Chunk* des_chunk2 = chunks.get_chunk(serial_dchunk);
    assert(des_chunk2->type_ == 'D');
    DoubleChunk* des_dchunk = des_chunk2->as_double();
    assert(des_dchunk != nullptr);

    cout << "Checking serialization and deserialization of Bool Chunk." << endl;

    bool b = true;
    BoolChunk* bchunk = new BoolChunk();
    for (size_t i = 0; i < 1024 * 256; ++i) {
      bchunk->push_back(b);
      b = !b;
    }
    const char* serial_bchunk = chunks.serialize(bchunk);
    Chunk* des_chunk3 = chunks.get_chunk(serial_bchunk);
    assert(des_chunk3->type_ == 'B');
    BoolChunk* des_bchunk = des_chunk3->as_bool();
    assert(des_bchunk != nullptr);

    cout << "Checking serialization and deserialization of String Chunk." << endl;

    StringChunk* schunk = new StringChunk();
    for (size_t i = 0; i < 100 * 256; ++i) {
      schunk->push_back(new String(to_string(i).c_str()));
    }
    const char* serial_schunk = chunks.serialize(schunk);
    Chunk* des_chunk4 = chunks.get_chunk(serial_schunk);
    assert(des_chunk4->type_ == 'S');
    StringChunk* des_schunk = des_chunk4->as_string();
    assert(des_schunk != nullptr);

    cout << "Checking serialization and deserialization of DataFrame with each Column type." << endl << endl;

    IntColumn* icol = new IntColumn(kv);
    DoubleColumn* dcol = new DoubleColumn(kv);
    BoolColumn* bcol = new BoolColumn(kv);
    StringColumn* scol = new StringColumn(kv);
    for (size_t i = 0; i < 500 * 256; ++i) {
      icol->push_back((int)i);
      dcol->push_back((double)i);
      bcol->push_back(b);
      scol->push_back(new String(to_string(i).c_str()));
      b = !b;
    }
    icol->finalize();
    dcol->finalize();
    bcol->finalize();
    scol->finalize();
    Schema scm;
    DataFrame* df = new DataFrame(scm, kv);
    df->add_column(icol);
    df->add_column(dcol);
    df->add_column(bcol);
    df->add_column(scol);
    const char* serial_df = df->serialize(df);
    DataFrame* df2 = df->get_dataframe(serial_df);
    assert(df2 != nullptr);

    delete blue;
    delete red;
    delete green;
    delete yellow;
    delete purple;
    delete colors;
    delete adr1;
    delete adr2;
    delete adr3;
    delete adr4;
    delete[] serial_colors;
    deserial_colors->delete_all();
    delete deserial_colors;
    delete dubs;
    delete[] serial_dubs;
    delete deserial_dubs;
    delete ack;
    delete[] serial_ack;
    delete des_msg1;
    delete reg;
    delete[] serial_reg;
    delete des_msg3;
    delete dir;
    delete[] serial_dir;
    delete des_msg4;
    delete addrs;
    delete text;
    delete[] serial_text;
    delete des_msg5;
    delete ichunk;
    delete[] serial_ichunk;
    delete des_chunk1;
    delete dchunk;
    delete[] serial_dchunk;
    delete des_chunk2;
    delete bchunk;
    delete[] serial_bchunk;
    delete des_chunk3;
    delete schunk;
    delete[] serial_schunk;
    delete des_chunk4;
    delete df;
    delete[] serial_df;
    delete df2;
    delete kv;
}

int main(int argc, const char** argv) {
    if (argc != 2) {
        cout << "please enter ./eau2 <filename>" << endl;
        exit(1);
    }

    cout << "\033[33mRUNNING MILESTONE 1 TESTS:\033[0m" << endl << endl;
    milestone1(argv[1]);
    cout << "\033[32mMilestone 1 tests successful.\033[0m" << endl << endl;

    cout << "\033[33mRUNNING MILESTONE 2 TESTS:\033[0m" << endl << endl;
    milestone2();
    cout << "\033[32mMilestone 2 tests successful.\033[0m" << endl << endl;

    cout << "\033[33mRUNNING KEY TESTS:\033[0m" << endl << endl;
    test_key();
    cout << "\033[32mKey tests successful.\033[0m" << endl << endl;

    cout << "\033[33mRUNNING SERIAL TESTS:\033[0m" << endl << endl;
    test_serial();
    cout << "\033[32mSerial tests successful.\033[0m" << endl << endl;

    return 0;
}
