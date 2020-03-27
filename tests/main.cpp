#include "trivial.h"
#include "demo.h"
#include "dataframe.h"
#include "row.h"
#include "helper.h"
#include "sorer.h"
#include <string>
#include <iostream>

using namespace std;

/**
 * creates a dataframe from an input test file using sorer,
 * sums all ints, and reverses all strings in the df.
 */
void milestone1(string filename) {

    cout << "creating dataframe from file " << filename << "." << endl << endl;
    Sorer s(filename);
    DataFrame* df = s.generate_dataframe();
    cout << "dataframe created." << endl << endl;

    cout << "summing all ints in the df: "; //<< endl;
    SumRower sr;
    df->pmap(sr);
    cout << sr.getSum() << "." << endl << endl;

    cout << "reversing all strings in the df." << endl << endl;
    ReverseRower rr;
    df->pmap(rr);

    delete df;
}

/**
 * tests the trivial example.
 */
void milestone2() {
    KDFMap* masterKV = new KDFMap();
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
    k->setName(tester2);
    k->setHomeNode(100);

    assert(k->getName()->equals(tester2));
    assert(k->getHomeNode() == 100);

    cout << "Checking that hashing works for key values.\n\n";
    Key* k2 = new Key(tester2, 0);
    assert(k->hash() != k2->hash());

    delete tester;
    delete tester2;
    delete k;
    delete k2;
}

/**
  * runs the example code given with a distributed kv store
  */
void milestone3() {
    KDFMap* masterKV = new KDFMap();
    DemoThread d1(0, 100, masterKV);
    DemoThread d2(1, 200, masterKV);
    DemoThread d3(2, 300, masterKV);
    delete masterKV;
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

    cout << "\033[33mRUNNING MILESTONE 3 TESTS:\033[0m" << endl << endl;
    milestone3();
    cout << "\033[32mMilestone 3 tests successful.\033[0m" << endl << endl;

    cout << "\033[33mRUNNING KEY TESTS:\033[0m" << endl << endl;
    test_key();
    cout << "\033[32mKey tests successful.\033[0m" << endl << endl;


    return 0;
}
