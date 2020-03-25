#include "trivial.h"
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
    Trivial t(0);
}

int main(int argc, const char** argv) {
    if (argc != 2) {
        cout << "please enter ./eau2 <filename>" << endl;
        exit(1);
    }

    cout << "running milestone 1 tests:" << endl << endl;
    milestone1(argv[1]);
    cout << "milestone 1 tests successful." << endl << endl << endl;

    cout << "running milestone 2 tests:" << endl << endl;
    milestone2();
    cout << "milestone 2 tests successful." << endl;

    return 0;
}
