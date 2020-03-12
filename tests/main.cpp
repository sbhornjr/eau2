#include "dataframe/dataframe.h"
#include "dataframe/row.h"
#include "helper.h"
#include "sorer/sorer.h"

#include <string>
#include <iostream>

using namespace std;

/** 
 * program that takes in a filename, creates a DF from it,
 * sums all ints and reverses all strings in the DF
 */
int main(int argc, const char** argv) {
    if (argc != 2) {
        cout << "please enter ./p1 <filename>" << endl;
        exit(1);
    }

    string filename(argv[1]);

    cout << "creating dataframe from file " << filename << endl << endl;

    Sorer s(filename);

    DataFrame* df = s.generate_dataframe();

    cout << "dataframe created:" << endl << endl;

    df->print();

    cout << endl << "summing all ints in the df:" << endl;

    SumRower sr;

    df->pmap(sr);

    cout << sr.getSum() << endl << endl;

    cout << "reversing all strings in the df:" << endl;

    ReverseRower rr;

    df->pmap(rr);

    df->print();

    delete df;

    return 0;
}