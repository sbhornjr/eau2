#include "trivial.h"

#include <string>
#include <iostream>

using namespace std;

/** 
 * program that tests the trivial example.
 */
int main(int argc, const char** argv) {

    Trivial t(0);

    t.run();

    printf("trivial example complete and successfull.\n");

    return 0;
}