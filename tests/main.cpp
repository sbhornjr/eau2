#include "trivial.h"

#include <string>
#include <iostream>

using namespace std;

/**
 * program that tests the trivial example.
 */
int main(int argc, const char** argv) {

    Trivial t(0);

    t.run_int();

    printf("Trivial example for IntArray complete and successful.\n");

    t.run_double();

    printf("Trivial example for DoubleArray complete and successful.\n");

    return 0;
}
