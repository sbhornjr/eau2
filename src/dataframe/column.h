// lang::CwC

#pragma once
#include "string.h"
#include <math.h>
#include <stdarg.h>

using namespace std;

/** @designers: vitekj@me.com, course staff */

// class definitions
class IntColumn;
class BoolColumn;
class DoubleColumn;
class StringColumn;

size_t ARR_SIZE = 256 * 100;
size_t STRING_ARR_SIZE = 128;
size_t BOOL_ARR_SIZE = 1024;

/**************************************************************************
 * Column ::
 * Represents one column of a data frame which holds values of a single type.
 * This abstract class defines methods overriden in subclasses. There is
 * one subclass per element type. Columns are mutable, equality is pointer
 * equality.
 * @authors horn.s@husky.neu.edu, armani.a@husky.neu.edu
 */
class Column : public Object {
public:
    char type_; // one of 'I' (integer), 'B' (boolean), 'D' (double), 'S' (String*)

    /** Type converters: Return same column under its actual type, or
     *  nullptr if of the wrong type.  */

    // returns this Column as an IntColumn*, or nullptr if not an IntColumn*
    virtual IntColumn* as_int() {return nullptr;}

    // returns this Column as a BoolColumn*, or nullptr if not a BoolColumn*
    virtual BoolColumn* as_bool() {return nullptr;}

    // returns this Column as a DoubleColumn*, or nullptr if not a DoubleColumn*
    virtual DoubleColumn* as_double() {return nullptr;}

    // returns this Column as a StringColumn*, or nullptr if not a StringColumn*
    virtual StringColumn* as_string() {return nullptr;}

    /** Type appropriate push_back methods. Calling the wrong method is
     * undefined behavior. **/

    // if we're here, this means the method did not exist in the subclass
    // and it shouldn't have been called. assert false.
    virtual void push_back(int val) { assert(false); }
    virtual void push_back(bool val) { assert(false); }
    virtual void push_back(double val) { assert(false); }
    virtual void push_back(String* val) { assert(false); }

    virtual void delete_all() {}

    /** Returns the number of elements in the column. Pure virtual. */
    virtual size_t size() = 0;

    /** Return the type of this column as a char: 'S', 'B', 'I' and 'D'. */
    char get_type() {
        return type_;
    }

    /** set the type of this column
     * only should be called from the constructors of subclasses
     */
    void set_type_(char type) {
        type_ = type;
    }
};

/*************************************************************************
 * IntColumn::
 * Holds primitive int values, unwrapped.
 * @authors: horn.s@husky.neu.edu, armani.a@husky.neu.edu
 */
class IntColumn : public Column {
public:

    int** arr_;         // internal array of int arrays
    size_t num_arr_;    // number of int arrays in arr_
    size_t size_;       // number of ints total in arr_


    // default constructor - initialize as an empty IntColumn
    IntColumn() {
        set_type_('I');
        size_ = 0;
        num_arr_ = 0;
        arr_ = new int*[0];
    }

    /**
     * constructor with values given - initialize all values into arr_
     * @param n: number of ints in the args
     * @param ...: the ints, handled by va_list etc.
     */
    IntColumn(int n, ...) {
        set_type_('I');

        // each int* in arr_ will be of size 100
        int* ints = new int[ARR_SIZE];

        // set the number of num_arr_ we will have based on n
        if (n % ARR_SIZE == 0) num_arr_ = n / ARR_SIZE;
        else num_arr_ = (n / ARR_SIZE) + 1;

        // initialize arr_ and n
        arr_ = new int*[num_arr_];
        size_t sn = n;
        size_ = sn;

        size_t curr_arr = 0;    // the current int* we are at in arr_
        va_list args;           // args given
        va_start(args, n);

        // for loop to fill arr_
        for (size_t i = 0; i < sn; ++i) {
            // our array of size is filled - add to arr_
            if (i % ARR_SIZE == 0 && i != 0) {
                arr_[curr_arr] = ints;
                ++curr_arr;
                ints = new int[ARR_SIZE];
            }
            // add the current int to ints
            ints[i % ARR_SIZE] = va_arg(args, int);
        }
        // add the last array into arr_
        arr_[curr_arr] = ints;
        va_end(args);
    }

    // destructor - delete arr_ and its sub-arrays
    ~IntColumn() {
        for (size_t i = 0; i < num_arr_; ++i) {
            delete[] arr_[i];
        }
        delete[] arr_;
    }

    /**
     * gets the int at the given position in the column
     * @param idx: index of int to get
     * @returns the int at that index
     */
    int get(size_t idx) {
        assert(idx < size_);
        return arr_[idx / ARR_SIZE][idx % ARR_SIZE];
    }

    /**
     * turns this Column* into an IntColumn* (assuming it is one)
     * @returns this column as an IntColumn*
     */
    IntColumn* as_int() {
        return this;
    }

    /**
     * push the given val to the end of the column
     * @param val: int to push back
     */
    virtual void push_back(int val) {
        // the last int* in arr_ is full
        // copy int*s into a new int** - copies pointers but not payload
        if (size_ % ARR_SIZE == 0) {
            // increment size values
            ++size_;
            ++num_arr_;

            // create new int* and initialize with val at first idx
            int* ints = new int[ARR_SIZE];
            ints[0] = val;

            // set up a temp int**, overwrite arr_ with new int**
            int** tmp = arr_;
            arr_ = new int*[num_arr_];

            // for loop copy values from temp into arr_
            for (size_t i = 0; i < num_arr_ - 1; ++i) {
                arr_[i] = tmp[i];
            }

            // add new int* into arr_ and delete the temp
            arr_[num_arr_ - 1] = ints;
            delete[] tmp;
        // we have room in the last int* of arr_ - add the val
        } else {
            arr_[size_ / ARR_SIZE][size_ % ARR_SIZE] = val;
            ++size_;
        }
    }

    /**
     * replaces the int at given index with given int
     * @param idx: the index at which to place this value
     * @param val: val to put at the index
     */
    void set(size_t idx, int val) {
        assert(idx < size_);
        arr_[idx / ARR_SIZE][idx % ARR_SIZE] = val;
    }

    /**
     * get the amount of ints in the column
     * @returns the amount of ints in the column
     */
    size_t size() {
        return size_;
    }
};

// Other primitive column classes similar...

/*************************************************************************
 * BoolColumn::
 * Holds primitive bool values, unwrapped.
 * @authors horn.s@husky.neu.edu, armani.a@husky.neu.edu
 */
class BoolColumn : public Column {
public:

    bool** arr_;        // internal array of bool arrays
    size_t num_arr_;    // number of bool arrays in arr_
    size_t size_;       // number of bools total in arr_

    // default constructor - initialize as an empty BoolColumn
    BoolColumn() {
        set_type_('B');
        size_ = 0;
        num_arr_ = 0;
        arr_ = new bool*[0];
    }

    /**
     * constructor with values given - initialize all values into arr_
     * @param n: number of bools in the args
     * @param ...: the bools, handled by va_list etc.
     */
    BoolColumn(int n, ...) {
        set_type_('B');

        // each bool* in arr_ will be of size 10
        bool* bools = new bool[BOOL_ARR_SIZE];

        // set the number of num_arr_ we will have based on n
        if (n % BOOL_ARR_SIZE == 0) num_arr_ = n / BOOL_ARR_SIZE;
        else num_arr_ = (n / BOOL_ARR_SIZE) + 1;

        // initialize arr_ and n
        arr_ = new bool*[num_arr_];
        size_t sn = n;
        size_ = sn;

        size_t curr_arr_ = 0;   // the current bool* we are at in arr)
        va_list args;           // args given
        va_start(args, n);

        // for loop to fill arr_
        for (size_t i = 0; i < sn; ++i) {
            // our array of size is filled - add to arr_
            if (i % BOOL_ARR_SIZE == 0 && i != 0) {
                arr_[curr_arr_] = bools;
                ++curr_arr_;
                bools = new bool[BOOL_ARR_SIZE];
            }
            // add the current bool to bools
            bools[i % BOOL_ARR_SIZE] = va_arg(args, int);
        }
        // add the last array into arr_
        arr_[curr_arr_] = bools;
        va_end(args);
    }

    // destructor - delete arr_ and its sub-arrays
    ~BoolColumn() {
        for (size_t i = 0; i < num_arr_; ++i) {
            delete[] arr_[i];
        }
        delete[] arr_;
    }

    /**
     * gets the bool at the given position in the column
     * @param idx: index of bool to get
     * @returns the bool at that index
     */
    bool get(size_t idx) {
        assert(idx < size_);
        return arr_[idx / BOOL_ARR_SIZE][idx % BOOL_ARR_SIZE];
    }

    /**
     * turns this Column* into an BoolColumn* (assuming it is one)
     * @returns this column as an BoolColumn*
     */
    BoolColumn* as_bool() {
        return this;
    }

    /**
     * push the given val to the end of the column
     * @param val: bool to push back
     */
    virtual void push_back(bool val) {
        // the last bool* in arr_ is full
        // copy bool*s into a new bool** - copies pointers but not payload
        if (size_ % BOOL_ARR_SIZE == 0) {
            // increment size values
            ++size_;
            ++num_arr_;

            // create new bool* and initialize with val at first idx
            bool* bools = new bool[BOOL_ARR_SIZE];
            bools[0] = val;

            // set up a temp bool**, overwrite arr_ with new bool**
            bool** tmp = arr_;
            arr_ = new bool*[num_arr_];

            // for loop copy values from temp into arr_
            for (size_t i = 0; i < num_arr_ - 1; ++i) {
                arr_[i] = tmp[i];
            }

            // add new bool* into arr_ and delete the temp
            arr_[num_arr_ - 1] = bools;
            delete[] tmp;
        // we have room in the last bool* of arr_ - add the val
        } else {
            arr_[size_ / BOOL_ARR_SIZE][size_ % BOOL_ARR_SIZE] = val;
            ++size_;
        }
    }

    /**
     * replaces the bool at given index with given bool
     * @param idx: the index at which to place this value
     * @param val: val to put at the index
     */
    void set(size_t idx, bool val) {
        assert(idx < size_);
        arr_[idx / BOOL_ARR_SIZE][idx % BOOL_ARR_SIZE] = val;
    }

    /**
     * get the amount of ints in the column
     * @returns the amount of ints in the column
     */
    size_t size() {
        return size_;
    }
};

/*************************************************************************
 * DoubleColumn::
 * Holds primitive double values, unwrapped.
 * @authors horn.s@husky.neu.edu, armani.a@husky.neu.edu
 */
class DoubleColumn : public Column {
public:

    double** arr_;       // internal array of double arrays
    size_t num_arr_;    // number of double arrays in arr_
    size_t size_;       // number of doubles total in arr_

    // default constructor - initialize as empty DoubleColumn
    DoubleColumn() {
        set_type_('D');
        size_ = 0;
        num_arr_ = 0;
        arr_ = new double*[0];
    }

    /**
     * constructor with values given - initialize all values into arr_
     * @param n: number of doubles in the args
     * @param ...: the doubles, handled by va_list etc.
     */
    DoubleColumn(int n, ...) {
        set_type_('D');

        // each bool* in arr_ will be of size 10
        double* doubles = new double[ARR_SIZE];

        // set the number of num_arr_ we will have based on n
        if (n % ARR_SIZE == 0) num_arr_ = n / ARR_SIZE;
        else num_arr_ = (n / ARR_SIZE) + 1;

        // initialize arr_ and n
        arr_ = new double*[num_arr_];
        size_t sn = n;
        size_ = sn;

        size_t curr_arr_ = 0;   // the current double* we are at in arr_
        va_list args;           // args given
        va_start(args, n);

        // for loop to fill arr_
        for (size_t i = 0; i < sn; ++i) {
            // our array of size is filled - add to arr_
            if (i % ARR_SIZE == 0 && i != 0) {
                arr_[curr_arr_] = doubles;
                ++curr_arr_;
                doubles = new double[ARR_SIZE];
            }
            // add the current double into doubles
            doubles[i % ARR_SIZE] = va_arg(args, double);
        }
        // add the last array into arr_
        arr_[curr_arr_] = doubles;
        va_end(args);
    }

    // destructor - delete arr_ and its sub-arrays
    ~DoubleColumn() {
        for (size_t i = 0; i < num_arr_; ++i) {
            delete[] arr_[i];
        }
        delete[] arr_;
    }

    /**
     * gets the double at the given position in the column
     * @param idx: index of double to get
     * @returns the double at that index
     */
    double get(size_t idx) {
        assert(idx < size_);
        return arr_[idx / ARR_SIZE][idx % ARR_SIZE];
    }

    /**
     * turns this Column* into an DoubleColumn* (assuming it is one)
     * @returns this column as an DoubleColumn*
     */
    DoubleColumn* as_double() {
        return this;
    }

    /**
     * push the given val to the end of the column
     * @param val: double to push back
     */
    virtual void push_back(double val) {
        // the last double* in arr_ is full
        // copy double*s into a new double** - copies pointers but not payload
        if (size_ % ARR_SIZE == 0) {
            // increment size values
            ++size_;
            ++num_arr_;

            // create new double* and initialize val at first idx
            double* doubles = new double[ARR_SIZE];
            doubles[0] = val;

            // set up a temp double**, overwrite arr_ with new double**
            double** tmp = arr_;
            arr_ = new double*[num_arr_];

            // for loop to copy values from temp into arr_
            for (size_t i = 0; i < num_arr_ - 1; ++i) {
                arr_[i] = tmp[i];
            }

            // add new double* into arr_ and delete the temp
            arr_[num_arr_ - 1] = doubles;
            delete[] tmp;
        // we have room in the last double* of arr_ - add the val
        } else {
            arr_[size_ / ARR_SIZE][size_ % ARR_SIZE] = val;
            ++size_;
        }
    }

    /**
     * replaces the double at given index with given double
     * @param idx: the index at which to place this value
     * @param val: val to put at the index
     */
    void set(size_t idx, double val) {
        assert(idx < size_);
        arr_[idx / ARR_SIZE][idx % ARR_SIZE] = val;
    }

    /**
     * get the amount of doubles in the column
     * @returns the amount of doubles in the column
     */
    size_t size() {
        return size_;
    }
};

/*************************************************************************
 * StringColumn::
 * Holds string pointers. The strings are external.  Nullptr is a valid
 * value.
 * @authors horn.s@husky.neu.edu, armani.a@husky.neu.edu
 */
class StringColumn : public Column {
public:

    String*** arr_;     // internal array of String* arrays
    size_t num_arr_;    // number of String* arrays there are in arr_
    size_t size_;       // number of String*s total there are in arr_

    // default construtor - initialize as an empty StringColumn
    StringColumn() {
        set_type_('S');
        size_ = 0;
        num_arr_ = 0;
        arr_ = new String**[0];
    }

    /**
     * constructor with values given - initialize all values into arr_
     * @param n: number of String* in the args
     * @param ...: the Strings, handled by va_list etc.
     */
    StringColumn(int n, ...) {
        set_type_('S');

        // each String** in arr_ will be of size 10
        String** strings = new String*[STRING_ARR_SIZE];

        // set the number of num_arr_ we will have based on n
        if (n % STRING_ARR_SIZE == 0) num_arr_ = n / STRING_ARR_SIZE;
        else num_arr_ = (n / STRING_ARR_SIZE) + 1;

        // initialize arr_ and n
        arr_ = new String**[num_arr_];
        size_t sn = n;
        size_ = sn;

        size_t curr_arr_ = 0;   // the current String** we are at in arr_
        va_list args;           // args given
        va_start(args, n);

        // for loop to fill arr_
        for (size_t i = 0; i < sn; ++i) {
            // our array of size is filled - add to arr_
            if (i % STRING_ARR_SIZE == 0 && i != 0) {
                arr_[curr_arr_] = strings;
                ++curr_arr_;
                strings = new String*[STRING_ARR_SIZE];
            }
            // add the current String* to strings - copy value
            String* s = va_arg(args, String*);
            strings[i % STRING_ARR_SIZE] = s;
        }
        // add the last array into arr_
        arr_[curr_arr_] = strings;
        va_end(args);
    }

    // destructor - delete arr_, its sub-arrays, and their strings
    ~StringColumn() {
        for (size_t i = 0; i < num_arr_; ++i) {
            delete[] arr_[i];
        }
        delete[] arr_;
    }

    /** delete all strings */
    void delete_all() {
        for (size_t i = 0; i < num_arr_; ++i) {
            for (size_t j = 0; j < STRING_ARR_SIZE && (i * STRING_ARR_SIZE) + j < size_; ++j) {
                delete arr_[i][j];
            }
            delete[] arr_[i];
        }
        size_ = 0;
        num_arr_ = 0;
        delete[] arr_;
        arr_ = new String**[num_arr_];
    }

    /**
     * turns this Column* into an StringColumn* (assuming it is one)
     * @returns this column as an StringColumn*
     */
    StringColumn* as_string() {
        return this;
    }

    /** Returns the string at idx; undefined on invalid idx.
     * @param idx: index of String* to get
     * @returns the String* at that index
     */
    String* get(size_t idx) {
        assert(idx < size_);
        return arr_[idx / STRING_ARR_SIZE][idx % STRING_ARR_SIZE];
    }

    /**
     * Acquire ownership fo the string.
     * replaces the int at given index with given String*
     * @param idx: the index at which to place this value
     * @param val: val to put at the index
     */
    void set(size_t idx, String* val) {
        assert(idx < size_);
        String* s = val;
        delete arr_[idx / STRING_ARR_SIZE][idx % STRING_ARR_SIZE];
        arr_[idx / STRING_ARR_SIZE][idx % STRING_ARR_SIZE] = s;
    }

    /**
     * Acquire ownership fo the string.
     * replaces the int at given index with given String*
     * @param idx: the index at which to place this value
     * @param val: val to put at the index
     * @param del: indicates whether this string should be deleted
     */
    void set(size_t idx, String* val, bool del) {
        assert(idx < size_);
        String* s = val;
        if (del) delete arr_[idx / STRING_ARR_SIZE][idx % STRING_ARR_SIZE];
        arr_[idx / STRING_ARR_SIZE][idx % STRING_ARR_SIZE] = s;
    }

    /**
     * push the given val to the end of the column
     * @param val: String* to push back
     */
    void push_back(String* val) {
        // the last String** in arr_ is full
        // copy String**s into a new String*** - copies pointers but not payload
        if (size_ % STRING_ARR_SIZE == 0) {
            // increment size values
            //std::cout << size_ << std::endl;
            ++size_;
            ++num_arr_;

            // create new String** and initialize with val at first idx
            String** strings = new String*[STRING_ARR_SIZE];
            String* s = val;
            strings[0] = s;

            // set up a temp String***, overwrite arr_ with new String***
            String*** tmp = arr_;
            arr_ = new String**[num_arr_];

            // for loop to copy values from temp into arr_
            for (size_t i = 0; i < num_arr_ - 1; ++i) {
                arr_[i] = tmp[i];
            }

            // add new String** into arr_ and delete the temp
            arr_[num_arr_ - 1] = strings;
            delete[] tmp;
        // we have room in the last String** of arr_ - add the val
        } else {
            String* s = val;
            arr_[size_ / STRING_ARR_SIZE][size_ % STRING_ARR_SIZE] = s;
            ++size_;
        }
    }

    /**
     * get the amount of Strings in the column
     * @returns the amount of Strings in the column
     */
    size_t size() {
        return size_;
    }
};
