#include <string>
#include <iostream>
#include <stdexcept>
#include <vector>
#include <fstream>
#include "sorer.h"

using namespace std;

/**
 * prints out the column type of the given column based on the schema.
 * @arg args: vector of size 1 containing the given column
 * @arg schema: the schema of the file
 * authors: horn.s@husky.neu.edu, armani.a@husky.neu.edu
 */
void print_col_type(vector<size_t> args, DataFrame* df) {
  if (args[0] >= df->ncols()) {
    cout << "error: invalid column for print_col_type" << endl;
    exit(1);
  }
  Schema scm = df->get_schema();
  cout << scm.col_type(args[0]) << endl;
}

/**
 * prints out the value of the given column and row.
 * @arg args: vector of size 2 containing the given column and row
 * @arg database: the processed database of the given file
 * @arg schema: the schema of the file
 * authors: horn.s@husky.neu.edu, armani.a@husky.neu.edu
 */
void print_col_idx(vector<size_t> args, DataFrame* df) {
  if (args[0] >= df->ncols() || args[1] >= df->nrows()) {
    cout << "error: invalid arguments for print_col_idx" << endl;
    exit(1);
  }
  Schema scm = df->get_schema();
  if (scm.col_type(args[0]) == 'S') {
    cout << "\"" << df->get_string(args[0], args[1]) << "\"" << endl;
  } else if (scm.col_type(args[0]) == 'I') {
    cout << df->get_int(args[0], args[1]) << endl;
  }
  else if (scm.col_type(args[0]) == 'B') {
    cout << df->get_bool(args[0], args[1]) << endl;
  } else if (scm.col_type(args[0]) == 'F') {
    cout << df->get_float(args[0], args[1]) << endl;
  }
}

/**
 * prints out the whether the value of the given column and row is missing
 * @arg args: vector of size 2 containing the given column and row
 * @arg database: the processed database of the given file
 * authors: horn.s@husky.neu.edu, armani.a@husky.neu.edu
 *
void is_missing_idx(vector<size_t> args, DataFrame* df) {
  if (args[0] >= df.size() || args[1] >= database[0].size()) {
    cout << "error: invalid arguments for is_missing_idx" << endl;
    exit(1);
  }
  if (database[args[0]][args[1]] == "") {
    cout << 1 << endl;
  } else {
    cout << 0 << endl;
  }
}
*/

/**
 * parses the given arguments and checks that they're valid
 * @arg argc: arg count
 * @arg argv: arguments array
 * @arg filename: the name of the data file
 * @arg from: byte to start reading from in file
 * @arg len: number of bytes to read
 * @arg func: which function was passed
 *    (1 = print_col_type, 2 = print_col_idx, 3 = is_missing_idx)
 * @arg func_args: the arguments given for the given function
 * authors: horn.s@husky.neu.edu, armani.a@husky.neu.edu
 */
void parse_args(int argc, char** argv, string &filename, size_t &from,
  size_t &len, size_t &func, vector<size_t> &func_args) {

  // generic error message
  string error = "invalid input: string must be of form";
  error += "./sorer -f \"filename\" -from uint -len uint <-print_col_type uint,";
  error += "-print_col_idx uint uint, -is_missing_idx uint uint>";

  // boolean flags to see which arguments have been processed.
  // ensures no dupes and allows for default values.
  bool file_found = false;
  bool from_found = false;
  bool len_found = false;
  bool func_found = false;

  // loop through arguments
  for (int i = 1; argv[i] != 0; i++) {
    string str = argv[i];
    if (i + 1 == argc) {
      cout << "flags must be followed by 1+ argument(s)" << endl;
      exit(1);
    }
    // -f flag -> filename
    if (str == "-f" && !file_found) {
      file_found = true;
      i++;
      string arg = argv[i];
      filename = arg;
    }
    // -from flag -> from
    else if (str == "-from" && !from_found) {
      from_found = true;
      i++;
      string arg = argv[i];
      // try to convert to uint, if failed, invalid argument
      try {
        int n = stoi(arg);
        if (n < 0) {
          cout << "-from requires 1 uint argument" << endl;
          exit(1);
        }
        from = stoi(arg);
      } catch (...) {
        cout << "-from requires 1 uint argument" << endl;
        exit(1);
      }
    }
    // -len flag -> len
    else if (str == "-len" && !len_found) {
      len_found = true;
      i++;
      string arg = argv[i];
      // try to convert to uint, if failed, invalid argument
      try {
        int n = stoi(arg);
        if (n < 0) {
          cout << "-len requires 1 uint argument" << endl;
          exit(1);
        }
        len = stoi(arg);
      } catch (...) {
        cout << "-len requires 1 uint argument" << endl;
        exit(1);
      }
    }
    // -print_col_type flag -> func and func_args
    else if (str == "-print_col_type" && !func_found) {
      func_found = true;
      i++;
      func = 1;
      string arg = argv[i];
      // make sure function arguments are uints
      try {
        int n = stoi(arg);
        if (n < 0) {
          cout << "-print_col_type requires 1 uint argument" << endl;
          exit(1);
        }
        func_args.push_back(stoi(arg));
      } catch (...) {
        cout << "-print_col_type requires 1 uint argument" << endl;
        exit(1);
      }
    }
    // -print_col_idx flag -> func and func_args
    else if (str == "-print_col_idx" && !func_found) {
      if (i + 2 < argc) {
        func_found = true;
        func = 2;
        i++;
        string arg = argv[i];
        // make sure the arguments are uints
        try {
          int n = stoi(arg);
          if (n < 0) {
            cout << "-print_col_idx requires 2 uint arguments" << endl;
            exit(1);
          }
          func_args.push_back(stoi(arg));
          i++;
          arg = argv[i];
          n = stoi(arg);
          if (n < 0) {
            cout << "-print_col_idx requires 2 uint arguments" << endl;
            exit(1);
          }
          func_args.push_back(stoi(arg));
        } catch (...) {
          cout << "-print_col_idx requires 2 uint arguments" << endl;
          exit(1);
        }
      } else {
        cout << "-print_col_idx requires 2 arguments" << endl;
        exit(1);
      }
    }
    // -is_missing_idx -> func and func_args
    else if (str == "-is_missing_idx" && !func_found) {
      if (i + 2 < argc) {
        func_found = true;
        func = 3;
        i++;
        string arg = argv[i];
        // make sure arguments are uints
        try {
          int n = stoi(arg);
          if (n < 0) {
            cout << "-is_missing_idx requires 2 uint arguments" << endl;
            exit(1);
          }
          func_args.push_back(stoi(arg));
          i++;
          arg = argv[i];
          n = stoi(arg);
          if (n < 0) {
            cout << "-is_missing_idx requires 2 uint arguments" << endl;
            exit(1);
          }
          func_args.push_back(stoi(arg));
        } catch (...) {
          cout << "-is_missing_idx requires 2 uint arguments" << endl;
          exit(1);
        }
      } else {
        cout << "-is_missing_idx requires 2 arguments" << endl;
        exit(1);
      }
    }
    // unknown flag or argument
    else {
      cout << error << endl;
      exit(1);
    }
  }

  // either file not found or func not found -> error
  if (!file_found || !func_found) {
    cout << error << endl;
    exit(1);
  }

  // len not found -> default is size of file
  if (!len_found) {
    ifstream file_len (filename, ios::binary);
    streampos fsize = file_len.tellg();
    file_len.seekg(0, ios::end);
    fsize = file_len.tellg() - fsize;
    file_len.close();
    len = fsize;
  }
}

int main(int argc, char** argv) {

  // get command line arguments
  string filename = "";
  size_t from = 0;
  size_t len = 1000;
  size_t func = 0;
  vector<size_t> func_args;
  parse_args(argc, argv, filename, from, len, func, func_args);

  Sorer s(filename, from, len);

  DataFrame* df = s.generate_dataframe();

  df->print();

  // print_col_type
  if (func == 1) {
    print_col_type(func_args, df);
  }
  // print_col_idx
  else if (func == 2) {
    print_col_idx(func_args, df);
  }
  // is_missing_idx
  //else if (func == 3) {
  //  is_missing_idx(func_args, df);
  //}
}
