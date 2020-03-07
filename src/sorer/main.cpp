#include <string>
#include <iostream>
#include <stdexcept>
#include <vector>
#include <fstream>

using namespace std;

/**
 * prints out the column type of the given column based on the schema.
 * @arg args: vector of size 1 containing the given column
 * @arg schema: the schema of the file
 * authors: horn.s@husky.neu.edu, armani.a@husky.neu.edu
 */
void print_col_type(vector<size_t> args, vector<string> schema) {
  if (args[0] >= schema.size()) {
    cout << "error: invalid column for print_col_type" << endl;
    exit(1);
  }
  cout << schema[args[0]] << endl;
}

/**
 * prints out the value of the given column and row.
 * @arg args: vector of size 2 containing the given column and row
 * @arg database: the processed database of the given file
 * @arg schema: the schema of the file
 * authors: horn.s@husky.neu.edu, armani.a@husky.neu.edu
 */
void print_col_idx(vector<size_t> args, vector<vector<string>> database, vector<string> schema) {
  if (args[0] >= database.size() || args[1] >= database[0].size()) {
    cout << "error: invalid arguments for print_col_idx" << endl;
    exit(1);
  }
  if (schema[args[0]] == "STRING") {
    cout << "\"" << database[args[0]][args[1]] << "\"" << endl;
  } else {
    cout << database[args[0]][args[1]] << endl;
  }
}

/**
 * prints out the whether the value of the given column and row is missing
 * @arg args: vector of size 2 containing the given column and row
 * @arg database: the processed database of the given file
 * authors: horn.s@husky.neu.edu, armani.a@husky.neu.edu
 */
void is_missing_idx(vector<size_t> args, vector<vector<string>> database) {
  if (args[0] >= database.size() || args[1] >= database[0].size()) {
    cout << "error: invalid arguments for is_missing_idx" << endl;
    exit(1);
  }
  if (database[args[0]][args[1]] == "") {
    cout << 1 << endl;
  } else {
    cout << 0 << endl;
  }
}

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

/**
 * scans the first 500 lines and returns the row number
 * with the most valid fields
 * @arg filename: file to scan
 * @return: the row number of the first longest valid row
 * authors: horn.s@husky.neu.edu, armani.a@husky.neu.edu
 */
int find_golden_row(string filename) {
  int longest_line;
  int longest_line_length = 0;
  int longest_line_empties = 0;
  string line;
  ifstream file (filename);
  if (file.is_open()) {
    int lines_read = 0;
    // loop through first 500 lines in file
    while (getline(file, line) && lines_read < 500) {
      lines_read++;
      int fields = 0;
      int empty_fields = 0;
      bool well_formed = true;
      bool open_found = false;
      bool other_found = false;
      bool quotes_found = false;
      bool quotes_ended = false;
      bool space_found = false;
      // loop through characters in the line
      for (size_t i = 0; line[i] != 0; i++) {
        // found a field
        if (line[i] == '<' && !quotes_found) {
          fields++;
          open_found = true;
        }
        // found a space after another character and without quotes
        else if (other_found && !quotes_found && line[i] == ' ') {
          space_found = true;
        }
        // found quotes without seeing anything else
        else if (open_found && line[i] == '\"' && !other_found && !quotes_found) {
          quotes_found = true;
          other_found = true;
          open_found = false;
        }
        // found something after the opening of the field
        else if (open_found && !other_found && line[i] != ' ' && line[i] != '>') {
          other_found = true;
          open_found = false;
        }
        // found the end of the field
        else if (line[i] == '>' && !quotes_found) {
          if (!other_found) {
            empty_fields += 1;
          }
          other_found = false;
          space_found = false;
          open_found = false;
          quotes_found = false;
          quotes_ended = false;
        }
        // character after quote
        else if (quotes_ended && line[i] != ' ') {
          well_formed = false;
          break;
        }
        // found a quote after another -> quotes ended
        else if (quotes_found && line[i] == '\"') {
          quotes_ended = true;
          quotes_found = false;
        }
        // found a space in the middle of the field-> out of the running
        else if (space_found && !quotes_found) {
          well_formed = false;
          break;
        }
      }
      // this row was well formed and it's the longest row so far
      // OR: this row was well formed and it has the same amount of
      //     fields as the longest so far, with less empty fields
      if ((well_formed && fields > longest_line_length)
      || (well_formed && fields == longest_line_length &&
        empty_fields < longest_line_empties)) {

        longest_line_length = fields;
        longest_line = lines_read;
        longest_line_empties = empty_fields;
      }
    }
    file.close();
  } else {
    cout << "unable to open file" << endl;
    exit(1);
  }
  return longest_line;
}

/**
 * populates schema vector based on longest line's types
 * @arg filename: name of the data file
 * @arg schema: schema vector to build
 * @arg longest_line: row number of longest line in data file
 * authors: horn.s@husky.neu.edu, armani.a@husky.neu.edu
 */
void make_schema(string filename, vector<string> &schema, int longest_line) {
  string line;
  int line_no = 0;
  ifstream file_again (filename);
  if (file_again.is_open()) {
    // loop through lines of the file
    while (getline(file_again, line)) {
      line_no++;
      // have not reached the longest line yet
      if (line_no != longest_line) {
        continue;
      }
      // at longest line -> loop through characters
      for (size_t character = 0; line[character] != 0; character++) {
        string field = "";
        // found a field
        if(line[character] == '<') {
          // loop through the characters of this field
          while (line[character] != '>') {
            character++;
            // found a quote -> add to field regardless of spaces
            if (line[character] == '\"') {
              field += line[character];
              character++;
              while (line[character] != '\"') {
                field += line[character];
                character++;
              }
              field += line[character];
            } else if (line[character] != ' ' && line[character] != '>') {
              field += line[character];
            }
          }
          // Case: field is empty -> BOOL
          if (field.size() == 0) {
            schema.push_back("BOOL");
            continue;
          }
          // Case: found a + or - (FLOAT || INT) -> trim field
          if (field[0] == '+' || field[0] == '-') {
            field = field.substr(1, field.length());
          }
          // Case: found a period. (FLOAT || STRING)
          if (field.find('.') != string::npos) {
            // try stof -> succeed = float, fail = string
            try {
              stof(field);
              schema.push_back("FLOAT");
            } catch (...) {
              schema.push_back("STRING");
            }
          }

          // Case: no period (INT || STRING)
          else {
            // try stoi -> succeed = int || bool (0, 1), fail = string
            try {
              int n = stoi(field);
              if (n > 1) {
                schema.push_back("INT");
              } else {
                schema.push_back("BOOL");
              }
            } catch (...) {
              schema.push_back("STRING");
            }
          }
        }
      }
      break;
    }
  } else {
    cout << "unable to open file" << endl;
    exit(1);
  }
}

/**
 * generates the database of the file based on inferred schema.
 * @arg filename: name of the data file
 * @arg schema: the inferred schema field types
 * @arg database: to populate columns and rows
 * @arg from: byte to start from in file
 * @arg len: number of bytes to read
 * authors: horn.s@husky.neu.edu, armani.a@husky.neu.edu
 */
void generate_database(string filename, vector<string> schema,
  vector<vector<string>> &database, size_t from, size_t len) {

  // populate database with empty vectors for columns
  for (size_t i = 0; i < schema.size(); i++) {
    vector<string> v = {};
    database.push_back(v);
  }

  string line;
  size_t bytes_read = 0;
  // Boolean to keep track of if we started vectorizing.
  bool started = false;
  // if we start at beginning of file, no need to skip rows
  if (from == 0) {
    started = true;
  }
  ifstream file_three (filename);
  if (file_three.is_open()) {
    // loop through lines of the file
    while (getline(file_three, line)) {
      // skip rows if not starting at 0 (or not at from yet)
      if (!started) {
        // count the bytes in the line
        for (size_t i = 0; line[i] != 0; i++) {
          bytes_read++;
        }
        // if we have gotten to or past from, start at the next line
        if (bytes_read >= from) {
          bytes_read = bytes_read - from;
          started = true;
        }
        continue;
      }

      // if we can't get past this line because of len, end here
      if (bytes_read + line.length() > len) {
        break;
      }

      // this row is well formed so we can add to database
      bool well_formed = true;
      int num_field = 0;  // num of fields in this row
      vector<string> tmp; // fields in this row (before validating types)

      // loop through characters in this row
      for (size_t character = 0; line[character] != 0; character++) {
        // NOTE: every time we increment character, we increment bytes read
        bytes_read++;
        string field = "";
        // found a field
        if(line[character] == '<') {
          bool other_found = false;
          bool space_found = false;
          bool quotes_found = false;
          character++;
          bytes_read++;
          // loop through the field
          while (line[character] != '>') {
            // found a quote
            if (line[character] == '\"') {
              quotes_found = true;
              character++;
              bytes_read++;
              // while still in the quote, add all to field
              while (line[character] != '\"') {
                field += line[character];
                character++;
                bytes_read++;
              }
            }
            // we've found something in the field
            else if (line[character] != ' ' && line[character] != '>' && !space_found && !quotes_found) {
              other_found = true;
              field += line[character];
            }
            // found a space after value
            else if (line[character] == ' ' && other_found) {
              space_found = true;
            }
            // found a value and then a space and then another value -> not well formed
            else if (space_found) {
              well_formed = false;
            }
            // something found after quotes
            else if (quotes_found && line[character] != ' ') {
              well_formed = false;
            }
            // found end of field
            if (line[character] == '>') {
              other_found = false;
              space_found = false;
            }
            character++;
            bytes_read++;
          }

          // increment num of fields, add field to tmp
          num_field++;
          tmp.push_back(field);
        }
      }
      // something in the row is not well formed -> throw out row
      if (!well_formed) {
        continue;
      }

      // Only add in if it fits the schema.
      vector<string> validated;
      // Determines if we throw out the row or not.
      bool isValidated = true;
      // Insert into vector
      for (size_t i = 0; i < tmp.size(); i++) {
        // Case: missing -> add to validated
        if(tmp[i] == "") {
          validated.push_back(tmp[i]);
        }
        // Case: String, push back no matter what.
        else if(schema[i] == "STRING") {
          validated.push_back(tmp[i]);
        }
        // Case: Float, push back if one of <FLOAT> <INT> <BOOL>
        else if(schema[i] == "FLOAT") {
          try {
            stof(tmp[i]);
            string toAdd = "";
            toAdd += tmp[i];
            if (tmp[i].find('.') == string::npos) {
              toAdd += ".0";
            }
            validated.push_back(toAdd);
          } catch (...) {
            try {
              stoll(tmp[i]);
              string toAdd = "";
              toAdd += tmp[i];
              if (tmp[i].find('.') == string::npos) {
                toAdd += ".0";
              }
              validated.push_back(toAdd);
            } catch ( ...) {
              isValidated = false;
              break;
            }
          }
        }
        // Case: Int, push back if one of <INT> <BOOL>
        else if (schema[i] == "INT") {
          // Anything with a period is thrown out.
          if (tmp[i].find('.') != string::npos) {
            isValidated = false;
            break;
          }
          try {
            int n = stoi(tmp[i]);
            validated.push_back(to_string(n));
          } catch (...) {
            try {
              long long int n = stoll(tmp[i]);
              validated.push_back(to_string(n));
            } catch ( ...) {
              isValidated = false;
              break;
            }
          }
        }

        // Case: Bool, push back if one of <BOOL>
        else if (schema[i] == "BOOL") {
          if (tmp[i][0] == '1' || tmp[i][0] == '0') {
            validated.push_back(tmp[i]);
          } else {
            isValidated = false;
          }
        }
      } // End creation of validated vector
      if (isValidated) {
        // add all of tmp into database
        for (size_t i = 0; i < schema.size(); ++i) {
          if (i < validated.size()) {
            database[i].push_back(validated[i]);
          }
          // if everything was well formed to this point, then the
          // remaining can be missing values
          else {
            database[i].push_back("");
          }
        }
      }
      // clear temporary vectors
      tmp.clear();
      validated.clear();
    }
  } else {
    cout << "unable to open file" << endl;
    exit(1);
  }
}

/**
 * prints the database with schema for clarity
 * @arg database: generated database of file
 * @arg schema: schema of file
 * authors: horn.s@husky.neu.edu, armani.a@husky.neu.edu
 */
void print_database(vector<vector<string>> database, vector<string> schema) {
  // printing for testing purposes
  for (size_t i = 0; i < database.size(); i++) {
    cout << schema[i] << endl;
    for (size_t j = 0; j < database[i].size(); j++) {
      cout << database[i][j] << "\t";
    }
    cout << endl;
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

  // find line in first 500 with most amount of fields
  int longest_line = find_golden_row(filename);

  // the schema
  vector<string> schema;

  // set the schema
  make_schema(filename, schema, longest_line);

  // the database
  vector<vector<string>> database;

  // generate the database
  generate_database(filename, schema, database, from, len);

  // print_col_type
  if (func == 1) {
    print_col_type(func_args, schema);
  }
  // print_col_idx
  else if (func == 2) {
    print_col_idx(func_args, database, schema);
  }
  // is_missing_idx
  else if (func == 3) {
    is_missing_idx(func_args, database);
  }

  //print_database(database, schema);
}
