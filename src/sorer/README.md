PART 1 - SORER

We chose to implement sorer in c++.

Our implementation of sorer is defined by 5 major sections:
1. Argument Parsing

    We did not use an argument parsing library and elected to parse the
    arguments the old fashioned way: looping through the array of arguments.
    For our sorer, the flags and arguments can be entered in any order, as
    long as a flag is followed by its argument(s). No flags are necessary
    except for -f (filename) and exactly one of the three functions. from
    has a default of 0, and len has a default of the size of the entire file.
    We represent which function was chosen with an integer flag where 1 is
    print_col_type, 2 is print_col_idx, and 3 is is_missing_idx. We represent
    the arguments of those functions with a vector of size 1 for print_col_type
    and of size 2 for the other 2.

2. Finding the Golden Row

    To find rows within the first 500 rows that contain the most amount
    of valid fields, we scan through each of the rows. If we find a field in
    the row that is invalid (e.g. <h i>, <"hi"a>, etc.), then the row is not
    considered. Otherwise, the longest row with the least amount of empty
    fields is saved to a vector. The vector will later be used to differentiate
    between Integers and Booleans in schemas. (0's and 1's are both types).

3. Inferring a Schema

    To infer a schema, we loop through the file again until we get to the
    golden row. Once we do, we go through each of the fields to determine
    its type. We are guaranteed that each field is valid because we already
    checked for this in the previous step. If it has quotes, it is a string.
    If the field can be cast into a double (numbers with a '.'), then we say it
    is a double. If it can be cast into an int (numbers) then it is an int,
    unless it is a 0 or 1, then it is a boolean. Otherwise it is a string. We
    add each of these as strings ("STRING", "DOUBLE", etc.) into a vector
    called schema. The value in the vector at a given index corresponds to
    the schema of that column.

4. Generating the Database

    The database is a 2d vector containing a number of sub-vectors equal to
    the number of items in our schema (i.e. the number of columns). We go
    through each row in the file. First, we get all the fields into a temporary
    vector, making sure that all the fields are valid (including empty fields).
    Then, we check that vector to make sure that the fields match the schema
    (the indexes of the temp vector and the schema vector match up). If they do,
    we add into the database in order (i.e. the 2nd field gets placed into the
    second database column, etc.). If there are not enough fields in the temp
    vector but all the fields matched the schema up to that point, we say that
    the rest of the fields are missing by adding "" to those columns.

5. Executing the Function

    After we have our database, we can then execute the given functions.
    print_col_type can simply get the given column's type from the schema
    vector. The other two can access database[arg1][arg2] to get the desired
    object. print_col_idx prints that object, and is_missing_idx checks
    if that object is "".
