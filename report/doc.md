# EAU2 Application
## Steven Horn & Arthur Armani

### Introduction:

The eau2 application will be used by customers to query large data frames for results in order to analyze big data. It will be able to read large (100+ GB) schema-on-read files and load the data into a data frame using distributed arrays. The data frame will be stored in several underlying nodes hidden in the network layer that use a distributed key-value store and communicate with one another to exchange chunks of data as necessary. It will employ a 3-layer architecture (network - data frame - customer-facing) described below. 

### Architecture:

The system will consist of three levels. 

The first level will be the network component that contains many nodes divvying up the data in a distributed key value store. This network of nodes can interact with one another to exchange or retrieve data.

The second level will consist of the data frames and distributed arrays to store the values. 

The third layer will be the customer facing layer. A data analyst can input a query and receive an answer based on computations that happen in the previous two layers.

### Implementation:

Note: Our implementation will be changing as the project develops. This is a basic outline of what we expect to have.

The Sorer class will handle reading in data from a schema-on-read file and determining a DataFrame schema. Four types of data will be supported: booleans, integers, strings, and doubles. Data will be read only once. Basic operations happening within Sorer would include determining the overall size of passed in data and splitting it up into smaller dataframes, retrieving a schema from data, and handling rows with missing values by discarding them.
* DataFrame* generate_dataframe(Schema s)

The DataFrame class will have a particular schema derived from the result of running Sorer on the data. Basic operations will include returning the schema, getting the number of rows and columns, adding rows and columns, getting and setting values, map to run through each row, filtering rows, and printing.
* size_t nrows(), size_t ncols()
* void add_row(Row r), void add_column(Column* col)
* int get_int(size_t col, size_t row), bool get_bool(...), float get_float(...), String* get_string(...)
* void set(size_t col, size_t row, int val), set(..., bool val), set(..., float val), set(..., String* val)
* void map(Rower r), DataFrame* filter(Rower r), void print()

The network will consist of various client nodes that are able to register with a server node and then exchange messages with other clients directly. They will each store part of a distributed key-value store, so exchanging chunks of data will be necessary. Data frames and messages will be able to be serialized to allow for easy travel through sockets.
* int share_dataframe_with_neighbor(Socket s)
* int kill_node(Socket s)
* void shutdown()

The Key-Value store will be a <String, DataFrame> mapping where keys can be any string value and values will be data frames or sections of data frames distributed among various nodes. The KV store will support put(k, v), which adds the mapping of <k, v>, or updates k’s value if it already exists; get(k), which gets the value corresponding to k if it exists; and getAndWait(k), which gets the value corresponding to k, and if k does not exist, it will wait until it does (i.e. it is blocking). Additionally, we support the removal of mappings with the remove(k) method.
* void put(String* key, DataFrame* value)
* DataFrame* get(String* key)
* DataFrame* getAndWait(String* key)
* DataFrame* remove(String* key)

The application will consist of customer-facing code that allows users to enter queries. The results of queries will be output to the user’s console (or whatever front-end they are accessing the eau2 application from). 
* <return type TBD> sendQuery(String query)

### Use cases:

We are not yet sure about what exact use-cases there will be for the final iteration of eau2, so we will instead show off use-cases of what we have now, which is basically reading from a file to create a dataframe and mapping a key to a dataframe.

Given a sor data file data.sor:

`Sorer s(“data.sor”);`

`DataFrame* df = s.generate_dataframe();`

`df->print();`

It is that simple; the Sorer object takes the data file and parses it to infer a schema. When the generate_dataframe() method is called, the Sorer will use the schema to create a data frame and then parses the entire file to populate the data frame with rows that match the schema. You can also give the Sorer other parameters to dictate where to start reading the file and how many bytes to read in the file:

`Sorer s(“data.sor”, 100, 1000);	will read the 1000 bytes of data.sor that come after byte 100`

`Sorer s(“data.sor”, 100, -1); 	will read the entire file starting at byte 100`

`Sorer s(“data.sor”, 0, -1);	will read the entire file starting at byte 0 (same as Sorer s(“data.sor”))`

In our second milestone, we have added the distributed key-value store.

Given a key in the form of some string, we can retrieve, set, or remove a dataframe mapping:

`DataFrame* df = kv.get(key);`

`SDFMap* m = new SDFMAP();`

`String* key1 = new String("FRAME1");`

`m.put(key1, new DataFrame(new Schema("")));`

`m.get(key1);`

`m.getAndWait(key1); // blocking`

`m.remove(key1); // removes mapping and returns DataFrame`

### Open questions:

_Customer Facing:_ How will the customer interface with the application and run queries? Will our application need to have some sort of front end? Will it just be input from stdin? What are some example queries?

_Networking/Data Frame:_ How will we divide the data between nodes? How many nodes should we have? How should we implement updating values in a dataframe (version numbers)?

_File Reading:_ How can we improve the performance of creating a dataframe from gigantic files? (Possible solution is to introduce threading)

### Status:

[Week of March 9] Our project is currently in the planning and preparation stage. Technical debt from the individual components such as the networking layer, the dataframe, and the schema-on-read parser has been resolved. We are able to read from large files and store reasonably sized dataframes. Operations can be carried out on the dataframes such as summation and filtering. At the moment, the network layer is not connected to a distributed Key-Value store. We plan to connect all the individual parts together to form the three layer architecture in the coming week. After that, we will likely need to make performance improvements and do thorough load testing with multi-gigabyte data sets.

[Week of March 16] Our project now has the Key-Value store with operations such as get, put, remove, and wait_and_get, taking in a key and returning a dataframe. We have improved the performance of our system significantly, halving execution time. In order to arrive at this performance boost, we removed the need to copy strings in our columns and arrays. We have changed one of the datatypes - floats - to now be doubles. In getting the trivial example to work for numbers with decimals, we noticed that floats were too small and lost precision after a certain threshold. Thus, doubles allowed us to maintain precision. In changing the datatypes, we had to modify our implementation of schema, columns, serialization, and arrays. The trivial example works for integers as well. We discovered a memory management bug with valgrind where empty schemas (character arrays with a null terminator) were not being appended to correctly. This bug was fixed by splitting our add_column method in schema into two cases (empty schema/non-empty schema). 

