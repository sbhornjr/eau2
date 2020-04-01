// lang: CwC
#pragma once

#include "object.h"
#include "string.h"
#include "dataframe.h"
#include "array.h"
#include "serial.h"
#include "chunk.h"
#include <cstdio>

/**
 * Represents a map containing String-Object key-value pairs.
 * @authors: horn.s@husky.neu.edu, armani.a@husky.neu.edu
 */
class Map: public Object {
	public:

		KeyArray* keys_;
		size_t size_;  // number of key-value pairs in this map

		Map() {
			keys_ = new KeyArray();
			size_ = 0;
		}

		// Destructor for Map
		~Map() {
			keys_->delete_all();
			delete keys_;
		}

		// Returns the amount of entries in this map
		size_t size() {
			return size_;
		}

		/**
		 * Gets the value at a specific key.
		 * @param key: the key whose value we want to get
		 * @returns the value that corresponds with the given key
		 */
		Object* get(String* key) {
			return nullptr;
		}

		/**
		 * Gets the value at a specific key. Blocking
		 * @param key: the key whose value we want to get
		 * @returns the value that corresponds with the given key
		 */
		Object* getAndWait(Key* key) {
			return nullptr;
		}

		/**
		 * Sets the value at the specified key to the value.
		 * If the key already exists, its value is replaced.
		 * If the key does not exist, a key-value pair is created.
		 * @param key: the key whose value we want to set
		 * @param value: the value we want associated with the key
		 */
		void put(Key* key, Object* value) {}

		/**
		 * Removes value at the specified key and returns the removed object
		 * @param key: the key whose value we want to remove
		 * @returns the value that corresponds with the given key
		 */
		Object* remove(Key* key) {
			return nullptr;
		}

		/**
		 * Gets all the keys of this map
		 * @returns the array of keys
		 */
		KeyArray* getKeys() {
			return keys_;
		}

		/**
		 * Gets all the values of this map
		 * @returns the array of values
		 */
		Array* getValues() {
			return nullptr;
		}

		/**
		 * Checks if this map is equal to another object
		 * @param o: the object to check equality for
		 * @returns whether the given object is equal to this map
		 */
		bool equals(Object* o) {
			if (o == nullptr) {
				return false;
			}
			Map* target = dynamic_cast<Map*>(o);
			if (target == nullptr) {
				return false;
			}
			if (size_ != target->size()) return false;
			for (size_t i = 0; i < size_; ++i) {
				bool kequal = keys_->get(i)->equals(target->keys_->get(i));
				if (!kequal) return false;
			}
			return true;
		}

		/**
		 * Gets a hashcode value of this map.
		 * @returns the hashcode value
		 */
		size_t hash() {
			size_t hash = 0;
			for (size_t i = 0; i < size_; ++i) {
				hash += keys_->get(i)->hash();
			}
			return hash;
		}
};

/**
 * Represents a map containing Key-Chunk key-value pairs.
 * @authors: horn.s@husky.neu.edu, armani.a@husky.neu.edu
 */
class KChunkMap: public Map {
	public:

		ChunkArray* values_;
		Serializer s;
		size_t num_nodes;
		size_t next_node;
		size_t this_node;
		size_t next_id;

		KChunkMap(size_t num_nodes_, size_t this_node_) : Map() {
			assert(num_nodes_ != 0);
			values_ = new ChunkArray();
			num_nodes = num_nodes_;
			this_node = this_node_;
			next_id = 0;
		}

		~KChunkMap() {
			delete values_;
		}

		size_t get_id() {
			return next_id++;
		}

		/**
		 * Gets the value at a specific key.
		 * @param key: the key whose value we want to get
		 * @returns the value that corresponds with the given key
		 */
		Chunk* get(Key* key) {
			// this chunk is stored here
			if (key->getHomeNode() == this_node) {
				int ind = -1;
				for (size_t i = 0; i < size_; ++i) {
					if (key->equals(keys_->get(i))) {
						ind = i;
						break;
					}
				}
				if (ind == -1) {
					return nullptr;
				}
				Chunk* chunk = values_->get(ind);
				return chunk;
			}
			// TODO else request from network
		}

		/**
		 * Gets the value at a specific key. Blocking.
		 * @param key: the key whose value we want to get
		 * @returns the value that corresponds with the given key
		 */
		Chunk* getAndWait(Key* key) {
			// TODO

		}

		/**
		 * Sets the value at the specified key to the value.
		 * If the key already exists, its value is replaced.
		 * If the key does not exist, a key-value pair is created.
		 * @param key: the key whose value we want to set
		 * @param value: the value we want associated with the key
		 */
		void put(Key* key, Chunk* value) {
			// choose which node this chunk will go to
			key->setHomeNode(next_node);
			++next_node;
			if (next_node == num_nodes) next_node = 0;

			// does this chunk belong here
			if (key->getHomeNode() == this_node) {
				cout << "adding to map" << key->name_->c_str() << endl;
				// yes - add to map
				keys_->push_back(key);
				values_->push_back(value);
				++size_;
			} else {
				// TODO: no - send to correct node
			}
		}

		/**
		 * Removes value at the specified key and returns the removed string
		 * @param key: the key whose value we want to remove
		 * @returns the value that corresponds with the given key
		 */
		Chunk* remove(Key* key) {
			// TODO

		}

		/**
		 * Gets all the keys of this map
		 * @returns the array of keys
		 */
		KeyArray* getKeys() {
			return keys_;
		}

		/**
		 * Gets all the values of this map
		 * @returns the array of values
		 */
		ChunkArray* getValues() {
			return values_;
		}
};

/*************************************************************************
 * DFArray::
 * Holds DF pointers. The strings are external.  Nullptr is a valid
 * value.
 * @authors horn.s@husky.neu.edu, armani.a@husky.neu.edu
 */
class DFArray : public Array {
public:

		DataFrame*** arr_;     // internal array of DataFrame* arrays
		size_t num_arr_;    // number of DataFrame* arrays there are in arr_
		size_t size_;       // number of DataFrame*s total there are in arr_

		// default construtor - initialize as an empty StringArray
		DFArray() {
				set_type_('D');
				size_ = 0;
				num_arr_ = 0;
				arr_ = new DataFrame**[0];
		}

		// destructor - delete arr_, its sub-arrays, and their dfs
		~DFArray() {
				for (size_t i = 0; i < num_arr_; ++i) {
						delete[] arr_[i];
				}
				delete[] arr_;
		}

		/** returns true if this is equal to that */
		bool equals(Object* that) {
				if (that == this) return true;
				DFArray* x = dynamic_cast<DFArray*>(that);
				if (x == nullptr) return false;
				if (size_ != x->size_) return false;
				for (size_t i = 0; i < size_; ++i) {
						if (get(i) != x->get(i)) return false;
				}
				return true;
		}

		/** gets the hash code value */
		size_t hash() {
				size_t hash = 0;
				for (size_t i = 0; i < size_; ++i) {
						hash += 17;
				}
				return hash;
		}

		/**
		 * turns this Array* into an DFArray* (assuming it is one)
		 * @returns this Array as an DFArray*
		 */
		DFArray* as_df() {
				return this;
		}

		/** Returns the df at idx; undefined on invalid idx.
		 * @param idx: index of DF* to get
		 * @returns the DF* at that index
		 */
		DataFrame* get(size_t idx) {
				assert(idx < size_);
				return arr_[idx / STRING_ARR_SIZE][idx % STRING_ARR_SIZE];
		}

		/**
		 * Acquire ownership for the df.
		 * replaces the DF* at given index with given DF*
		 * @param idx: the index at which to place this value
		 * @param val: val to put at the index
		 */
		void set(size_t idx, DataFrame* val) {
				assert(idx < size_);
				arr_[idx / STRING_ARR_SIZE][idx % STRING_ARR_SIZE] = val;
		}

		/**
		 * push the given val to the end of the Array
		 * @param val: DF* to push back
		 */
		void push_back(DataFrame* val) {
				// the last DF** in arr_ is full
				// copy DF**s into a new DF*** - copies pointers but not payload
				if (size_ % STRING_ARR_SIZE == 0) {
						// increment size values
						++size_;
						++num_arr_;

						// create new DF** and initialize with val at first idx
						DataFrame** dfs = new DataFrame*[STRING_ARR_SIZE];
						dfs[0] = val;

						// set up a temp DF***, overwrite arr_ with new DF***
						DataFrame*** tmp = arr_;
						arr_ = new DataFrame**[num_arr_];

						// for loop to copy values from temp into arr_
						for (size_t i = 0; i < num_arr_ - 1; ++i) {
								arr_[i] = tmp[i];
						}

						// add new DF** into arr_ and delete the temp
						arr_[num_arr_ - 1] = dfs;
						delete[] tmp;
				// we have room in the last DF** of arr_ - add the val
				} else {
						arr_[size_ / STRING_ARR_SIZE][size_ % STRING_ARR_SIZE] = val;
						++size_;
				}
		}

		/** remove DF at given idx */
		void remove(size_t idx) {
				assert(idx < size_);
				if (idx == size_ - 1) {
						arr_[idx / STRING_ARR_SIZE][idx % STRING_ARR_SIZE] = NULL;
				} else {
						for (size_t i = idx; i < size_; ++i) {
								set(i, arr_[(i + 1) / STRING_ARR_SIZE][(i + 1) % STRING_ARR_SIZE]);
						}
				}
				--size_;
				if (size_ % STRING_ARR_SIZE == 0) --num_arr_;
		}

		/**
		 * get the amount of DFs in the Array
		 * @returns the amount of DFs in the Array
		 */
		size_t size() {
				return size_;
		}
};

/**
 * Represents a map containing Key-DF key-value pairs.
 * @authors: horn.s@husky.neu.edu, armani.a@husky.neu.edu
 */
class KDFMap: public Map {
	public:

		DFArray* values_;
		Serializer s;
		KChunkMap* chunk_map;

		KDFMap(KChunkMap* chunk_map) : Map() {
			values_ = new DFArray();
		}

		~KDFMap() {
			delete values_;
		}

		/**
		 * Gets the value at a specific key.
		 * @param key: the key whose value we want to get
		 * @returns the value that corresponds with the given key
		 */
		DataFrame* get(Key* key) {
			int ind = -1;
			for (size_t i = 0; i < size_; ++i) {
				if (key->equals(keys_->get(i))) {
					ind = i;
					break;
				}
			}
			if (ind == -1) {
				return nullptr;
			}
			DataFrame* df = values_->get(ind);
			return df;
		}

		/**
		 * Gets the value at a specific key. Blocking.
		 * @param key: the key whose value we want to get
		 * @returns the value that corresponds with the given key
		 */
		DataFrame* getAndWait(Key* key) {
			int ind = -1;
			while (ind == -1) {
				for (size_t i = 0; i < size_; ++i) {
					if (key->equals(keys_->get(i))) {
						ind = i;
						break;
					}
				}
			}
			DataFrame* df = values_->get(ind);
			return df;
		}

		/**
		 * Sets the value at the specified key to the value.
		 * If the key already exists, its value is replaced.
		 * If the key does not exist, a key-value pair is created.
		 * @param key: the key whose value we want to set
		 * @param value: the value we want associated with the key
		 */
		void put(Key* key, DataFrame* value) {
			for (size_t i = 0; i < size_; ++i) {
				if (key->equals(keys_->get(i))) {
					values_->set(i, value);
					return;
				}
			}
			keys_->push_back(key);
			values_->push_back(value);
			++size_;
		}

		/**
		 * Removes value at the specified key and returns the removed string
		 * @param key: the key whose value we want to remove
		 * @returns the value that corresponds with the given key
		 */
		DataFrame* remove(Key* key) {
			int ind = -1;
			for (size_t i = 0; i < size_; ++i) {
				if (key->equals(keys_->get(i))) {
					ind = i;
					break;
				}
			}
			if (ind == -1) {
				printf("ERROR: key not found.");
				exit(1);
			}
			Key* k = keys_->get(ind);
			DataFrame* df = values_->get(ind);
			keys_->remove(ind);
			values_->remove(ind);
			--size_;
			delete k;
			return df;
		}

		/**
		 * Gets all the keys of this map
		 * @returns the array of keys
		 */
		KeyArray* getKeys() {
			return keys_;
		}

		/**
		 * Gets all the values of this map
		 * @returns the array of values
		 */
		DFArray* getValues() {
			return values_;
		}
};
