// lang: CwC
#pragma once

#include "object.h"
#include "string.h"
#include "dataframe.h"
#include "array.h"
#include <cstdio>

/**
 * Represents a map containing String-Object key-value pairs.
 * @authors: horn.s@husky.neu.edu, armani.a@husky.neu.edu
 */
class Map: public Object {
	public:

		StringArray* keys_;
		size_t size_;  // number of key-value pairs in this map

		Map() {
			keys_ = new StringArray();
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
		Object* getAndWait(String* key) {
			return nullptr;
		}

		/**
		 * Sets the value at the specified key to the value.
		 * If the key already exists, its value is replaced.
		 * If the key does not exist, a key-value pair is created.
		 * @param key: the key whose value we want to set
		 * @param value: the value we want associated with the key
		 */
		void put(String* key, Object* value) {}

		/**
		 * Removes value at the specified key and returns the removed object
		 * @param key: the key whose value we want to remove
		 * @returns the value that corresponds with the given key
		 */
		Object* remove(String* key) {
			return nullptr;
		}

		/**
		 * Gets all the keys of this map
		 * @returns the array of keys
		 */
		StringArray* getKeys() {
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
 * Represents a map containing String-String key-value pairs.
 * @authors: horn.s@husky.neu.edu, armani.a@husky.neu.edu
 */
class SSMap: public Map {
	public:

		StringArray* values_;

		SSMap() : Map() {
			values_ = new StringArray();
		}

		~SSMap() {
			values_->delete_all();
			delete values_;
		}

		/**
		 * Gets the value at a specific key.
		 * @param key: the key whose value we want to get
		 * @returns the value that corresponds with the given key
		 */
		String* get(String* key) {
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
			String* s = values_->get(ind);
			return s;
		}

		/**
		 * Gets the value at a specific key. Blocking.
		 * @param key: the key whose value we want to get
		 * @returns the value that corresponds with the given key
		 */
		String* getAndWait(String* key) {
			int ind = -1;
			while (ind != -1) {
				for (size_t i = 0; i < size_; ++i) {
					if (key->equals(keys_->get(i))) {
						ind = i;
						break;
					}
				}
			}
			String* s = values_->get(ind);
			return s;
		}

		/**
		 * Sets the value at the specified key to the value.
		 * If the key already exists, its value is replaced.
		 * If the key does not exist, a key-value pair is created.
		 * @param key: the key whose value we want to set
		 * @param value: the value we want associated with the key
		 */
		void put(String* key, String* value) {
			for (size_t i = 0; i < size_; ++i) {
				if (key->equals(keys_->get(i))) {
					values_->set(i, value);
					return;
				}
			}
			keys_->push_back(key);
			values_->push_back(value);
		}

		/**
		 * Removes value at the specified key and returns the removed string
		 * @param key: the key whose value we want to remove
		 * @returns the value that corresponds with the given key
		 */
		String* remove(String* key) {
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
			String* k = keys_->get(ind);
			String* v = values_->get(ind);
			keys_->remove(ind);
			values_->remove(ind);
			--size_;
			delete k;
			return v;
		}

		/**
		 * Gets all the keys of this map
		 * @returns the array of keys
		 */
		StringArray* getKeys() {
			return keys_;
		}

		/**
		 * Gets all the values of this map
		 * @returns the array of values
		 */
		StringArray* getValues() {
			return values_;
		}
};

/**
 * Represents a map containing String-DF key-value pairs.
 * @authors: horn.s@husky.neu.edu, armani.a@husky.neu.edu
 */
class SDFMap: public Map {
	public:

		DFArray* values_;

		SDFMap() : Map() {
			values_ = new DFArray();
		}

		~SDFMap() {
			delete values_;
		}

		/**
		 * Gets the value at a specific key.
		 * @param key: the key whose value we want to get
		 * @returns the value that corresponds with the given key
		 */
		DataFrame* get(String* key) {
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
		DataFrame* getAndWait(String* key) {
			int ind = -1;
			while (ind != -1) {
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
		void put(String* key, DataFrame* value) {
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
		DataFrame* remove(String* key) {
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
			String* k = keys_->get(ind);
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
		StringArray* getKeys() {
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
