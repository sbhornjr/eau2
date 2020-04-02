// lang: CwC
#pragma once

#include "object.h"
#include "string.h"
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
			next_node = 0;
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
			return nullptr;
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
		ChunkArray* getValues() {
			return values_;
		}
};

/**
 * Represents a map containing Key-SerializedDataFrames key-value pairs.
 * @authors: horn.s@husky.neu.edu, armani.a@husky.neu.edu
 */
class KSMap: public Map {
	public:

		StringArray* values_;
		Serializer s;
		KChunkMap* chunk_map;

		KSMap(KChunkMap* chunk_map) : Map() {
			values_ = new StringArray();
		}

		~KSMap() {
			delete values_;
		}

		/**
		 * Gets the value at a specific key.
		 * @param key: the key whose value we want to get
		 * @returns the value that corresponds with the given key
		 */
		String* get(Key* key) {
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
			String* df = values_->get(ind);
			return df;
		}

		/**
		 * Gets the value at a specific key. Blocking.
		 * @param key: the key whose value we want to get
		 * @returns the value that corresponds with the given key
		 */
		String* getAndWait(Key* key) {
			int ind = -1;
			while (ind == -1) {
				for (size_t i = 0; i < size_; ++i) {
					if (key->equals(keys_->get(i))) {
						ind = i;
						break;
					}
				}
			}
			String* df = values_->get(ind);
			return df;
		}

		/**
		 * Sets the value at the specified key to the value.
		 * If the key already exists, its value is replaced.
		 * If the key does not exist, a key-value pair is created.
		 * @param key: the key whose value we want to set
		 * @param value: the value we want associated with the key
		 */
		void put(Key* key, String* value) {
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
		String* remove(Key* key) {
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
			String* df = values_->get(ind);
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
		StringArray* getValues() {
			return values_;
		}
};
