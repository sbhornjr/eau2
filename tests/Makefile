build:
	g++ -g -Wall -std=c++11 -pthread main.cpp -o eau2
	g++ -g -Wall -std=c++11 -pthread demo.cpp -o milestone3

test:
	./eau2 data.sor
	./milestone3 -num_nodes 3

valgr:
	valgrind --leak-check=full --show-leak-kinds=all ./eau2 data.sor
