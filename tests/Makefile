build:
	g++ -g -Wall -std=c++11 -pthread demo.cpp -o eau2 

test:
	./eau2 0 1

valgr:
	valgrind --leak-check=full --show-leak-kinds=all ./eau2 0 1
