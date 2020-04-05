build:
	g++ -g -Wall -std=c++11 -pthread main.cpp -o eau2

test:
	./eau2 data.sor

valgr:
	valgrind --leak-check=full --show-leak-kinds=all ./eau2 data.sor
