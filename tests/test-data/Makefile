build:
	g++ -g -Wall -std=c++11 -pthread main.cpp -o p1

test:
	./p1 data.sor

valgr:
	valgrind --leak-check=full ./p1 data.sor