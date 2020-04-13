build:
	g++ -g -Wall -std=c++11 -pthread main.cpp -o eau2
	g++ -g -Wall -std=c++11 -pthread demo.cpp -o milestone3

test:
	./eau2 data.sor
	# Running Milestone 3 Tests:
	./milestone3 1 3 127.0.0.2 8081 127.0.0.1 8080 &
	./milestone3 2 3 127.0.0.3 8082 127.0.0.1 8080 &
	./milestone3 0 3 127.0.0.1 8080 127.0.0.1 8080
	# Milestone 3 Tests End

valgr:
	valgrind --leak-check=full --show-leak-kinds=all ./eau2 data.sor
