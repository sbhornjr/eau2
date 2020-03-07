build:
	g++ -std=c++11 -Wall server.cpp -o server
	g++ -std=c++11 -Wall client.cpp -o client

run:
	./client -ip 127.0.0.2 -port 8080 -serverip 127.0.0.1 &
	./client -ip 127.0.0.3 -port 8080 -serverip 127.0.0.1 &
	./client -ip 127.0.0.4 -port 8080 -serverip 127.0.0.1 &
	./server -ip 127.0.0.1 -port 8080