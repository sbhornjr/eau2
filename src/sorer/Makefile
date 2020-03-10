### THIS MAKEFILE IS SOLELY FOR THE TESTING OF SORER ITSELF
### AND SHOULD NEVER BE USED OUTSIDE OF DEV TESTING PURPOSES

build:
	clear
	clear
	g++ -Wall -std=c++11 main.cpp -o sorer

run:
	clear
	clear
	./sorer -f "data.sor" -from 0 -len 500 -print_col_type 0
	./sorer -f "data.sor" -from 0 -len 500 -print_col_idx 0 0

clean:
	rm sorer

test:
	clear
	clear
	# argument errors
	- ./sorer
	- ./sorer -f "data.sor" -from 30 -len 1000
	- ./sorer -f "data.sor" -from 20 -len 1000 -print_col_type
	- ./sorer -f "data.sor" -from 20 -len 1000 -print_col_type a
	- ./sorer -f "data.sor" -from 20 -len 1000 -print_col_type 1 2
	- ./sorer -f "data.sor" -from 20 -len 1000 -print_col_idx
	- ./sorer -f "data.sor" -from 20 -len 1000 -print_col_idx a
	- ./sorer -f "data.sor" -from 20 -len 1000 -print_col_idx 1
	- ./sorer -f "data.sor" -from 20 -len 1000 -print_col_idx 1 a
	- ./sorer -f "data.sor" -from 20 -len 1000 -print_col_idx a a
	- ./sorer -f "dat.sor" -from 20 -len 1000 -print_col_type 1
	- ./sorer -f "data.sor" -from -len 1000 -print_col_type 1
	- ./sorer -f "data.sor" -from a -len 1000 -print_col_type 1
	- ./sorer -f "data.sor" -from 0 -len -print_col_type 1
	- ./sorer -f "data.sor" -from 0 -len a -print_col_type 1
	- ./sorer -f -from -len 1000 -print_col_type 1
	- ./sorer -from -len 1000 -print_col_type 1
	- ./sorer -f "data.sor" -from -1 -len 10 -print_col_type 1
	- ./sorer -f "data.sor" -from 0 -len -1 -print_col_type 1
	- ./sorer -f "data.sor" -from 0 -len -print_col_type -1
	- ./sorer -f "data.sor" -from 20 -len 1000 -print_col_type 5
	- ./sorer -f "data.sor" -from 20 -len 1000 -print_col_idx 5 1
	- ./sorer -f "data.sor" -from 20 -len 1000 -print_col_idx 1 100
	## SHOULD PASS
	# omitted arguments
	./sorer -f "data.sor" -print_col_type 1
	./sorer -f "data.sor" -from 20 -print_col_idx 2 1
	./sorer -f "data.sor" -len 1000 -print_col_idx 1 1
	./sorer -f "data.sor" -from 0 -len 500 -print_col_type 0 # exp. BOOL
	./sorer -f "data.sor" -from 0 -len 500 -print_col_type 1 # exp. INT
	./sorer -f "data.sor" -from 0 -len 500 -print_col_type 2 # exp. STRING
	./sorer -f "data.sor" -from 0 -len 500 -print_col_type 3 # exp. FLOAT
	./sorer -f "data.sor" -from 0 -len 500 -print_col_type 4 # exp. INT

	./sorer -f "data.sor" -from 0 -len 500 -print_col_idx 0 0 # exp. 0
	./sorer -f "data.sor" -from 0 -len 500 -print_col_idx 1 2 # exp. ""
	./sorer -f "data.sor" -from 0 -len 500 -print_col_idx 2 6 # exp. "hello sir"
	./sorer -f "data.sor" -from 0 -len 500 -print_col_idx 3 7 # exp. -1.0
	./sorer -f "data.sor" -from 0 -len 500 -print_col_idx 4 1 # exp. +3

	./sorer -f "data.sor" -from 5 -len 500 -print_col_idx 0 0 # exp. 1
	./sorer -f "data.sor" -from 5 -len 500 -print_col_idx 1 2 # exp. 23
	./sorer -f "data.sor" -from 5 -len 500 -print_col_idx 2 6 # exp. "hi"
	./sorer -f "data.sor" -from 5 -len 500 -print_col_idx 3 2 # exp. 2.2
	./sorer -f "data.sor" -from 5 -len 500 -print_col_idx 4 1 # exp. 3

	./sorer -f "data.sor" -from 0 -len 330 -print_col_idx 0 0 # exp. 0
	./sorer -f "data.sor" -from 0 -len 330 -print_col_idx 1 2 # exp. ""
	./sorer -f "data.sor" -from 0 -len 330 -print_col_idx 2 6 # exp. "hello sir"
	./sorer -f "data.sor" -from 0 -len 330 -print_col_idx 3 2 # exp. -1.0
	./sorer -f "data.sor" -from 0 -len 330 -print_col_idx 4 1 # exp. +3
