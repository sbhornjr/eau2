build:
	clear
	clear
	docker build -t cs4500:0.1 .
	- rm -rf ./tests/test-data
	mkdir ./tests/test-data
	cd ./src/dataframe; cp *.h ../../tests/test-data
	cd ./src/network; cp *.h ../../tests/test-data
	cd ./src/util; cp *.h ../../tests/test-data
	cd ./src/app; cp *.h ../../tests/test-data
	cd ./tests; cp main.cpp demo.cpp Makefile ./test-data
	cd ./data; cp data.sor ../tests/test-data
	clear
	docker run -ti -v "`pwd`":/test cs4500:0.1 bash -c "cd test/tests/test-data; make build"

test:
	clear
	clear
	docker build -t cs4500:0.1 .
	- rm -rf ./tests/test-data
	mkdir ./tests/test-data
	cd ./src/dataframe; cp *.h ../../tests/test-data
	cd ./src/network; cp *.h ../../tests/test-data
	cd ./src/util; cp *.h ../../tests/test-data
	cd ./src/app; cp *.h ../../tests/test-data
	cd ./tests; cp main.cpp demo.cpp Makefile ./test-data
	cd ./data; cp data.sor ../tests/test-data
	clear
	docker run -ti -v "`pwd`":/test cs4500:0.1 bash -c "cd test/tests/test-data; make build && make test"

valgr:
	clear
	clear
	docker build -t cs4500:0.1 .
	- rm -rf ./tests/test-data
	mkdir ./tests/test-data
	cd ./src/dataframe; cp *.h ../../tests/test-data
	cd ./src/network; cp *.h ../../tests/test-data
	cd ./src/util; cp *.h ../../tests/test-data
	cd ./src/app; cp *.h ../../tests/test-data
	cd ./tests; cp main.cpp demo.cpp Makefile ./test-data
	cd ./data; cp data.sor ../tests/test-data
	clear
	docker run -ti -v "`pwd`":/test cs4500:0.1 bash -c "cd test/tests/test-data; make build && make valgr"

clean:
	rm -rf ./tests/test-data
