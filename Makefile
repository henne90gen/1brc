build:
	clang++ -O3 -g main.cpp

run: build
	./a.out

perf: build
	perf record --call-graph dwarf ./a.out
