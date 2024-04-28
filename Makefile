run:
	clang++ -O3 -g main.cpp && ./a.out

perf:
	clang++ main.cpp && perf record --call-graph dwarf ./a.out
