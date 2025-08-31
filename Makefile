all: cpp python zig

cpp:
	cd cpp && clang++ -std=c++20 -O3 -g main.cpp && time ./a.out > ../solution_cpp.txt

cpp-perf: cpp
	cd cpp && perf record --call-graph dwarf ./a.out

python:
	cd python && time python main.py > ../solution_python.txt

zig:
	cd zig && zig build -Doptimize=ReleaseFast && time ./zig-out/bin/_1brc_henne > ../solution_zig.txt

zig-perf: zig
	cd zig && perf record --call-graph dwarf ./zig-out/bin/_1brc_henne

.PHONY: cpp python zig
