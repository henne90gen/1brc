all: cpp python zig

cpp-build:
	cd cpp && clang++ -std=c++20 -O3 -g main.cpp

cpp-run: cpp-build
	cd cpp && time ./a.out > ../solution_cpp.txt

cpp-perf: cpp-build
	cd cpp && perf record --call-graph dwarf ./a.out

python-run:
	cd python && time python main.py > ../solution_python.txt

zig-build:
	cd zig && zig build -Doptimize=ReleaseFast

zig-run: zig-build
	cd zig && time ./zig-out/bin/_1brc_henne > ../solution_zig.txt

zig-perf: zig-build
	cd zig && perf record --call-graph dwarf ./zig-out/bin/_1brc_henne

bench: cpp-build zig-build
	hyperfine --warmup 3 './zig/zig-out/bin/_1brc_henne' './cpp/a.out'
