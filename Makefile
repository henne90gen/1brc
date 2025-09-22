all: cpp-run python-run zig-run

python-run:
	cd python && time python main.py > ../solution_python.txt

cpp-build:
	cd cpp && clang++ -std=c++20 -O3 -ffast-math -march=native main.cpp -o brc

cpp-run: cpp-build
	cd cpp && time ./brc > ../solution_cpp.txt

cpp-perf: cpp-build
	cd cpp && perf record --call-graph dwarf ./brc

zig-build:
	cd zig && zig build -Doptimize=ReleaseFast

zig-run: zig-build
	cd zig && time ./zig-out/bin/brc > ../solution_zig.txt

zig-run-mmap: zig-build
	cd zig && time ./zig-out/bin/brc_mmap > ../solution_zig.txt

zig-perf-mmap: zig-build
	cd zig && perf record --call-graph dwarf ./zig-out/bin/brc_mmap

zig-perf: zig-build
	cd zig && perf record --call-graph dwarf ./zig-out/bin/brc

bench: cpp-build zig-build
	hyperfine --warmup 3 './zig/zig-out/bin/brc' './zig/zig-out/bin/brc_mmap' './cpp/brc'
