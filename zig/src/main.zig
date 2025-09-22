const std = @import("std");

const multi_threaded = true;
const use_debug_allocator = true;
const maximum_buffer_capacity: usize = 128 * 1024 * 1024;
// const measurements_file_path = "/home/henne/Workspace/1brc/measurements-100M.txt";
const measurements_file_path = "/home/henne/Workspace/1brc/measurements-1B.txt";
// const measurements_file_path = "D:\\Workspace\\1brc\\measurements-1B.txt";

const StationSummary = struct {
    name: []const u8,
    min: i32 = 1000,
    max: i32 = -1000,
    sum: i64 = 0,
    count: i32 = 0,

    pub fn init(name: []const u8) StationSummary {
        return StationSummary{ .name = name };
    }

    pub fn update_temp(self: *StationSummary, temp: i32) void {
        self.count += 1;
        self.sum += @intCast(temp);
        if (temp > self.max) {
            self.max = temp;
        }
        if (temp < self.min) {
            self.min = temp;
        }
    }

    pub fn update_station(self: *StationSummary, other: *StationSummary) void {
        self.count += other.count;
        self.sum += other.sum;
        if (other.max > self.max) {
            self.max = other.max;
        }
        if (other.min < self.min) {
            self.min = other.min;
        }
    }

    pub fn average_temp(self: *StationSummary) f32 {
        const sum_f: f32 = @floatFromInt(self.sum);
        const count_f: f32 = @floatFromInt(self.count);
        return (sum_f / count_f) / 10.0;
    }
};

const StationSummaryHashMap = std.StringHashMap(StationSummary);

const Worker = struct {
    allocator: std.mem.Allocator,
    thread: std.Thread,
    buffer: []u8,
    buffer_size: usize,
    buffer_capacity: usize,
    stations: StationSummaryHashMap = undefined,

    processing: std.atomic.Value(bool) = std.atomic.Value(bool).init(false),
    done: std.atomic.Value(bool) = std.atomic.Value(bool).init(false),

    pub fn init(allocator: std.mem.Allocator, idx: usize, buffer_capacity: usize) !*Worker {
        var result = try allocator.create(Worker);
        result.allocator = allocator;
        result.buffer_capacity = buffer_capacity;
        result.buffer = try allocator.alloc(u8, buffer_capacity);
        result.stations = StationSummaryHashMap.init(allocator);
        result.thread = try std.Thread.spawn(.{}, Worker.run, .{result});

        const thread_name = try std.fmt.allocPrint(allocator, "Worker-{}", .{idx});
        try result.thread.setName(thread_name);

        return result;
    }

    pub fn run(self: *Worker) void {
        while (!self.done.load(.monotonic)) {
            if (!self.processing.load(.monotonic)) {
                continue;
            }

            process_chunk(self.buffer, self.buffer_size, &self.stations, self.allocator) catch {};

            self.processing.store(false, .monotonic);
        }
    }
};

fn process_chunk(buffer: []u8, buffer_size: usize, stations: *StationSummaryHashMap, gpa: std.mem.Allocator) !void {
    var current_pos: usize = 0;
    var temp: i32 = 0;
    var is_negative: bool = false;
    var station_name_length: usize = 0;
    var start_of_line: usize = current_pos;
    while (current_pos < buffer_size) {
        current_pos += 1;
        if (current_pos >= buffer_size) {
            break;
        }

        if (buffer[current_pos] == '\n') {
            if (is_negative) {
                temp *= -1;
            }

            const station_name = buffer[start_of_line .. start_of_line + station_name_length];

            var result = try stations.getOrPutAdapted(station_name, stations.ctx);
            if (!result.found_existing) {
                const station_name_copy = try gpa.alloc(u8, station_name_length);
                @memcpy(station_name_copy, station_name);
                result.value_ptr.* = StationSummary.init(station_name_copy);
                result.key_ptr.* = result.value_ptr.*.name;
            }

            result.value_ptr.update_temp(temp);

            temp = 0;
            is_negative = false;
            station_name_length = 0;
            start_of_line = current_pos + 1;
            continue;
        }

        if (buffer[current_pos] == ';') {
            station_name_length = current_pos - start_of_line;
            continue;
        }

        if (station_name_length == 0) {
            continue;
        }

        if (buffer[current_pos] == '.') {
            continue;
        }

        if (buffer[current_pos] == '-') {
            is_negative = true;
            continue;
        }

        temp *= 10;
        temp += @intCast(buffer[current_pos] - '0');
    }
}

pub fn main() !void {
    var debug_allocator = std.heap.DebugAllocator(.{}).init;
    defer {
        if (comptime use_debug_allocator) {
            const result = debug_allocator.deinit();
            switch (result) {
                .ok => {},
                .leak => {
                    std.debug.print("Memory leak detected\n", .{});
                },
            }
        }
    }

    var gpa: std.mem.Allocator = undefined;
    if (comptime use_debug_allocator) {
        gpa = debug_allocator.allocator();
    } else {
        gpa = std.heap.page_allocator;
    }

    const file = try std.fs.openFileAbsolute(measurements_file_path, .{});
    defer file.close();

    const parallel_executions = if (multi_threaded) try std.Thread.getCpuCount() else 1;
    const workers: []*Worker = try gpa.alloc(*Worker, parallel_executions);
    defer gpa.free(workers);
    const worker_arenas: []std.heap.ArenaAllocator = try gpa.alloc(std.heap.ArenaAllocator, parallel_executions);
    defer gpa.free(worker_arenas);

    const buffer_capacity = @min(maximum_buffer_capacity, try file.getEndPos() / parallel_executions);

    for (0..workers.len) |idx| {
        worker_arenas[idx] = std.heap.ArenaAllocator.init(gpa);
        workers[idx] = try Worker.init(worker_arenas[idx].allocator(), idx, buffer_capacity);
    }

    var copy_from_previous: usize = 0;
    var previous_buffer: []u8 = undefined;
    var previous_buffer_size: usize = 0;
    var total_bytes_read: usize = 0;
    while (true) {
        var selected_worker: ?*Worker = null;
        while (selected_worker == null) {
            for (workers) |worker| {
                if (!worker.processing.load(.monotonic)) {
                    selected_worker = worker;
                    break;
                }
            }
        }

        if (copy_from_previous != 0) {
            std.mem.copyForwards(u8, selected_worker.?.buffer, previous_buffer[previous_buffer_size - copy_from_previous .. previous_buffer_size]);
        }

        const expected_bytes_read = selected_worker.?.buffer_capacity - copy_from_previous;
        const bytes_read = try file.read(selected_worker.?.buffer[copy_from_previous..]);
        total_bytes_read += bytes_read;
        if (bytes_read != expected_bytes_read and total_bytes_read < try file.getEndPos()) {
            for (workers) |worker| {
                worker.done.store(true, .monotonic);
            }
            break;
        }

        selected_worker.?.buffer_size = bytes_read + copy_from_previous;

        selected_worker.?.processing.store(true, .monotonic);

        if (total_bytes_read >= try file.getEndPos()) {
            break;
        }

        var last_line_break: usize = selected_worker.?.buffer_size - 1;
        while (last_line_break > 0) : (last_line_break -= 1) {
            if (selected_worker.?.buffer[last_line_break] == '\n') {
                break;
            }
        }

        previous_buffer = selected_worker.?.buffer;
        previous_buffer_size = selected_worker.?.buffer_size;
        copy_from_previous = selected_worker.?.buffer_size - last_line_break - 1;
    }

    for (workers) |worker| {
        worker.done.store(true, .monotonic);
    }

    for (workers) |worker| {
        worker.thread.join();
    }

    var result_stations = StationSummaryHashMap.init(gpa);
    defer result_stations.deinit();
    for (workers) |worker| {
        var itr = worker.stations.valueIterator();
        while (itr.next()) |value| {
            const result = try result_stations.getOrPut(value.*.name);
            if (!result.found_existing) {
                result.value_ptr.* = StationSummary.init(value.*.name);
            }
            result.value_ptr.update_station(value);
        }
    }

    try print_results(gpa, &result_stations);

    for (worker_arenas) |arena| {
        arena.deinit();
    }
}

fn compareStrings(_: void, lhs: []const u8, rhs: []const u8) bool {
    return std.mem.order(u8, lhs, rhs).compare(std.math.CompareOperator.lt);
}

fn print_results(gpa: std.mem.Allocator, stations: *StationSummaryHashMap) !void {
    var keys = try std.ArrayList([]const u8).initCapacity(gpa, stations.count());
    defer keys.deinit(gpa);

    var itr = stations.iterator();
    while (itr.next()) |entry| {
        try keys.append(gpa, entry.key_ptr.*);
    }

    std.sort.block([]const u8, keys.items, {}, compareStrings);

    var buffer = [_]u8{0} ** (1024 * 1024);
    var stdout = std.fs.File.stdout().writer(&buffer);
    defer stdout.interface.flush() catch {};

    try stdout.interface.print("{{", .{});
    var is_first = true;
    for (keys.items) |key| {
        if (!is_first) {
            try stdout.interface.print(", ", .{});
        } else {
            is_first = false;
        }

        const value = stations.getPtr(key).?;
        const min_f: f32 = @floatFromInt(value.*.min);
        const average = value.*.average_temp();
        const max_f: f32 = @floatFromInt(value.*.max);
        try stdout.interface.print("{s}={d:.1}/{d:.1}/{d:.1}", .{ value.*.name, min_f / 10.0, average, max_f / 10.0 });
    }

    try stdout.interface.print("}}\n", .{});
}
