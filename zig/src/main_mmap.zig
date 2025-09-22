const std = @import("std");
const builtin = @import("builtin");

const multi_threaded = true;
const use_debug_allocator = true;
// const measurements_file_path = "/home/henne/Workspace/1brc/measurements-10M.txt";
const measurements_file_path = "/home/henne/Workspace/1brc/measurements-1B.txt";
// const measurements_file_path = "D:\\Workspace\\1brc\\measurements-1B.txt";

const StationSummary = struct {
    name: []const u8,
    min: i32 = 1000,
    max: i32 = -1000,
    sum: i64 = 0,
    count: i32 = 0,

    pub fn init(name_: []const u8) StationSummary {
        return StationSummary{ .name = name_ };
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

fn mmap_file(file: std.fs.File, total_file_size: usize) ![*]u8 {
    if (builtin.os.tag == .linux) {
        const mmap_result = std.os.linux.mmap(
            null,
            @intCast(total_file_size),
            1,
            .{ .TYPE = std.os.linux.MAP_TYPE.SHARED },
            file.handle,
            0,
        );
        if (mmap_result == -1) {
            std.debug.print("Failed to mmap file\n", .{});
            return error.MmapFailed;
        }

        return @ptrFromInt(mmap_result);
    } else if (builtin.os.tag == .windows) {
        const windows = @import("windows.zig");
        const mapping = windows.CreateFileMapping( //
            file.handle, // hFile
            null, // Mapping attributes
            windows.PAGE_READONLY, // Protection flags
            0, // MaximumSizeHigh
            0, // MaximumSizeLow
            null // Name
        );
        if (mapping == null) {
            std.debug.print("CreateFileMapping failed with error {d}\n", .{std.os.windows.GetLastError()});
            return error.MmapFailed;
        }

        const ptr = windows.MapViewOfFile(
            mapping, // hFileMappingObject
            windows.FILE_MAP_READ, // dwDesiredAccess
            0, // dwFileOffsetHigh
            0, // dwFileOffsetLow
            0, // dwNumberOfBytesToMap
        );
        if (ptr == null) {
            std.debug.print("MapViewOfFile failed with error {d}\n", .{std.os.windows.GetLastError()});
            std.os.windows.CloseHandle(mapping.?);
            return error.MmapFailed;
        }

        return @ptrCast(ptr);
    } else {
        std.debug.print("Memory mapping is only implemented for Linux\n", .{});
        return error.UnsupportedOS;
    }

    return error.MmapFailed;
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

    try file.seekFromEnd(0);
    const total_file_size = try file.getPos();
    try file.seekTo(0);

    const buffer: [*]u8 = try mmap_file(file, total_file_size);

    const parallel_executions = try std.Thread.getCpuCount();
    const chunk_size = @divFloor(total_file_size, parallel_executions);
    var threads: []std.Thread = try gpa.alloc(std.Thread, parallel_executions);
    defer gpa.free(threads);
    var stations: []std.StringHashMap(StationSummary) = try gpa.alloc(std.StringHashMap(StationSummary), parallel_executions);
    defer gpa.free(stations);
    for (0..parallel_executions) |idx| {
        const station_map = std.StringHashMap(StationSummary).init(gpa);
        stations[idx] = station_map;

        const start_pos = chunk_size * idx;
        var end_pos = chunk_size * (idx + 1);
        if (idx == parallel_executions - 1) {
            end_pos = total_file_size;
        }

        if (comptime multi_threaded) {
            const thread = try std.Thread.spawn(.{}, process_chunk, .{
                buffer,
                total_file_size,
                start_pos,
                end_pos,
                &stations[idx],
            });
            threads[idx] = thread;
        } else {
            try process_chunk(buffer, total_file_size, start_pos, end_pos, &stations[idx]);
        }
    }

    if (comptime multi_threaded) {
        for (threads) |thread| {
            thread.join();
        }
    }

    var result_stations = std.StringHashMap(StationSummary).init(gpa);
    defer result_stations.deinit();
    for (0..parallel_executions) |idx| {
        defer stations[idx].deinit();

        var itr = stations[idx].valueIterator();
        while (itr.next()) |value| {
            const result = try result_stations.getOrPut(value.*.name);
            if (!result.found_existing) {
                result.value_ptr.* = StationSummary.init(value.*.name);
            }
            result.value_ptr.update_station(value);
        }
    }

    try print_results(gpa, &result_stations);
}

fn process_chunk(buffer: [*]u8, buffer_size: usize, start_pos: usize, end_pos: usize, stations: *std.StringHashMap(StationSummary)) !void {
    var current_pos: usize = start_pos;

    if (start_pos != 0) {
        // find first line break
        while (buffer[current_pos] != '\n') {
            current_pos += 1;
        }
        // skip line break
        current_pos += 1;
    }

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
            const result = try stations.getOrPut(station_name);
            if (!result.found_existing) {
                result.value_ptr.* = StationSummary.init(station_name);
            }
            result.value_ptr.update_temp(temp);

            temp = 0;
            is_negative = false;
            station_name_length = 0;
            start_of_line = current_pos + 1;

            if (current_pos >= end_pos) {
                break;
            }
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

fn compareStrings(_: void, lhs: []const u8, rhs: []const u8) bool {
    return std.mem.order(u8, lhs, rhs).compare(std.math.CompareOperator.lt);
}

fn print_results(gpa: std.mem.Allocator, stations: *std.StringHashMap(StationSummary)) !void {
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
