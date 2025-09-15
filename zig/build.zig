const std = @import("std");

pub fn build(b: *std.Build) void {
    const target = b.standardTargetOptions(.{});
    const optimize = b.standardOptimizeOption(.{});

    const exe_mmap_mod = b.createModule(.{
        .root_source_file = b.path("src/main_mmap.zig"),
        .target = target,
        .optimize = optimize,
    });

    const exe_mmap = b.addExecutable(.{
        .name = "brc_mmap",
        .root_module = exe_mmap_mod,
    });
    b.installArtifact(exe_mmap);
    const run_mmap_cmd = b.addRunArtifact(exe_mmap);
    run_mmap_cmd.step.dependOn(b.getInstallStep());
    if (b.args) |args| {
        run_mmap_cmd.addArgs(args);
    }
    const run_mmap_step = b.step("run_mmap", "Run the mmap variant");
    run_mmap_step.dependOn(&run_mmap_cmd.step);

    const exe_mod = b.createModule(.{
        .root_source_file = b.path("src/main.zig"),
        .target = target,
        .optimize = optimize,
    });

    const exe = b.addExecutable(.{
        .name = "brc",
        .root_module = exe_mod,
    });
    b.installArtifact(exe);
    const run_cmd = b.addRunArtifact(exe);
    run_cmd.step.dependOn(b.getInstallStep());
    if (b.args) |args| {
        run_cmd.addArgs(args);
    }
    const run_step = b.step("run", "Run the normal variant");
    run_step.dependOn(&run_cmd.step);
}
