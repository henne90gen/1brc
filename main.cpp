#include <string>
#include <iostream>
#include <fstream>
#include <map>
#include <unordered_map>
#include <vector>
#include <array>
#include <thread>
#include <cmath>

#include <stdio.h>
#include <stdlib.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include <fcntl.h>
#include <sys/mman.h>

#define THREADED 1

// single threaded:    2.784s
// multi threaded:     0.845s
// best java solution: 0.163s

struct StationSummary
{
    std::string name;
    int min = 1000;
    int max = -1000;
    int64_t sum = 0;
    int count = 0;

    StationSummary(std::string name_) : name(std::move(name_)) {}
};

double custom_round(double value, int decimal_places)
{
    const double multiplier = std::pow(10.0, decimal_places);
    return std::round(value * multiplier) / multiplier;
}

inline StationSummary &find_or_insert(std::unordered_map<std::string, StationSummary> &stations_map, const std::string &station)
{
    auto itr = stations_map.find(station);
    if (itr == stations_map.end())
    {
        const auto insert_result = stations_map.emplace(std::make_pair(station, StationSummary(station)));
        if (!insert_result.second)
        {
            std::cerr << "failed to insert station into stations_map" << std::endl;
            exit(1);
        }
        itr = insert_result.first;
    }
    return itr->second;
}

void print_result(std::array<std::unordered_map<std::string, StationSummary>, 8> &result)
{
    std::map<std::string, StationSummary> ordered = {};
    for (const auto &thread_result : result)
    {
        for (const auto &entry : thread_result)
        {
            auto itr = ordered.find(entry.first);
            if (itr == ordered.end())
            {
                ordered.emplace(entry);
                continue;
            }

            itr->second.count += entry.second.count;
            itr->second.sum += entry.second.sum;
            if (entry.second.max > itr->second.max)
            {
                itr->second.max = entry.second.max;
            }
            if (entry.second.min < itr->second.min)
            {
                itr->second.min = entry.second.min;
            }
        }
    }

    for (const auto &entry : ordered)
    {
        const auto minimum = (double)entry.second.min / 10.0;
        const auto average = custom_round((double)entry.second.sum / (double)entry.second.count / 10.0, 1);
        const auto maximum = (double)entry.second.max / 10.0;
        std::cout << entry.first << ": " << //
            minimum << "/" << average << "/" << maximum << " - " << entry.second.sum / 10.0 << " - " << entry.second.count << std::endl;
    }
}

struct Chunk
{
    size_t start;
    size_t end;
};

void process_chunk(char *file_buffer, bool is_first_chunk, const Chunk &chunk, std::unordered_map<std::string, StationSummary> &result)
{
    char *current_pos = file_buffer + chunk.start;
    char *end_pos = file_buffer + chunk.end;

    if (!is_first_chunk)
    {
        // find first line break
        while (*current_pos != '\n')
        {
            current_pos++;
        }
        // skip line break
        current_pos++;
    }

    while (current_pos < end_pos)
    {
        char *start_of_line = current_pos;

        int temp = 0;
        bool is_negative = false;
        int station_name_length = 0;
        for (; *current_pos != '\n'; current_pos++)
        {
            if (*current_pos == ';')
            {
                station_name_length = current_pos - start_of_line;
                continue;
            }

            if (station_name_length == 0)
            {
                continue;
            }

            if (*current_pos == '.')
            {
                continue;
            }
            if (*current_pos == '-')
            {
                is_negative = true;
                continue;
            }

            temp *= 10;
            temp += static_cast<int>(*current_pos - '0');
        }
        if (is_negative)
        {
            temp *= -1;
        }

        std::string station = std::string(start_of_line, station_name_length);

        auto &station_summary = find_or_insert(result, station);

        station_summary.count++;
        station_summary.sum += static_cast<int64_t>(temp);
        if (temp > station_summary.max)
        {
            station_summary.max = temp;
        }
        if (temp < station_summary.min)
        {
            station_summary.min = temp;
        }

        // skip line break
        current_pos++;
    }
}

int main()
{
#if 1
    std::string path = "../1brc/measurements.txt";
#else
    std::string path = "../1brc/measurements-1brc.txt";
#endif

    std::ifstream f(path, std::ios::ate);
    if (!f.is_open())
    {
        std::cerr << "failed to open file " << path << std::endl;
        return 1;
    }

    const auto file_size_bytes = f.tellg();
    const auto chunk_size_bytes = file_size_bytes / 8;
    std::array<Chunk, 8> chunks = {};
    for (int i = 0; i < chunks.size(); i++)
    {
        chunks[i].start = i * chunk_size_bytes;
        chunks[i].end = (i + 1) * chunk_size_bytes;
    }
    chunks[7].end = file_size_bytes;

    auto fd = open(path.c_str(), O_RDONLY);
    if (fd == -1)
    {
        std::cerr << "failed to open file " << path << std::endl;
        return 1;
    }

    auto file_buffer = reinterpret_cast<char *>(mmap(0, file_size_bytes, PROT_READ, MAP_SHARED, fd, 0));
    if (file_buffer == MAP_FAILED)
    {
        close(fd);
        std::cerr << "failed to map file " << path << " into memory" << std::endl;
        return 1;
    }

    std::array<std::unordered_map<std::string, StationSummary>, 8> result = {};
    std::array<std::thread, 8> threads = {};
    for (int i = 0; i < threads.size(); i++)
    {
#if THREADED
        threads[i] = std::thread([i, file_buffer, &chunks, &result]()
                                 { process_chunk(file_buffer, i == 0, chunks[i], result[i]); });
#else
        process_chunk(file_buffer, i == 0, chunks[i], result[i]);
#endif
    }

#if THREADED
    for (auto &thread : threads)
    {
        thread.join();
    }
#endif

    print_result(result);
}
