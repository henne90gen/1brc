#include <string>
#include <iostream>
#include <fstream>
#include <map>
#include <unordered_map>
#include <vector>
#include <array>

#define USE_HASH_MAP 0

struct StationSummary
{
    std::string name;
    int min = 1000;
    int max = -1000;
    int64_t sum = 0;
    int count = 0;

    StationSummary(std::string name_) : name(std::move(name_)) {}
};

#if USE_HASH_MAP
std::unordered_map<std::string, StationSummary> stations_map = {};
#else
std::array<std::vector<StationSummary>, 26> stations = {};
std::vector<StationSummary> other_stations = {};
#endif

inline StationSummary &find_or_insert_vec(std::vector<StationSummary> &vec, const std::string &station)
{
    for (auto &s : vec)
    {
        if (s.name == station)
        {
            return s;
        }
    }

    return vec.emplace_back(station);
}

inline StationSummary &find_or_insert(const std::string &station)
{
#if USE_HASH_MAP
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
#else
    const auto index = static_cast<int>(station[0] - '0');
    if (station[0] >= 'A' and station[0] <= 'Z')
    {
        return find_or_insert_vec(stations[index], station);
    }
    else
    {
        return find_or_insert_vec(other_stations, station);
    }
#endif
}

void insert_into_ordered(std::map<std::string, StationSummary> &ordered)
{
#if USE_HASH_MAP
    for (const auto &entry : stations_map)
    {
        ordered.emplace(entry);
    }
#else
    for (const auto &vec : stations)
    {
        for (const auto &s : vec)
        {
            ordered.emplace(std::make_pair(s.name, s));
        }
    }
    for (const auto &s : other_stations)
    {
        ordered.emplace(std::make_pair(s.name, s));
    }
#endif
}

void print_result()
{
    std::map<std::string, StationSummary> ordered = {};
    insert_into_ordered(ordered);
    for (const auto &entry : ordered)
    {
        std::cout << entry.first << ": " << //
            entry.second.min / 10.0 << "/" << entry.second.sum / entry.second.count / 10.0 << "/" << entry.second.max / 10.0 << std::endl;
    }
}

int main()
{
#if 1
    std::string path = "../1brc/measurements.txt";
#else
    std::string path = "../1brc/measurements-1brc.txt";
#endif

#if USE_HASH_MAP
#else
    for (auto &vec : stations)
    {
        vec.reserve(50);
    }
    other_stations.reserve(50);
#endif

    std::ifstream f(path);
    if (!f.is_open())
    {
        std::cerr << "failed to open file " << path << std::endl;
        return 1;
    }

    std::string line;
    while (std::getline(f, line))
    {
        int temp = 0;
        bool is_negative = false;
        int station_name_length = 0;
        for (int i = 0; i < line.size(); i++)
        {
            if (line[i] == ';')
            {
                station_name_length = i;
                continue;
            }

            if (station_name_length == 0)
            {
                continue;
            }

            if (line[i] == '.')
            {
                continue;
            }
            if (line[i] == '-')
            {
                is_negative = true;
                continue;
            }

            temp *= 10;
            temp += static_cast<int>(line[i] - '0');
        }
        if (is_negative)
        {
            temp *= -1;
        }

        std::string station = line.substr(0, station_name_length);

        auto &station_summary = find_or_insert(station);

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
    }

    print_result();
}
