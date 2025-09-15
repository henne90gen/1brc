from dataclasses import dataclass


@dataclass
class StationSummary:
    name: str
    minimum: float
    maximum: float
    temp_sum: int
    count: int


result = {}
with open("/home/henne/Workspace/1brc/measurements-10M.txt") as f:
    lines = f.readlines()
    for line in lines:
        parts = line.split(";")
        station_name = parts[0]
        temp = float(parts[1])
        if station_name not in result:
            result[station_name] = StationSummary(station_name, 1000, -1000, 0, 0)
        if temp < result[station_name].minimum:
            result[station_name].minimum = temp
        if temp > result[station_name].maximum:
            result[station_name].maximum = temp
        result[station_name].temp_sum += temp
        result[station_name].count += 1


final_str = "{"
is_first = True
for item in sorted(result.items(), key=lambda item: item[0]):
    if not is_first:
        final_str += ", "
    else:
        is_first = False

    summary: StationSummary = item[1]
    average = summary.temp_sum / summary.count
    final_str += f"{summary.name}={summary.minimum:.1f}/{average:.1f}/{summary.maximum:.1f}"
final_str += "}"

print(final_str)
