# Problem statement: 1 billion rows of temperature observations
#     need to ne reduced to <station:min/mean/max>
#     * Data are in ./weather_stations.csv
#     * There are ~10,000 stations, with observations [-99., 99.]
#
# Approach: 
#     1) seqentially read in the data (this will be slow)
#     2) reduce these data as fast as possible, limiting reads/writes
#       2a) store no more data than is necessary
#     3) finally loop through and compute final values / sort
#
# Naive implementation:
#     1) Read through the input file sequentially
#     2) Generate a hash table (or similar data structure) with K/V pairs, creating new
#           stations as we find them
#       2a) key:value is station:[min:max:sum:count] -> str:list[float:float:int:int]
#     3) For each K/V pair calculate average from sum and count and return formatted
#     4) Sort by key
#
#
# Some optimizations to be aware of but not implement at this stage: 
#     1) Sequential reads is not going to be the best play here, ideally this has multiple 
#           streams of data in
#     2) Memory allocation and performance can be improved if we use mmap array 
#     3) There are likely better options than a hash map for a data sctucture,
#            some sort of balanced tree (i.e rb tree) may have faster inserts at the cost
#            of traversing for readout
#     4) There are a few ways we could implement multiprocessing/threading. We need to be careful
#            of the overhead of spawning and managing too many threads
#       4a) Implement some sort of map reduce
#
#

using BenchmarkTools
# Read in the data from file and store in hashmap (dict)

function process_file(file_path::String)
 data_dict = Dict{String,Dict{String,Float32}}()

 # Open the file and read line by line
 open(file_path, "r") do file

  for line in eachline(file)
   # Split the line into station and value using ";" as delimiter
   stn, temp_str = split(line, ";")
   temp = parse(Float32, temp_str)

   # Check if station already exists in dictionary
   if haskey(data_dict, stn)
    # Update existing entry
    current_values = data_dict[stn]

    # ifelse is faster than min/max
    current_values["min"] = min(temp, current_values["min"]) # ifelse(temp < current_values["min"], temp, current_values["min"])

    current_values["max"] = max(temp, current_values["max"]) # ifelse(temp > current_values["max"], temp, current_values["max"])
    current_values["sum"] += temp
    current_values["count"] += 1
   else
    # Initialize new entry
    data_dict[stn] = Dict("min" => temp, "max" => temp, "sum" => temp, "count" => 1)
   end
  end
 end
 print("Done")
 return data_dict
end

# Sort the dictionary by keys and print
function print_stats(summary_stats::Dict{String,Dict{String,Float32}})
 sorted_keys = sort(collect(keys(summary_stats)))
 for stn in sorted_keys
  min_temp = summary_stats[stn]["min"]
  max_temp = summary_stats[stn]["max"]
  avg_temp = summary_stats[stn]["sum"] / summary_stats[stn]["count"]
  println("$stn;$min_temp;$max_temp;$avg_temp")
 end
end

if abspath(PROGRAM_FILE) == @__FILE__                               # If the main program being executed is this file
 print(pwd())
 print(ARGS)
 filepath = isempty(ARGS) ? "./measurements_short.txt" : ARGS[1]    # If no args passed use weather_stations.txt
 @time process_file(filepath) |> print_stats                     # Process file, pipe results into print_stats function above
end



# Call the function with the path to the measurments file
#@benchmark process_file("./measurements_short.txt");
# Full measurments timings:
# * Single core naive (including sort and max/min calcs): 1350.273 s (18000383031 allocations: 656.58 GiB)
# * Single core naive (not including sort and using ifelse): 880.447 s (3.00 G allocations: 286 Gib)
# EOF
