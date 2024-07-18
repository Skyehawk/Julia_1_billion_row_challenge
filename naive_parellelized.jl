using BenchmarkTools
using CSV
using ThreadsX
#using Tables


# Define the process_chunk function to process each chunk
function process_chunk(c)
    data_dict = Dict{String, Dict{String, Float32}}()
    for r in c
        stn = r[1]
        temp = r[2]

        # Check if station already exists in dictionary
        if haskey(data_dict, stn)
            # Update existing entry
            current_values = data_dict[stn]

            current_values["min"] = ifelse(temp < current_values["min"], temp, current_values["min"])
            current_values["max"] = ifelse(temp > current_values["max"], temp, current_values["max"])            
            current_values["sum"] +=temp
            current_values["count"] += 1
        else
            # Initialize new entry
            data_dict[stn] = Dict("min" => temp, "max" => temp, "sum" => temp, "count" => 1)
        end
    end
    return data_dict
end

# Function to merge two dictionaries
function merge_dicts(d1::Dict{String, Dict{String, Float32}}, d2::Dict{String, Dict{String, Float32}})
    for (key, value) in d2
        if haskey(d1, key)
            d1[key]["min"] = min(d1[key]["min"], value["min"])
            d1[key]["max"] = max(d1[key]["max"], value["max"])
            d1[key]["sum"] += value["sum"]
            d1[key]["count"] += value["count"]
        else
            d1[key] = value
        end
    end
    return d1
end

# Define the process_file function to read the file in chunks and process them in parallel
function process_file(f::String)
    chunks = CSV.Chunks(f, ntasks=8, delim=";", types=Dict(1 => String, 2 => Float32), header=0, buffer_in_memory=false)
    @time results = ThreadsX.map(process_chunk, chunks)
    return ThreadsX.reduce(merge_dicts, results; init=Dict{String, Dict{String, Float32}}())
end

# Define the print_stats function to print the result
function print_stats(summary_stats::Dict{String, Dict{String, Float32}})
    sorted_keys = sort(collect(keys(summary_stats)))
    for stn in sorted_keys
        min_temp = summary_stats[stn]["min"]
        max_temp = summary_stats[stn]["max"]
        avg_temp = summary_stats[stn]["sum"] / summary_stats[stn]["count"]
        #println("$stn;$min_temp;$max_temp;$avg_temp")
    end
end

# Main script execution
if abspath(PROGRAM_FILE) == @__FILE__                               # If the main program being executed is this file
    println(pwd())
    println(ARGS)
    filepath = isempty(ARGS) ? "./measurements_short.txt" : ARGS[1]    # If no args passed use weather_stations.txt
    @time process_file(filepath) |> print_stats                     # Process file, pipe results into print_stats function above
end


