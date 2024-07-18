using BenchmarkTools
using CSV
using StaticArrays
using ThreadsX
using ProfileView

# Define the process_chunk function to process each chunk
function process_chunk(c)
    data_dict = Dict{String, MVector{4, Float32}}()
    for r in c
        stn = r[1]
        temp = Float32(r[2])

        if haskey(data_dict, stn)
            current_values = data_dict[stn]
            current_values[1] = min(current_values[1], temp) # min
            current_values[2] = max(current_values[2], temp) # max
            current_values[3] += temp                       # sum
            current_values[4] += 1                          # count
        else
            data_dict[stn] = @MVector [temp, temp, temp, 1.0f0]
        end
    end
    return data_dict
end

# Function to merge two dictionaries
function merge_dicts(d1::Dict{String, MVector{4, Float32}}, d2::Dict{String, MVector{4, Float32}})
    for (k, v) in d2
        if haskey(d1, k)
            d1[k][1] = min(d1[k][1], v[1])  # min
            d1[k][2] = max(d1[k][2], v[2])  # max
            d1[k][3] += v[3]                # sum
            d1[k][4] += v[4]                # count
        else
            d1[k] = v
        end
    end
    return d1
end

# Define the process_file function to read the file in chunks and process them in parallel
function process_file(f::String)
    chunks = CSV.Chunks(f, ntasks=8, delim=";", types=Dict(1 => String, 2 => Float32), header=0, buffer_in_memory=false)
    results = ThreadsX.map(process_chunk, chunks)
    return ThreadsX.reduce(merge_dicts, results; init=Dict{String, MVector{4, Float32}}())
end

# Define the print_stats function to print the result
function print_stats(summary_stats::Dict{String, MVector{4, Float32}})
    sorted_keys = sort(collect(keys(summary_stats)))
    for stn in sorted_keys
        min_temp = summary_stats[stn][1]
        max_temp = summary_stats[stn][2]
        avg_temp = summary_stats[stn][3] / summary_stats[stn][4]
        #println("$stn;$min_temp;$max_temp;$avg_temp")
    end
end

# Main script execution
if abspath(PROGRAM_FILE) == @__FILE__  # If the main program being executed is this file
    println(pwd())
    println(ARGS)
    filepath = isempty(ARGS) ? "./measurements_short.txt" : ARGS[1]  # If no args passed use measurements_short.txt
    @time process_file(filepath) |> print_stats       # Process file, pipe results into print_stats function above
end

