using Mmap, Base.Threads, BenchmarkTools, ThreadsX

# Define the process_chunk function to process each chunk
function process_chunk(mapped_data::Vector{UInt8}, start_idx::Int, end_idx::Int)
    data_dict = Dict{String, Dict{String, Int32}}()

    i = start_idx
    leftover_start = start_idx

    while i <= end_idx
        if mapped_data[i] == UInt8('\n') || i == end_idx
            line_data = mapped_data[leftover_start:i-1]
            leftover_start = i + 1

            if !isempty(line_data)
                semicolon_index = find_semicolon_from_end(line_data)
                if semicolon_index > 0
                    station_data = String(view(line_data, 1:(semicolon_index-1)))
                    temp_data = view(line_data, (semicolon_index+1):length(line_data))

                    try
                        temp_int = extract_temperature(temp_data)
                        # Check if station already exists in dictionary
                        if haskey(data_dict, station_data)
                            current_values = data_dict[station_data]
                            # ifelse is faster than min/max
                            current_values["min"] =  ifelse(temp_int < current_values["min"], temp_int, current_values["min"]) #min(current_values["min"], temp_float)
                            current_values["max"] =  ifelse(temp_int > current_values["max"], temp_int, current_values["max"]) #max(current_values["max"], temp_float)
                            current_values["sum"] += temp_int
                            current_values["count"] += 1
                        else
                            data_dict[station_data] = Dict("min" => temp_int, "max" => temp_int, "sum" => temp_int, "count" => 1)
                        end
                    catch e
                        # Handle the error if necessary
                    end
                end
            end
        end
        i += 1
    end

    return data_dict
end

function find_semicolon_from_end(line_data::Vector{UInt8})
    for i in length(line_data):-1:1
        if line_data[i] == UInt8(';')
            return i
        end
    end
    return -1
end

function extract_temperature(temp_data::SubArray{UInt8})
    is_negative = false
    temp_int = 0

    for byte in temp_data
        if byte == UInt8('-')
            is_negative = true
        elseif byte == UInt8('.')
            continue  # Skip the decimal point
        elseif byte >= UInt8('0') && byte <= UInt8('9')
            digit = byte - UInt8('0')
            temp_int = temp_int * 10 + digit
        end
    end

    if is_negative
        temp_int = -temp_int
    end

    return temp_int
end

# Define the process_file function to read the file in chunks and process them in parallel
function process_file(file_path::String)
    file = open(file_path, "r")
    mapped_data = Mmap.mmap(file)
    close(file)

    total_length = length(mapped_data)
    chunk_length = div(total_length, Threads.nthreads())

    chunks = [(i, min(i + chunk_length - 1, total_length)) for i in 1:chunk_length:total_length]

    @time results = ThreadsX.map(chunk -> process_chunk(mapped_data, chunk[1], chunk[2]), chunks)
    combined_result = Dict{String, Dict{String, Int32}}()
    for result in results
        merge_dicts!(combined_result, result)
    end
    return combined_result
end

# Function to merge two dictionaries in place
function merge_dicts!(d1::Dict{String, Dict{String, Int32}}, d2::Dict{String, Dict{String, Int32}})
    for (key, value) in d2
        if haskey(d1, key)
            # doesn't matter a lot here, but ifelse is faster than min/max
            d1[key]["min"] = ifelse(value["min"] < d1[key]["min"], value["min"], d1[key]["min"]) #min(d1[key]["min"], value["min"])
            d1[key]["max"] = ifelse(value["max"] > d1[key]["max"], value["min"], d1[key]["max"]) #max(d1[key]["max"], value["max"])
            d1[key]["sum"] += value["sum"]
            d1[key]["count"] += value["count"]
        else
            d1[key] = value
        end
    end
end

# Define the print_stats function to print the result
function print_stats(summary_stats::Dict{String, Dict{String, Int32}})
    sorted_keys = sort(collect(keys(summary_stats)))
    for stn in sorted_keys
        min_temp = summary_stats[stn]["min"]
        max_temp = summary_stats[stn]["max"]
        avg_temp = (summary_stats[stn]["sum"] /10) / summary_stats[stn]["count"]     # div by 10 to get from our shorthand int that ignored the decimal back to normal float
        #println("$stn;$min_temp;$max_temp;$avg_temp")
    end
end

# Main script execution
if abspath(PROGRAM_FILE) == @__FILE__
    println(pwd())
    println(ARGS)
    file_path = isempty(ARGS) ? "./measurements_10M.txt" : ARGS[1]
    @time process_file(file_path) |> print_stats
end
