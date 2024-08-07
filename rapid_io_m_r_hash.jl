using Mmap, Base.Threads, BenchmarkTools, ThreadsX
using Base.Iterators: enumerate

# Function to hash SubArray{UInt8}
function hash_subarray(subarray::SubArray{UInt8})
    hash_value = 0
    for byte in subarray
        hash_value = hash_value * 256 + byte
    end
    return hash_value
end

# Define the process_chunk function to process each chunk
function process_chunk(mapped_data::Vector{UInt8}, start_idx::Int, end_idx::Int)
    data_dict = Dict{Int64, Vector{Int32}}()

    i = start_idx
    leftover_start = start_idx
    incomplete_lines = Vector{Tuple{Int, Int}}()

    @inbounds while i <= end_idx   
        if mapped_data[i] == 0x0a || i == end_idx    # 0x0a is the newline char
            line_data = view(mapped_data, leftover_start:i-1) # Avoid allocating additional memory for line data
            leftover_start = i + 1

            semicolon_index = find_semicolon_from_end(line_data)
            if semicolon_index > 0    # 4 is the minimum value of the temp data
                station_data = view(line_data, 1:(semicolon_index-1))    # Station name
                temp_data = view(line_data, (semicolon_index+1):length(line_data))    # Temperature data
                temp_int = extract_temperature(temp_data)

                station_hash = hash_subarray(station_data)

                if haskey(data_dict, station_hash)
                    current_values = data_dict[station_hash]
                    current_values[1] = ifelse(temp_int < current_values[1], temp_int, current_values[1])   # Min
                    current_values[2] = ifelse(temp_int > current_values[2], temp_int, current_values[2])   # Max
                    current_values[3] += temp_int   # Sum
                    current_values[4] += 1    # Count
                else
                    data_dict[station_hash] = [temp_int, temp_int, temp_int, 1]
                end
            end
            i += 5    # Minimum line length is 5 bytes (e.g., ';0.0')
        end
        i += 1 # Iterate the counter one more time (independent of our += 5 above)
    end

    if leftover_start <= end_idx
        push!(incomplete_lines, (leftover_start, end_idx))
    end

    return data_dict, incomplete_lines
end

function find_semicolon_from_end(line_data::SubArray{UInt8})
    for i in length(line_data)-4:-1:1    # Start from the end of the line
        if line_data[i] == 0x3b  # ASCII ';'
            return i
        end
    end
    return -1
end

function extract_temperature(temp_data::SubArray{UInt8})
    is_negative = false
    temp_int = 0
    
    for byte in temp_data
        if byte == 0x2D    # ASCII '-'
            is_negative = true
        elseif byte != 0x2E    # ASCII '.'
            digit = byte - 0x30    # ASCII '0'
            temp_int = temp_int * 10 + digit
        end
    end

    return is_negative ? -temp_int : temp_int
end

function process_file(file_path::String)
    file = open(file_path, "r")
    mapped_data = Mmap.mmap(file)
    close(file)

    total_length = length(mapped_data)
    chunk_length = div(total_length, Threads.nthreads())

    @views chunks = [(i, min(i + chunk_length - 1, total_length)) for i in 1:chunk_length:total_length]

    results = ThreadsX.map(chunk -> process_chunk(mapped_data, chunk[1], chunk[2]), chunks)
    combined_result = Dict{Int64, Vector{Int32}}()
    incomplete_lines = Vector{Tuple{Int, Int}}()

    for result in results
        merge_dicts!(combined_result, result[1])
        append!(incomplete_lines, result[2])
    end

    for (start_idx, end_idx) in incomplete_lines
        partial_result, _ = process_chunk(mapped_data, start_idx, end_idx)
        merge_dicts!(combined_result, partial_result)
    end

    return combined_result
end

function merge_dicts!(d1::Dict{Int64, Vector{Int32}}, d2::Dict{Int64, Vector{Int32}})
    for (key, value) in d2
        if haskey(d1, key)
            d1[key][1] = ifelse(value[1] < d1[key][1], value[1], d1[key][1])    # Min
            d1[key][2] = ifelse(value[2] > d1[key][2], value[2], d1[key][2])    # Max
            d1[key][3] += value[3]                                              # Sum
            d1[key][4] += value[4]                                              # Count
        else
            d1[key] = value
        end
    end
end

function print_stats(summary_stats::Dict{Int64, Vector{Int32}})
    sorted_keys = sort(collect(keys(summary_stats)))
    for stn_hash in sorted_keys
        min_temp = summary_stats[stn_hash][1] / 10
        max_temp = summary_stats[stn_hash][2] / 10 
        avg_temp = (summary_stats[stn_hash][3] / 10) / summary_stats[stn_hash][4]
        println("Station Hash $(stn_hash); Min: $min_temp; Max: $max_temp; Avg: $avg_temp")
    end
end

if abspath(PROGRAM_FILE) == @__FILE__
    println(pwd())
    println(ARGS)
    file_path = isempty(ARGS) ? "./measurements_10M.txt" : ARGS[1]
    @time process_file(file_path) |> print_stats
end

