using Mmap, Base.Threads, BenchmarkTools, ThreadsX
using Base.Iterators: enumerate
using DataStructures

# Define the process_chunk function to process each chunk
function process_chunk(mapped_data::Vector{UInt8}, start_idx::Int, end_idx::Int)
    data_dict = Dict{SubArray{UInt8, 1, Vector{UInt8}, Tuple{UnitRange{Int64}}, true}, Vector{Int32}}()

    i = start_idx
    leftover_start = start_idx
    incomplete_lines = Vector{Tuple{Int, Int}}()
    
    # TODO: If we find a new line we can progress our 'i' index along at least 5 bits (e.g. minimum line length would be 'a;0.0', or 5 bits)
    # 
    # if we were running a for loop we could use the @simd (single instruction, multiple data) macro, but we need the while in this case
    @inbounds while  i <= end_idx   
        if mapped_data[i] == 0x0a || i == end_idx    # 0x0a is the newline char, loop until we find the next newline character 
            line_data = view(mapped_data, leftover_start:i-1) # Avoid allocating additional memory for line data
            leftover_start = i + 1

            #if !isempty(line_data)
                semicolon_index = find_semicolon_from_end(line_data)
                if semicolon_index > 0    # 4 is the minimum value of the temp data, however, checking if x > 0 is faster than if x >= 4 
                    station_data = view(line_data, 1:(semicolon_index-1))    # the substring representing the station name
                    temp_data = view(line_data, (semicolon_index+1):length(line_data))    # substring of temperature data 
                    temp_int = extract_temperature(temp_data)
                    #println(temp_int)
                    if haskey(data_dict, station_data)
                        current_values = data_dict[station_data]
                        current_values[1] = ifelse(temp_int < current_values[1], temp_int, current_values[1])   # min
                        current_values[2] = ifelse(temp_int > current_values[2], temp_int, current_values[2])   # max
                        current_values[3] += temp_int   # sum
                        current_values[4] += 1    # count
                    else
                        data_dict[station_data] = [temp_int, temp_int, temp_int, 1]
                    end
                end
            #end
            #i += 5    # we do this because we know the next newline can't be any less than 5 bytes away (e.g.  a;0.0 )
        end
        i += 1 # itterate the loop one more time (independent of our += 6 above)
    end

    # Handle incomplete line at the end of the chunk
    if leftover_start <= end_idx
        push!(incomplete_lines, (leftover_start, end_idx))
    end

    return data_dict, incomplete_lines
end

#WARN: This function is unsafe if the temperature substring is less than 4 bytes
function find_semicolon_from_end(line_data::SubArray{UInt8})
    for i in length(line_data)-4:-1:1    # start from the end of the line, as close to the ';' as we can w/o knowing the length of temperature 
        if line_data[i] == 0x3b  # ASCII ';'
            return i
        end
    end
    return -1
end

# WARN: This function is unsafe if ANY characters other than '-','.' or [0-9] are passed in the SubArray, we rely on the input data to be correct
function extract_temperature(temp_data::SubArray{UInt8})
    is_negative = false
    temp_int = 0
    
    for byte in temp_data
        if byte == 0x2D    # ASCII '-'
            is_negative = true
        elseif byte != 0x2E    # ASCII '.' - Skip the decimal point
            digit = byte - 0x30    # ASCII '0'
            temp_int = temp_int * 10 + digit
        end
    end

    return is_negative ? -temp_int : temp_int
end

# Define the process_file function to read the file in chunks and process them in parallel
function process_file(file_path::String)
    file = open(file_path, "r")
    mapped_data = Mmap.mmap(file)
    close(file)

    total_length = length(mapped_data)
    chunk_length = div(total_length, Threads.nthreads())

    @views chunks = [(i, min(i + chunk_length - 1, total_length)) for i in 1:chunk_length:total_length]

    results = ThreadsX.map(chunk -> process_chunk(mapped_data, chunk[1], chunk[2]), chunks)
    combined_result = Dict{SubArray{UInt8, 1, Vector{UInt8}, Tuple{UnitRange{Int64}}, true}, Vector{Int32}}()
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

function merge_dicts!(d1::Dict{SubArray{UInt8, 1, Vector{UInt8}, Tuple{UnitRange{Int64}}, true}, Vector{Int32}},
                      d2::Dict{SubArray{UInt8, 1, Vector{UInt8}, Tuple{UnitRange{Int64}}, true}, Vector{Int32}})
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

function print_stats(summary_stats::Dict{SubArray{UInt8, 1, Vector{UInt8}, Tuple{UnitRange{Int64}}, true}, Vector{Int32}})
    sorted_keys = sort(collect(keys(summary_stats)))
    for stn in sorted_keys
        min_temp = summary_stats[stn][1] / 10
        max_temp = summary_stats[stn][2] / 10 
        avg_temp = (summary_stats[stn][3] / 10) / summary_stats[stn][4]

        println("$(String(stn));$min_temp;$max_temp;$avg_temp")
    end
end

# Main script execution
if abspath(PROGRAM_FILE) == @__FILE__
    println(pwd())
    println(ARGS)
    file_path = isempty(ARGS) ? "./measurements_10M.txt" : ARGS[1]
    @time process_file(file_path) |> print_stats
end

