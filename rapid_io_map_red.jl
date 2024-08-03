using Mmap, Base.Threads, BenchmarkTools, ThreadsX

# Define the process_chunk function to process each chunk
function process_chunk(mapped_data::Vector{UInt8}, start_idx::Int, end_idx::Int)
    data_dict = Dict{SubArray{UInt8, 1, Vector{UInt8}, Tuple{UnitRange{Int64}}, true}, Vector{Int32}}()

    i = start_idx
    leftover_start = start_idx
    incomplete_lines = Vector{Tuple{Int, Int}}()
    # TODO: [Done - needs testing] Gracefully handle leftovers
    # TODO: Refactor the temperature variables: temp_data, temp_int etc., so as not to be confused with temporary values 
    while i <= end_idx
        if mapped_data[i] == UInt8('\n') || i == end_idx
            line_data = view(mapped_data, leftover_start:i-1) # We want to avoid allocating additional memory for line data, especially since an allocation would require garbage collection
            leftover_start = i + 1

            if !isempty(line_data)
                semicolon_index = find_semicolon_from_end(line_data)
                if semicolon_index > 0
                    station_data = view(line_data, 1:(semicolon_index-1))
                    temp_data = view(line_data, (semicolon_index+1):length(line_data))

                    # the try-catch results in a bit of a perfomance hit (153.48 to 134.81 seconds)
                    #try
                        temp_int = extract_temperature(temp_data)
                        # Check if station already exists in dictionary
                        if haskey(data_dict, station_data)
                            current_values = data_dict[station_data]   # we use a Vector{Int32} here as updates are faster than dict for a small number of elements
                            current_values[1] = ifelse(temp_int < current_values[1], temp_int, current_values[1])   # min
                            current_values[2] = ifelse(temp_int > current_values[2], temp_int, current_values[2])   # max
                            current_values[3] += temp_int   # sum
                            current_values[4] += 1    # count

                        else
                            data_dict[station_data] = [temp_int, temp_int, temp_int, 1]
                        end
                    #catch e
                        # Handle the error if necessary
                    #    println(e)
                    #end
                end
            end
        end
        i += 1
    end

    # Handle incomplete line at the end of the chunk
    if leftover_start <= end_idx
        push!(incomplete_lines, (leftover_start, end_idx))
    end

    return data_dict, incomplete_lines
end

function find_semicolon_from_end(line_data::AbstractVector{UInt8})
    for i in length(line_data):-1:1
        if line_data[i] == UInt8(';')
            return i
        end
    end
    return -1
end

#TODO: See if there is any performance benifit to avoiding the UInt8(<String>) calls. These occur a lot, so small improvments could snowball
function extract_temperature(temp_data::SubArray{UInt8})
    is_negative = false
    temp_int = 0
    for byte in temp_data
        if byte == UInt8('-')
            is_negative = true
        elseif byte == UInt8('.')
            continue  # Skip the decimal point since we always have exactly 1 decimal place - we divide by 10 when computing final results
        elseif byte >= UInt8('0') && byte <= UInt8('9')
            digit = byte - UInt8('0')
            temp_int = temp_int * 10 + digit # Multiply current value by 10 to free up ones place, then add new digit into the ones place 
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

