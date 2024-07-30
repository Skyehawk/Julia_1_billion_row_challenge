using Mmap, Base.Iterators

function process_stream_chunk!(mapped_data::Vector{UInt8}, start_idx::Int, end_idx::Int, data::Vector{Tuple{SubArray{UInt8}, Int}}, data_idx::Int)
    i = start_idx
    leftover_start = start_idx

    while i <= end_idx
        if mapped_data[i] == UInt8('\n') || i == end_idx
            line_data = mapped_data[leftover_start:i-1]
            leftover_start = i + 1

            if !isempty(line_data)
                semicolon_index = find_semicolon_from_end(line_data)
                if semicolon_index > 0
                    station_data = view(line_data, 1:(semicolon_index-1))
                    temp_data = view(line_data, (semicolon_index+1):length(line_data))

                    try
                        temp_int = extract_temperature(temp_data)
                        data[data_idx] = (station_data, temp_int)
                        data_idx += 1
                    catch e
                        #println("Error processing line: ", String(line_data))
                    end
                end
            end
        end
        i += 1
    end

    return leftover_start, data_idx
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

function stream_from_disk(file_path::String, chunk_size::Int)
    # Open the file and memory map it
    file = open(file_path, "r")
    mapped_data = Mmap.mmap(file)
    close(file)

    initial_size = 10^6  # Start with a "large" initial size for the output array
    data = Vector{Tuple{SubArray{UInt8}, Int}}(undef, initial_size)
    data_idx = 1

    leftover_start = 1
    chunk_start = 1

    while chunk_start <= length(mapped_data)
        chunk_end = min(chunk_start + chunk_size - 1, length(mapped_data))
        leftover_start, data_idx = process_stream_chunk!(mapped_data, leftover_start, chunk_end, data, data_idx)
        chunk_start = chunk_end + 1
    end

    return data[1:data_idx-1]
end

# Example usage
file_path = "./measurements_short.txt"
chunk_size = 8192  # Adjust chunk size as needed
data = @time stream_from_disk(file_path, chunk_size)

# Print the results
#for (station_name, temp_int) in data
    #println("Station (bytes): ", String(station_name), ", Temp: ", temp_int)
#end

