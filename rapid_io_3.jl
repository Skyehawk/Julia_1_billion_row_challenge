using BenchmarkTools
using Mmap, Base.Threads

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
                        # println("Error processing line: ", String(line_data))
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

function stream_from_disk(file_path::String, io_chunk_size::Int, start_chunk::Int, end_chunk::Int, CHUNK_QUEUE::Channel{Vector{Tuple{SubArray{UInt8}, Int}}})
    println("Starting producer from chunk $start_chunk to $end_chunk")
    
    # Open the file and memory map it
    file = open(file_path, "r")
    mapped_data = Mmap.mmap(file)
    close(file)

    initial_size = div(length(mapped_data), io_chunk_size) + 1  # Start with a large initial size
    data = Vector{Tuple{SubArray{UInt8}, Int}}(undef, initial_size)
    data_idx = 1

    leftover_start = start_chunk
    chunk_start = start_chunk

    while chunk_start <= end_chunk
        chunk_end = min(chunk_start + io_chunk_size - 1, length(mapped_data))
        leftover_start, data_idx = process_stream_chunk!(mapped_data, leftover_start, chunk_end, data, data_idx)
        
        # If data array is full, put it into the queue and allocate a new one
        if data_idx > initial_size
            #println("Placing chunk in CHUNK_QUEUE")
            put!(CHUNK_QUEUE, data)
            data = Vector{Tuple{SubArray{UInt8}, Int}}(undef, initial_size)
            data_idx = 1
        end

        chunk_start = chunk_end + 1
    end

    # Put the last chunk into the queue
    if data_idx > 1
        #println("Placing chunk in CHUNK_QUEUE")
        put!(CHUNK_QUEUE, data[1:data_idx-1])
    end

    #println("Producer finished for chunk $start_chunk to $end_chunk")
end

function start_producers(file_path::String, io_chunk_size::Int, num_producers::Int, CHUNK_QUEUE::Channel{Vector{Tuple{SubArray{UInt8}, Int}}})
    # Calculate chunks to be read by each producer
    file = open(file_path, "r")
    mapped_data = Mmap.mmap(file)
    close(file)

    total_length = length(mapped_data)
    chunk_length = div(total_length, num_producers)
    
    tid = Vector{Task}(undef, num_producers)
    for i in 1:num_producers
        start_chunk = (i - 1) * chunk_length + 1
        end_chunk = i == num_producers ? total_length : i * chunk_length
        tid[i] = @spawn stream_from_disk(file_path, io_chunk_size, start_chunk, end_chunk, CHUNK_QUEUE)
    end

    # Wait for all producers to finish
    for t in tid
        wait(t)
    end

    # Close the channel after all producers are done
    close(CHUNK_QUEUE)
end

function start_consumers(num_consumers::Int, CHUNK_QUEUE::Channel{Vector{Tuple{SubArray{UInt8}, Int}}})
    for i in 1:num_consumers
        @spawn begin
            for data_chunk in CHUNK_QUEUE
                #println("Consumer $i processing chunk of size ", length(data_chunk))
                # Perform processing on data_chunk
                d = Dict{SubArray{UInt8},Int}()
                for (keys, val) in data_chunk, key in keys
                    #d[key] = val
                    #println(key, val)
                end
            end
            #println("Consumer $i finished processing")
        end
    end
end

function main()
    print(pwd())
    print("\n", ARGS)
    file_path = isempty(ARGS) ? "./measurements_100M.txt" : ARGS[1] # If no args passed use ./measurements_100M.txt
    io_chunk_size = 1024*8                                         # 8MB
    num_producers = 4
    num_consumers = 4

    # Queue for the producers to place in and consumers to pull from
    CHUNK_QUEUE = Channel{Vector{Tuple{SubArray{UInt8}, Int}}}(20)
    # Start consumer threads
    start_consumers(num_consumers, CHUNK_QUEUE)
    
    # Start producer threads
    start_producers(file_path, io_chunk_size, num_producers, CHUNK_QUEUE)

    # Main thread waits for completion
    while isopen(CHUNK_QUEUE) 
        sleep(0.05)
    end

    println("All processing complete")
end

if abspath(PROGRAM_FILE) == @__FILE__                               # If the main program being executed is this file, same as if name=main in python
 @btime main()    
end

