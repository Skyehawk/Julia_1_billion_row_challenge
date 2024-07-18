using BenchmarkTools
using CSV
using StaticArrays
using ThreadsX
#using ProfileView
using Base.Threads

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

# Producer function to read chunks and send them to the channel
function producer(file::String, chunk_channel::Channel)
    chunks = CSV.Chunks(file, ntasks=8, delim=";", types=Dict(1 => String, 2 => Float32), header=0, buffer_in_memory=false)
    for chunk in chunks
        put!(chunk_channel, chunk)
    end
    close(chunk_channel) # Indicate no more chunks will be sent
end

# Consumer function to process chunks from the channel
function consumer(chunk_channel::Channel, result_channel::Channel)
    while true
        chunk = take!(chunk_channel)
        result = process_chunk(chunk)
        put!(result_channel, result)
    end
end

# Main function to orchestrate the producer and consumer threads
function process_file(f::String)
    chunk_channel = Channel(32) # Buffer size of 32 chunks
    result_channel = Channel(32) # Buffer size of 32 results

    # Start the producer thread
    producer_task = @spawn producer(f, chunk_channel)

    # Start the consumer threads
    consumer_tasks = []
    n_consumers = Threads.nthreads() - 1
    for _ in 1:n_consumers
        push!(consumer_tasks, @spawn consumer(chunk_channel, result_channel))
    end

    # Collect results from the result_channel and merge them
    merged_result = Dict{String, MVector{4, Float32}}()
    finished_consumers = 0
    while finished_consumers < n_consumers
        if isready(result_channel)
            result = take!(result_channel)
            merged_result = merge_dicts(merged_result, result)
        else
            finished_consumers += 1
        end
    end

    # Wait for all tasks to complete
    wait(producer_task)
    for task in consumer_tasks
        wait(task)
    end

    return merged_result
end

# Define the print_stats function to print the result
function print_stats(summary_stats::Dict{String, MVector{4, Float32}})
    sorted_keys = sort(collect(keys(summary_stats)))
    for stn in sorted_keys
        min_temp = summary_stats[stn][1]
        max_temp = summary_stats[stn][2]
        avg_temp = summary_stats[stn][3] / summary_stats[stn][4]
        println("$stn;$min_temp;$max_temp;$avg_temp")
    end
end

# Main script execution
if abspath(PROGRAM_FILE) == @__FILE__  # If the main program being executed is this file
    println(pwd())
    println(ARGS)
    filepath = isempty(ARGS) ? "./measurements_short.txt" : ARGS[1]  # If no args passed use measurements_short.txt
    @time process_file(filepath) |> print_stats       # Process file, pipe results into print_stats function above
end

