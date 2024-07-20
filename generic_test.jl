using CSV
using ThreadsX
using Base.Threads
using Tables

function process_chunk(chunk)
    # Convert temperature to integer (multiplied by 10) for processing
    sum_col = sum(row -> Int(row[2] * 10), Tables.rows(chunk))
    return Dict("chunk_sum" => Dict("sum" => sum_col))
end

function process_file(file_path::String)
    chunk_size = 100_000  # Adjust the chunk size based on your needs and available memory
    results = Vector{Dict{String, Dict{String, Int}}}()
    types = Dict(1 => String, 2 => Float32)  # Define types explicitly

    open(file_path) do file
        reader = CSV.File(file; header=false, delim=";", types=types, buffer_in_memory=false)
        iterator = Tables.namedtupleiterator(reader)
        
        chunk = []
        for row in iterator
            push!(chunk, row)
            if length(chunk) >= chunk_size
                chunk_result = Threads.@spawn process_chunk(chunk)
                push!(results, fetch(chunk_result))
                chunk = []  # Reset chunk
            end
        end

        # Process the remaining rows
        if !isempty(chunk)
            chunk_result = Threads.@spawn process_chunk(chunk)
            push!(results, fetch(chunk_result))
        end
    end

    # Combine results from all chunks
    total_result = ThreadsX.reduce(merge_dicts, results; init=Dict{String, Dict{String, Int}}())
    return total_result
end

function merge_dicts(d1::Dict, d2::Dict)
    for (k, v) in d2
        if haskey(d1, k)
            for (sub_k, sub_v) in v
                if haskey(d1[k], sub_k)
                    d1[k][sub_k] += sub_v
                else
                    d1[k][sub_k] = sub_v
                end
            end
        else
            d1[k] = v
        end
    end
    return d1
end

function convert_result_to_float(result::Dict{String, Dict{String, Int}})
    converted_result = Dict{String, Dict{String, Float32}}()
    for (k, v) in result
        converted_result[k] = Dict{String, Float32}()
        for (sub_k, sub_v) in v
            converted_result[k][sub_k] = sub_v / 10.0
        end
    end
    return converted_result
end

file_path = "./measurements_10M.txt"
@time result = process_file(file_path)
final_result = convert_result_to_float(result)
println(final_result)

