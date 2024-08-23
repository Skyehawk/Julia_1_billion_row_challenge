using Mmap
using ThreadsX #BenchmarkTools
using Base.Iterators: enumerate
using Base: @inbounds

# Function to hash SubArray{UInt8} with length adjustment
# TODO: There may be even more time we can pull out of this hash
function hash_subarray(subarray::SubArray{UInt8})
 hash_value = 0
 for byte in subarray
  hash_value = hash_value * 256 + byte
 end
 # Incorporate the length of the subarray into the hash value to differentiate by length
 return hash_value * 256 + length(subarray)
end

# Define the process_chunk function to process each chunk
# TODO: Refactor "temp" to something that doesn't imply temporary 
function process_chunk(mapped_data::Vector{UInt8}, start_idx::Int, end_idx::Int)
 data_dict = Dict{Int64,Vector{Int64}}()
 name_dict = Dict{Int64,SubArray{UInt8}}()  # Mapping from hash to station name

 i = start_idx
 leftover_start = start_idx
 incomplete_lines = Vector{Tuple{Int,Int}}()

 @inbounds while i <= end_idx
  if mapped_data[i] == 0x0a || i == end_idx    # 0x0a is the newline char
   line_data = view(mapped_data, leftover_start:i-1) # Avoid allocating additional memory for line data
   leftover_start = i + 1

   semicolon_index = find_semicolon_from_end(line_data)
   # WARN: we do NOT currently handle the case where we didn't find a semicolon index (begining of chunk)
   if semicolon_index > 0    # 4 is the minimum value of the temp data, checks for > 0 are faster than > 4
    station_data = view(line_data, 1:(semicolon_index-1))    # Station name
    temp_data = view(line_data, (semicolon_index+1):length(line_data))    # Temperature data
    temp_int = extract_temperature(temp_data)

    station_hash = hash_subarray(station_data)

    if haskey(data_dict, station_hash)
     current_values = data_dict[station_hash]    # Cache the current values at the expense of memory allocaiton
     current_values[1] = ifelse(temp_int < current_values[1], temp_int, current_values[1])   # Min
     current_values[2] = ifelse(temp_int > current_values[2], temp_int, current_values[2])   # Max
     current_values[3] += temp_int   # Sum
     current_values[4] += 1    # Count
    else
     data_dict[station_hash] = [temp_int, temp_int, temp_int, 1]
     name_dict[station_hash] = station_data  # Store station name, we convert to string once dicts are merged
    end
   end
   i += 5    # Minimum line length is 5 bytes (e.g., 'a;0.0')
   # TODO: push! the first line to incomplete_lines no matter what - we cannot guarentee it is complete
  end
  i += 1 # Iterate the counter one more time (independent of our += 5 above)
 end

 if leftover_start <= end_idx
  push!(incomplete_lines, (leftover_start, end_idx))
 end

 return data_dict, name_dict, incomplete_lines
end

# WARN: This function is only safe if station/data Subarray that contain AT LEAST 4 bytes
# TODO: We may be able to check the more common indicies first (-5,-6,-4), but we need to handle chance that 6 
# could be out of bounds.
#=
function find_semicolon_from_end(line_data::SubArray{UInt8})
 for i in length(line_data)-4:-1:1    # Start from the end of the line
  if line_data[i] == 0x3b  # ASCII ';'
   return i
  end
 end
 return -1
end
=#

@inline function find_semicolon_from_end(line_data::SubArray{UInt8})
 n = length(line_data)
 @inbounds begin
  if n >= 7
   x5 = line_data[n-5]
   x6 = line_data[n-6]
   x4 = line_data[n-4]
   if x5 == 0x3b
    return n - 5
   elseif x6 == 0x3b
    return n - 6
   elseif x4 == 0x3b
    return n - 4
   end
  end
 end
 return -1  # or some other default value
end


# WARN: This function is only safe if temperature data SubArray ONLY contains '-', '.', or [0-9]
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
 combined_result = Dict{Int64,Vector{Int64}}()
 name_mapping = Dict{Int64,SubArray{UInt8}}()
 incomplete_lines = Vector{Tuple{Int,Int}}()

 for result in results
  merge_dicts!(combined_result, result[1])
  merge_name_dicts!(name_mapping, result[2])
  append!(incomplete_lines, result[3])    #TODO: Need to fix this so the dozen or so incompletes are properly processed
 end

 for (start_idx, end_idx) in incomplete_lines
  partial_result, partial_names, _ = process_chunk(mapped_data, start_idx, end_idx)
  merge_dicts!(combined_result, partial_result)
  merge_name_dicts!(name_mapping, partial_names)
 end

 return combined_result, name_mapping
end

function merge_dicts!(d1::Dict{Int64,Vector{Int64}}, d2::Dict{Int64,Vector{Int64}})
 for (key, value) in d2
  if @inbounds haskey(d1, key)
   v1 = d1[key]    # Cache the lookup at the expense of memory allocation
   v1[1] = ifelse(value[1] < v1[1], value[1], v1[1])    # Min
   v1[2] = ifelse(value[2] > v1[2], value[2], v1[2])    # Max
   v1[3] += value[3]                                              # Sum
   v1[4] += value[4]                                              # Count
  else
   d1[key] = value
  end
 end
end

function merge_name_dicts!(d1::Dict{Int64,SubArray{UInt8}}, d2::Dict{Int64,SubArray{UInt8}})
 for (key, value) in d2
  if @inbounds !haskey(d1, key) # only add the new k,v from d2 if it is absent in d1
   d1[key] = value
  end
 end
end

function print_stats(summary_stats::Dict{Int64,Vector{Int64}}, name_mapping::Dict{Int64,SubArray{UInt8}})
 #sorted_keys = sort(collect(keys(summary_stats)))  # just sort the hashes, but these will not be in lexicographical order 
 sorted_keys = [key for (key, _) in sort(collect(name_mapping), by=x -> x[2])] # vector of names' keys in sorted order of the values  
 for stn_hash in sorted_keys
  station_name = String(name_mapping[stn_hash])    # convert to string way down here to avoid redundent conversions
  summary_vals = summary_stats[stn_hash]    # Cache the lookup
  min_temp = summary_vals[1] / 10
  max_temp = summary_vals[2] / 10
  avg_temp = (summary_vals[3] / 10) / summary_vals[4]
  println("Station Name: $station_name; Min: $min_temp; Max: $max_temp; Avg: $avg_temp")
 end
end

if abspath(PROGRAM_FILE) == @__FILE__
 println(pwd())
 println(ARGS)
 file_path = isempty(ARGS) ? "./measurements_1B.txt" : ARGS[1]
 @time begin
  stats, name_mapping = process_file(file_path)
  print_stats(stats, name_mapping)
 end
end

