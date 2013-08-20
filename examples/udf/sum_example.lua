local function mapper(rec)
	return rec['aggbin']
end

local function reducer(val1, val2)
	return val1 + val2
end

function sum_single_bin(stream)
	return stream : map(mapper) : reduce(reducer)
end
