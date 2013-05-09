function sum_single_bin(s)

    local function mapper(rec)
    	return rec['aggbin']
    end

    local function reducer(val1, val2)
        return val1 + val2
    end

    return s : map(mapper) : reduce(reducer)
end
