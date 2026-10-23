-- Return the argument unchanged.
function echo(r, value)
    return value
end

local function describe(value)
    local result = map()
    result["size"] = bytes.size(value)
    result["type"] = bytes.get_type(value)
    result["value"] = value
    return result
end

-- The stages after reduce run in the client, so describe sees the vector as client-side Lua
-- receives it from the server.
function describe_bin(stream, name)
    local function mapper(rec)
        return rec[name]
    end
    local function keep_first(value1, value2)
        return value1
    end
    return stream : map(mapper) : reduce(keep_first) : map(describe)
end

-- Describe the vector argument as client-side Lua receives it.
function describe_arg(stream, value)
    local function one(rec)
        return 1
    end
    local function add(count1, count2)
        return count1 + count2
    end
    local function describe_value(count)
        return describe(value)
    end
    return stream : map(one) : reduce(add) : map(describe_value)
end
