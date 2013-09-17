local function putBin(r,name,value)
    if not aerospike:exists(r) then aerospike:create(r) end
    r[name] = value
    aerospike:update(r)
end

-- Set a particular bin
function writeBin(r,name,value)
    putBin(r,name,value)
end

-- Get a particular bin
function readBin(r,name)
    return r[name]
end

-- Return generation count of record
function getGeneration(r)
    return record.gen(r)
end

-- Update record only if gen hasn't changed
function writeIfGenerationNotChanged(r,name,value,gen)
    if record.gen(r) == gen then
        r[name] = value
        aerospike:update(r)
    end
end

-- Set a particular bin only if record does not already exist.
function writeUnique(r,name,value)
    if not aerospike:exists(r) then 
        aerospike:create(r) 
        r[name] = value
        aerospike:update(r)
    end
end

-- Validate value before writing.
function writeWithValidation(r,name,value)
    if (value >= 1 and value <= 10) then
        putBin(r,name,value)
    else
        error("1000:Invalid value") 
    end
end

-- Set expiration of record
-- function expire(r,ttl)
--    if record.ttl(r) == gen then
--        r[name] = value
--        aerospike:update(r)
--    end
-- end
