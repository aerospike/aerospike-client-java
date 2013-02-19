
local function _join(r,delim,...)
    local out = ''
    local len = select('#',...)
    for i=1, len do
        if i > 1 then
            out = out .. (delim or ',')
        end
        out = out .. r[select(i,...)]
    end
    return out
end

function join(r,delim,...)
    return _join(r,delim,...)
end

function cat(r,...)
    return _join(r,'',...)
end

-- Generic example
function example_lua(r,arg1,arg2,arg3,arg4)
    r[arg1] = arg2;
    r[arg3] = arg4;
    aerospike:update(r);
    return r['b'];
end

-- Get a particular bin
function getbin(r,name)
    return r[name]
end

-- Set a particular bin
function setbin(r,name,value)
    if not aerospike:exists(r) then aerospike:create(r) end
    local old = r[name]
    r[name] = value
    aerospike:update(r)
    return old
end

-- Set a particular bin
function setbin2(r,name,value)
    local old = r[name]
    r[name] = value
    if not aerospike:exists(r) then
        aerospike:create(r)
    else
        aerospike:create(r)
    end
    return old
end

-- Set a particular bin
function set_and_get(r,name,value)
    r[name] = value
    aerospike:update(r)
    return r[name]
end

-- Remove a paritcular bin
function remove(r,name)
    local old = r[name]
    r[name] = nil
    aerospike:update(r)
    return old
end

-- Create a record
function create_record(r,b1,v1,b2,v2)
     if not aerospike:exists(r) then
    	 aerospike:create(r);
 	    info("created");
 	 else 
 	     info("not created record already exists");
 	end
 	if b1 and v1 then 
        r[b1] = v1
        if b2 and v2 then 
            r[b2] = v2
        end
        aerospike:update(r);
        info("updated!")
    end
    return r[b1]
 end

-- delete a record
function delete_record(r,name)
    info ("here");
    if aerospike:exist(r) then
        info("exist. delete now");
    	return aerospike.remove(r)
    else
        info("not exist, delete should do nothing");
    	return aerospike.remove(r)
    end
 end

-- @TODO return record as is
-- function echo_record(record) 
--	return record;
-- end


