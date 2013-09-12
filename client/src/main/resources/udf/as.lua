
-- ############################################################################
--
-- List
--
-- ############################################################################

--
-- Clone a list.
-- This is a shallow copy, meaning the data pointed to by the pointers will
-- not be copied.
--
function list.clone(l)
    local ll = {}
    for v in list.iterator(l) do
        table.insert(ll, v)
    end
    return list(ll)
end

-- Merge two list
-- returns a new list with elements of l1 preceeding of l2
--
function list.merge(l1, l2)
    local ll = {}
    for v in list.iterator(l1) do
        table.insert(ll,v)
    end

    for v in list.iterator(l2) do
        table.insert(ll, v)
    end
    return list(ll)
end

-- ############################################################################
--
-- Map
--
-- ############################################################################

--
-- Create a new Map my merging two maps.
-- The function `f` is a function used to merge the value of matching keys.
--
function map.merge(m1,m2,f)
    local mm = {}
    for k,v in map.pairs(m1) do
        mm[k] = v
    end
    for k,v in map.pairs(m2) do
        mm[k] = (mm[k] and f and type(f) == 'function' and f(m1[k],m2[k])) or v
    end
    return map(mm)
end

--
-- Create a new Map that contains the keys 
-- that are not shared between two maps.
--
function map.diff(m1,m2)
    local mm = {}
    for k,v in map.pairs(m1) do
        if not m2[k] then
            mm[k] = v
        end
    end
    for k,v in map.pairs(m2) do
        if not m1[k] then
            mm[k] = v
        end
    end
    return map(mm)
end

--
-- Clone a map.
-- This is a shallow copy, meaning the data pointed to by the pointers will
-- not be copied.
-- 
function map.clone(m)
    local mm = {}
    for k,v in map.pairs(m) do
        mm[k] = v
    end
    return map(mm)
end

-- ############################################################################
--
-- Math
--
-- ############################################################################

--
-- Sum of the two values
-- 
function math.sum(a,b) 
    return a + b
end


--
-- Product of two values
-- 
function math.product(a, b)
    return a * b
end
