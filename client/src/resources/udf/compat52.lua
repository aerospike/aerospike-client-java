--
-- The server uses a Lua 5.1 interpreter and the java client uses a Lua 5.2 interpreter from the
-- "Luaj" library.
--
-- Luaj's 5.1 implementation (2.0.3) is not multi-threaded or even multi VM state.   
-- Luaj's 5.2 implementation (3.0) is multi-threaded as long as each as each thread uses a distinct VM state.  
-- Therefore, Luaj 3.0 and Lua 5.2 must be used for the java client.
--
-- This script emulates 5.1 features that were deprecated in 5.2.  Therefore, Lua 5.1 scripts can be written
-- that also run within a 5.2 interpreter.
--
local lua_debug = debug

setfenv = setfenv or function(f, t)
    f = (type(f) == 'function' and f or lua_debug.getinfo(f + 1, 'f').func)
    local name
    local up = 0
    repeat
        up = up + 1
        name = lua_debug.getupvalue(f, up)
    until name == '_ENV' or name == nil
    if name then
        lua_debug.upvaluejoin(f, up, function() return t end, 1)
    end
end

unpack = table.unpack
