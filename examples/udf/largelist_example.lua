require('aerospike')
local exports = {}

function exports.my_filter_func(key,args)
	if key == args[1] then
		return key
	else
		return nil
	end
end

return exports
