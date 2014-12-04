require('aerospike')
local exports = {}

function exports.my_filter_func(key,args)
	return key == 3
end

return exports
