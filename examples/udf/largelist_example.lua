require('aerospike')
local exports = {}

function exports.my_filter_func(key,args)
	if key==3 then
		return key
	else
		return nil
	end
end

return exports
