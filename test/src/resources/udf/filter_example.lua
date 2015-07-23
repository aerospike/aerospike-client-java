local function map_profile(record)
	-- Add user and password to returned map.
	-- Could add other record bins here as well.
	return map {name=record.name, password=record.password}
end

function profile_filter(stream,password)
	local function filter_password(record)
		return record.password == password
	end
	return stream : filter(filter_password) : map(map_profile)
end
