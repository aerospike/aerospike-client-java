local function map_loc(rec)
  -- info('%s %s', rec.filteramenity, tostring(rec.filterloc))
  return rec.filterloc
end

function match_amenity(stream, val)
  local function filter_amenity(rec)
    return rec.filteramenity == val
  end
  return stream : filter(filter_amenity) : map(map_loc)
end
