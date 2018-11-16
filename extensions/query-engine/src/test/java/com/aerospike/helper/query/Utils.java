package com.aerospike.helper.query;

import com.aerospike.client.query.KeyRecord;

import java.util.ArrayList;
import java.util.List;

public class Utils {

    public static List<KeyRecord> toList(KeyRecordIterator it) {
        List<KeyRecord> result = new ArrayList<>();
        it.forEachRemaining(result::add);
        return result;
    }
}
