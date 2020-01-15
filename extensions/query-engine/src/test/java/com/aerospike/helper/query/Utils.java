package com.aerospike.helper.query;

import com.aerospike.client.query.KeyRecord;
import org.assertj.core.api.Condition;
import org.assertj.core.api.HamcrestCondition;
import org.hamcrest.Matcher;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Supplier;

public class Utils {

    public static List<KeyRecord> toList(KeyRecordIterator it) {
        List<KeyRecord> result = new ArrayList<>();
        it.forEachRemaining(result::add);
        return result;
    }

    public static void tryWith(Supplier<KeyRecordIterator> supplier, Consumer<KeyRecordIterator> function) throws IOException {
        try (KeyRecordIterator it = supplier.get()) {
            function.accept(it);
        }
    }

    public static Condition<String> of(String expected) {
        return new Condition<>(actual -> actual.equals(expected), "expected: %s", expected);
    }

    public static Condition<Object> of(Object expected) {
        return new Condition<>(actual -> actual.equals(expected), "expected: %s", expected);
    }

    public static Condition<Integer> of(int expected) {
        return new Condition<>(actual -> actual == expected, "expected: %s", expected);
    }

    public static Condition<Long> of(long expected) {
        return new Condition<>(actual -> actual == expected, "expected: %s", expected);
    }

}
