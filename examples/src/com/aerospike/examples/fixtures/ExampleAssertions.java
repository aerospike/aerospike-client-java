/*
 * Copyright 2012-2026 Aerospike, Inc.
 *
 * Portions may be licensed to Aerospike, Inc. under one or more contributor
 * license agreements WHICH ARE COMPATIBLE WITH THE APACHE LICENSE, VERSION 2.0.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package com.aerospike.examples.fixtures;

import java.util.Arrays;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import com.aerospike.client.IAerospikeClient;
import com.aerospike.client.Key;
import com.aerospike.client.Record;
import com.aerospike.client.policy.Policy;
import com.aerospike.client.policy.QueryPolicy;
import com.aerospike.client.query.RecordSet;
import com.aerospike.client.query.ResultSet;
import com.aerospike.client.query.Statement;

public final class ExampleAssertions {
	private ExampleAssertions() {
	}

	public static Record assertRecordExists(IAerospikeClient client, Policy policy, Key key, String... binNames) {
		Record record = (binNames == null || binNames.length == 0) ?
			client.get(policy, key) : client.get(policy, key, binNames);

		if (record == null) {
			throw new IllegalStateException("Expected record to exist: " + describe(key));
		}
		return record;
	}

	public static void assertRecordMissing(IAerospikeClient client, Policy policy, Key key) {
		if (client.get(policy, key) != null) {
			throw new IllegalStateException("Expected record to be absent: " + describe(key));
		}
	}

	public static void assertBinEquals(
		IAerospikeClient client,
		Policy policy,
		Key key,
		String binName,
		Object expected
	) {
		Record record = assertRecordExists(client, policy, key, binName);
		Object actual = record.getValue(binName);

		if (! Objects.equals(expected, actual)) {
			throw new IllegalStateException(String.format(
				"Expected %s bin %s to equal %s but found %s",
				describe(key), binName, expected, actual));
		}
	}

	public static void assertBinDeepEquals(
		IAerospikeClient client,
		Policy policy,
		Key key,
		String binName,
		Object expected
	) {
		Record record = assertRecordExists(client, policy, key, binName);
		assertDeepEquals(
			String.format("%s bin %s", describe(key), binName),
			expected,
			record.getValue(binName));
	}

	public static void assertBinRemoved(IAerospikeClient client, Policy policy, Key key, String binName) {
		Record record = assertRecordExists(client, policy, key);

		if (record.getValue(binName) != null) {
			throw new IllegalStateException(String.format(
				"Expected %s bin %s to be removed but found %s",
				describe(key), binName, record.getValue(binName)));
		}
	}

	public static void assertListTailEquals(
		IAerospikeClient client,
		Policy policy,
		Key key,
		String binName,
		Object expectedTail
	) {
		Record record = assertRecordExists(client, policy, key, binName);
		Object value = record.getValue(binName);

		if (! (value instanceof List<?>)) {
			throw new IllegalStateException(String.format(
				"Expected %s bin %s to contain a list but found %s",
				describe(key), binName, value));
		}

		List<?> list = (List<?>)value;

		if (list.isEmpty()) {
			throw new IllegalStateException(String.format(
				"Expected %s bin %s to contain at least one value",
				describe(key), binName));
		}

		Object actualTail = list.get(list.size() - 1);

		if (! Objects.equals(expectedTail, actualTail)) {
			throw new IllegalStateException(String.format(
				"Expected %s bin %s to end with %s but found %s",
				describe(key), binName, expectedTail, actualTail));
		}
	}

	public static void assertBlobEquals(
		IAerospikeClient client,
		Policy policy,
		Key key,
		String binName,
		byte[] expected
	) {
		Record record = assertRecordExists(client, policy, key, binName);
		Object value = record.getValue(binName);

		if (! (value instanceof byte[])) {
			throw new IllegalStateException(String.format(
				"Expected %s bin %s to contain bytes but found %s",
				describe(key), binName, value));
		}

		byte[] actual = (byte[])value;

		if (! Arrays.equals(expected, actual)) {
			throw new IllegalStateException(String.format(
				"Expected %s bin %s bytes %s but found %s",
				describe(key), binName, Arrays.toString(expected), Arrays.toString(actual)));
		}
	}

	public static void assertQueryCount(IAerospikeClient client, Policy policy, Statement stmt, long expected) {
		assertQueryCount(client, queryPolicy(policy), stmt, expected);
	}

	public static void assertQueryCount(IAerospikeClient client, QueryPolicy policy, Statement stmt, long expected) {
		long count = countQuery(client, policy, stmt);

		if (count != expected) {
			throw new IllegalStateException(
				"Expected query to return " + expected + " records but found " + count);
		}
	}

	public static long countQuery(IAerospikeClient client, QueryPolicy policy, Statement stmt) {
		try (RecordSet recordSet = client.query(policy, stmt)) {
			long count = 0;

			while (recordSet.next()) {
				count++;
			}
			return count;
		}
	}

	public static void assertQueryUserKeys(
		IAerospikeClient client,
		QueryPolicy policy,
		Statement stmt,
		long expectedCount
	) {
		try (RecordSet recordSet = client.query(policy, stmt)) {
			long count = 0;

			while (recordSet.next()) {
				Key key = recordSet.getKey();

				if (key == null || key.userKey == null) {
					throw new IllegalStateException("Expected query result to include user keys");
				}
				count++;
			}

			if (count != expectedCount) {
				throw new IllegalStateException(
					"Expected query to return " + expectedCount + " records but found " + count);
			}
		}
	}

	public static void assertQueryUniqueBinValueCount(
		IAerospikeClient client,
		QueryPolicy policy,
		Statement stmt,
		String binName,
		long expectedUniqueCount
	) {
		try (RecordSet recordSet = client.query(policy, stmt)) {
			Set<Object> uniques = new LinkedHashSet<>();

			while (recordSet.next()) {
				Record record = recordSet.getRecord();
				uniques.add(record.getValue(binName));
			}

			if (uniques.size() != expectedUniqueCount) {
				throw new IllegalStateException(String.format(
					"Expected query to return %d unique %s values but found %d",
					expectedUniqueCount,
					binName,
					uniques.size()));
			}
		}
	}

	public static void assertSetRecordCount(
		IAerospikeClient client,
		QueryPolicy policy,
		String namespace,
		String setName,
		long expected
	) {
		Statement stmt = new Statement();
		stmt.setNamespace(namespace);
		stmt.setSetName(setName);
		assertQueryCount(client, policy, stmt, expected);
	}

	public static void assertHeaderHasGeneration(Record record, Key key) {
		if (record == null) {
			throw new IllegalStateException("Expected record header to exist: " + describe(key));
		}

		if (record.generation <= 0) {
			throw new IllegalStateException(String.format(
				"Expected record header generation for %s to be > 0 but found %d",
				describe(key),
				record.generation));
		}
	}

	public static void assertRecordHasOnlyBins(Record record, Key key, String... expectedBins) {
		Set<String> actualBins = new LinkedHashSet<>(record.bins.keySet());
		Set<String> expectedBinSet = new LinkedHashSet<>(Arrays.asList(expectedBins));

		if (! actualBins.equals(expectedBinSet)) {
			throw new IllegalStateException(String.format(
				"Expected %s to contain bins %s but found %s",
				describe(key),
				expectedBinSet,
				actualBins));
		}
	}

	public static void assertRecordBinsDeepEquals(Record record, Key key, Map<String, ?> expectedBins) {
		assertRecordHasOnlyBins(record, key, expectedBins.keySet().toArray(new String[0]));

		for (Map.Entry<String, ?> entry : expectedBins.entrySet()) {
			assertDeepEquals(
				String.format("%s bin %s", describe(key), entry.getKey()),
				entry.getValue(),
				record.getValue(entry.getKey()));
		}
	}

	public static void assertAggregateSingleLong(ResultSet resultSet, long expected) {
		List<Object> results = readAggregateResults(resultSet);

		if (results.size() != 1) {
			throw new IllegalStateException("Expected a single aggregate result but found " + results.size());
		}

		Object value = results.get(0);

		if (! (value instanceof Number) || ((Number)value).longValue() != expected) {
			throw new IllegalStateException("Expected aggregate result " + expected + " but found " + value);
		}
	}

	public static void assertAggregateSingleMapFields(ResultSet resultSet, Map<String, ?> expectedFields) {
		List<Object> results = readAggregateResults(resultSet);

		if (results.size() != 1) {
			throw new IllegalStateException("Expected a single aggregate result but found " + results.size());
		}

		Object value = results.get(0);

		if (! (value instanceof Map<?,?>)) {
			throw new IllegalStateException("Expected aggregate result map but found " + value);
		}

		@SuppressWarnings("unchecked")
		Map<Object,Object> actual = (Map<Object,Object>)value;

		for (Map.Entry<String, ?> entry : expectedFields.entrySet()) {
			if (! actual.containsKey(entry.getKey())) {
				throw new IllegalStateException("Expected aggregate result to contain field " + entry.getKey());
			}
			assertDeepEquals("aggregate field " + entry.getKey(), entry.getValue(), actual.get(entry.getKey()));
		}
	}

	public static void assertDeepEquals(String label, Object expected, Object actual) {
		if (expected == actual) {
			return;
		}

		if (expected == null || actual == null) {
			throw new IllegalStateException(label + " mismatch: expected " + expected + " but found " + actual);
		}

		if (expected instanceof byte[] && actual instanceof byte[]) {
			if (! Arrays.equals((byte[])expected, (byte[])actual)) {
				throw new IllegalStateException(label + " mismatch: expected " +
					Arrays.toString((byte[])expected) + " but found " + Arrays.toString((byte[])actual));
			}
			return;
		}

		if (expected instanceof List<?> && actual instanceof List<?>) {
			assertListDeepEquals(label, (List<?>)expected, (List<?>)actual);
			return;
		}

		if (expected instanceof Map<?,?> && actual instanceof Map<?,?>) {
			assertMapDeepEquals(label, (Map<?,?>)expected, (Map<?,?>)actual);
			return;
		}

		if (! Objects.equals(expected, actual)) {
			throw new IllegalStateException(label + " mismatch: expected " + expected + " but found " + actual);
		}
	}

	static QueryPolicy queryPolicy(Policy policy) {
		if (policy == null) {
			return null;
		}
		return (policy instanceof QueryPolicy) ?
			new QueryPolicy((QueryPolicy)policy) : new QueryPolicy(policy);
	}

	private static String describe(Key key) {
		return String.format("namespace=%s set=%s key=%s", key.namespace, key.setName, key.userKey);
	}

	private static void assertListDeepEquals(String label, List<?> expected, List<?> actual) {
		if (expected.size() != actual.size()) {
			throw new IllegalStateException(label + " size mismatch: expected " + expected.size() + " but found " + actual.size());
		}

		for (int i = 0; i < expected.size(); i++) {
			assertDeepEquals(label + '[' + i + ']', expected.get(i), actual.get(i));
		}
	}

	private static void assertMapDeepEquals(String label, Map<?,?> expected, Map<?,?> actual) {
		if (expected.size() != actual.size()) {
			throw new IllegalStateException(label + " size mismatch: expected " + expected.size() + " but found " + actual.size());
		}

		Map<Object,Object> remaining = new LinkedHashMap<>(actual);

		for (Map.Entry<?,?> entry : expected.entrySet()) {
			Object matchingKey = removeMatchingKey(remaining.keySet(), entry.getKey());

			if (matchingKey == null) {
				throw new IllegalStateException(label + " missing key " + entry.getKey());
			}

			assertDeepEquals(label + '[' + entry.getKey() + ']', entry.getValue(), actual.get(matchingKey));
			remaining.remove(matchingKey);
		}

		if (! remaining.isEmpty()) {
			throw new IllegalStateException(label + " has unexpected keys " + remaining.keySet());
		}
	}

	private static Object removeMatchingKey(Collection<Object> keys, Object expectedKey) {
		for (Object key : keys) {
			if (deepEquals(key, expectedKey)) {
				return key;
			}
		}
		return null;
	}

	private static boolean deepEquals(Object left, Object right) {
		try {
			assertDeepEquals("value", left, right);
			return true;
		}
		catch (IllegalStateException ignored) {
			return false;
		}
	}

	private static List<Object> readAggregateResults(ResultSet resultSet) {
		try (ResultSet rs = resultSet) {
			List<Object> results = new ArrayList<>();

			while (rs.next()) {
				results.add(rs.getObject());
			}
			return results;
		}
	}
}
