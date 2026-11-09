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
package com.aerospike.test.util;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Immutable map literals for Java 8, in place of {@code Map.of}.
 */
public final class TestMaps {
	public static <K,V> Map<K,V> of() {
		return Collections.emptyMap();
	}

	public static <K,V> Map<K,V> of(K k1, V v1) {
		return build(k1, v1);
	}

	public static <K,V> Map<K,V> of(K k1, V v1, K k2, V v2) {
		return build(k1, v1, k2, v2);
	}

	public static <K,V> Map<K,V> of(K k1, V v1, K k2, V v2, K k3, V v3) {
		return build(k1, v1, k2, v2, k3, v3);
	}

	public static <K,V> Map<K,V> of(K k1, V v1, K k2, V v2, K k3, V v3, K k4, V v4) {
		return build(k1, v1, k2, v2, k3, v3, k4, v4);
	}

	@SuppressWarnings("unchecked")
	private static <K,V> Map<K,V> build(Object... kv) {
		Map<K,V> map = new LinkedHashMap<>();

		for (int i = 0; i < kv.length; i += 2) {
			K key = (K)kv[i];
			V value = (V)kv[i + 1];

			if (key == null || value == null) {
				throw new NullPointerException();
			}

			if (map.put(key, value) != null) {
				throw new IllegalArgumentException("duplicate key: " + key);
			}
		}
		return Collections.unmodifiableMap(map);
	}
}
