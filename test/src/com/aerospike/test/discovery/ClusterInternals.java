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
package com.aerospike.test.discovery;

import java.lang.reflect.Field;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.Host;
import com.aerospike.client.cluster.Cluster;
import com.aerospike.client.cluster.Node;

/**
 * Client state the discovery tests assert on that the client does not expose.
 */
final class ClusterInternals {
	private ClusterInternals() {
	}

	/**
	 * Return the seed list the client holds for re-seeding.
	 */
	static Host[] seeds(AerospikeClient client) {
		return (Host[])get(Cluster.class, "seeds", client.getCluster());
	}

	/**
	 * Return the node's count of consecutive failed tend refreshes; the client drops a node at 5
	 * when no node answers.
	 */
	static int failures(Node node) {
		return (Integer)get(Node.class, "failures", node);
	}

	private static Object get(Class<?> type, String name, Object target) {
		try {
			Field f = type.getDeclaredField(name);
			f.setAccessible(true);
			return f.get(target);
		}
		catch (ReflectiveOperationException e) {
			throw new IllegalStateException(type.getSimpleName() + '.' + name + " is not readable", e);
		}
	}
}
