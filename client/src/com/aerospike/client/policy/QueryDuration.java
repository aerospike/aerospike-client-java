/*
 * Copyright 2012-2024 Aerospike, Inc.
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
package com.aerospike.client.policy;

/**
 * Expected query duration. The server treats the query in different ways depending on the expected duration.
 * This enum is ignored for aggregation queries, background queries and server versions &lt; 6.0.
 */
public enum QueryDuration {
	/**
	 * The query is expected to return more than 100 records per node. The server optimizes for a large record set in
	 * the following ways:
	 * <ul>
	 * <li>Allow query to be run in multiple threads using the server's query threading configuration.</li>
	 * <li>Do not relax read consistency for AP namespaces.</li>
	 * <li>Add the query to the server's query monitor.</li>
	 * <li>Do not add the overall latency to the server's latency histogram.</li>
	 * <li>Do not allow server timeouts.</li>
	 * </ul>
	 */
	LONG,

	/**
	 * The query is expected to return less than 100 records per node. The server optimizes for a small record set in
	 * the following ways:
	 * <ul>
	 * <li>Always run the query in one thread and ignore the server's query threading configuration.</li>
	 * <li>Allow query to be inlined directly on the server's service thread.</li>
	 * <li>Relax read consistency for AP namespaces.</li>
	 * <li>Do not add the query to the server's query monitor.</li>
	 * <li>Add the overall latency to the server's latency histogram.</li>
	 * <li>Allow server timeouts. The default server timeout for a short query is 1 second.</li>
	 * </ul>
	 */
	SHORT,

	/**
	 * Treat query as a LONG query, but relax read consistency for AP namespaces.
	 * This value is treated exactly like LONG for server versions &lt; 7.1.
	 */
	LONG_RELAX_AP
}
