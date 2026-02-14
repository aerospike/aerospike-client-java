/*
 * Copyright 2012-2021 Aerospike, Inc.
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
package com.aerospike.client.admin;

import java.util.List;

/**
 * User record: name, assigned roles, optional read/write stats and connection count. Returned by {@link com.aerospike.client.AerospikeClient#queryUser} and in the list from {@link com.aerospike.client.AerospikeClient#queryUsers}.
 * <p>
 * {@link #readInfo} and {@link #writeInfo} are server-defined statistic lists (e.g. quota, TPS, RPS); offsets may change in future server versions.
 *
 * <p><b>Example:</b>
 * <p>Query a user and read name and roles.</p>
 * <pre>{@code
 * IAerospikeClient client = new AerospikeClient("localhost", 3000);
 * try {
 *   User user = client.queryUser(null, "jdoe");
 *   if (user != null) {
 *     String name = user.name;
 *     List roles = user.roles;
 *   }
 * } finally {
 *   client.close();
 * }
 * }</pre>
 *
 * @see com.aerospike.client.AerospikeClient#queryUser
 * @see com.aerospike.client.AerospikeClient#queryUsers
 * @see com.aerospike.client.AerospikeClient#createUser
 * @see Role
 */
public final class User {
	/** User name. */
	public String name;

	/** Assigned role names. */
	public List<String> roles;

	/**
	 * Read statistics (may be null). Current offsets: 0 = read quota (RPS), 1 = read TPS, 2 = read scan/query RPS, 3 = limitless read scans/queries. Future server versions may add more.
	 */
	public List<Integer> readInfo;

	/**
	 * Write statistics (may be null). Current offsets: 0 = write quota (RPS), 1 = write TPS, 2 = write scan/query RPS, 3 = limitless write scans/queries. Future server versions may add more.
	 */
	public List<Integer> writeInfo;

	/** Number of currently open connections for this user. */
	public int connsInUse;

	public String toString() {
		return "User [name=" + name + ", roles=" + roles + ", readInfo=" + readInfo + ", writeInfo=" + writeInfo
				+ ", connsInUse=" + connsInUse + "]";
	}

	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((name == null) ? 0 : name.hashCode());
		return result;
	}

	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		User other = (User) obj;
		if (name == null) {
			if (other.name != null)
				return false;
		} else if (!name.equals(other.name))
			return false;
		return true;
	}
}
