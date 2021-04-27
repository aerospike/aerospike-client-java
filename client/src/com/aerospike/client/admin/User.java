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
 * User and assigned roles.
 */
public final class User {
	/**
	 * User name.
	 */
	public String name;

	/**
	 * List of assigned roles.
	 */
	public List<String> roles;

	/**
	 * List of read statistics. List may be null.
	 * Current statistics by offset are:
	 * <ul>
	 * <li>0: read quota in records per second</li>
	 * <li>1: single record read transaction rate (TPS)</li>
	 * <li>2: read scan/query record per second rate (RPS)</li>
	 * <li>3: number of limitless read scans/queries</li>
	 * </ul>
	 * Future server releases may add additional statistics.
	 */
	public List<Integer> readInfo;

	/**
	 * List of write statistics. List may be null.
	 * Current statistics by offset are:
	 * <ul>
	 * <li>0: write quota in records per second</li>
	 * <li>1: single record write transaction rate (TPS)</li>
	 * <li>2: write scan/query record per second rate (RPS)</li>
	 * <li>3: number of limitless write scans/queries</li>
	 * </ul>
	 * Future server releases may add additional statistics.
	 */
	public List<Integer> writeInfo;

	/**
	 * Number of currently open connections.
	 */
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
