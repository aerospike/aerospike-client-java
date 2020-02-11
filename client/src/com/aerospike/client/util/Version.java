/*
 * Copyright 2012-2020 Aerospike, Inc.
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
package com.aerospike.client.util;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.Info;
import com.aerospike.client.cluster.Node;
import com.aerospike.client.policy.InfoPolicy;

public final class Version {

	public static Version getServerVersion(AerospikeClient client, InfoPolicy policy) {
		Node node = client.getNodes()[0];
		String response = Info.request(policy, node, "build");
		return new Version(response);
	}

	private final int major;
	private final int minor;
	private final int revision;
	private final String extension;

	public Version(String version) {
		int begin = 0;
		int i = begin;
		int max = version.length();

		while (i < max) {
			if (! Character.isDigit(version.charAt(i))) {
				break;
			}
			i++;
		}

		major = (i > begin)? Integer.parseInt(version.substring(begin, i)) : 0;
		begin = ++i;

		while (i < max) {
			if (! Character.isDigit(version.charAt(i))) {
				break;
			}
			i++;
		}

		minor = (i > begin)? Integer.parseInt(version.substring(begin, i)) : 0;
		begin = ++i;

		while (i < max) {
			if (! Character.isDigit(version.charAt(i))) {
				break;
			}
			i++;
		}

		revision = (i > begin)? Integer.parseInt(version.substring(begin, i)) : 0;
		begin = i;
		extension = (begin < max)? version.substring(begin) : "";
	}

	public boolean isGreaterEqual(int v1, int v2, int v3) {
		return major > v1 || (major == v1 && (minor > v2 || (minor == v2 && revision >= v3)));
	}

	public boolean isLess(int v1, int v2, int v3) {
		return major < v1 || (major == v1 && (minor < v2 || (minor == v2 && revision < v3)));
	}

	@Override
	public String toString() {
		return Integer.toString(major) + "." + minor + "." + revision + extension;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		Version other = (Version) obj;
		return major == other.major && minor == other.minor && revision == other.revision;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + major;
		result = prime * result + minor;
		result = prime * result + revision;
		return result;
	}
}
