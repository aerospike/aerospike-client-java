/*
 * Copyright 2012-2025 Aerospike, Inc.
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

import java.net.InetSocketAddress;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.IAerospikeClient;
import com.aerospike.client.Info;
import com.aerospike.client.cluster.Node;
import com.aerospike.client.policy.InfoPolicy;

public final class Version implements Comparable<Version> {
	public static final Version SERVER_VERSION_8_1 = new Version(8, 1, 0, 0);
	public static final Version SERVER_VERSION_PSCAN = new Version(4, 9, 0, 3);
	public static final Version SERVER_VERSION_QUERY_SHOW = new Version(5, 7, 0, 0);
	public static final Version SERVER_VERSION_PQUERY_BATCH_ANY = new Version(6, 0, 0, 0);

	public static Version getServerVersion(IAerospikeClient client, InfoPolicy policy) {
		Node node = client.getCluster().getRandomNode();
		return getServerVersion(policy, node);
	}

	public static Version getServerVersion(InfoPolicy policy, Node node) {
		String response = Info.request(policy, node, "build");
		return new Version(response);
	}

	private final int major;
	private final int minor;
	private final int patch;
	private final int build;

	public static Version convertStringToVersion(String strVersion, String nodeName, InetSocketAddress primaryAddress) {
		Version version = new Version(strVersion);
		if (!strVersion.startsWith(version.toString())) {
			throw new AerospikeException("Node " + nodeName + " " + primaryAddress.toString() + " version is invalid: " + strVersion);
		}
		return version;
	}

	public Version(String version) {
		int begin = 0;
		int i = begin;
		int max = version.length();

		i = getNextVersionDigitEndOffset(i, max, version);
		major = (i > begin)? Integer.parseInt(version.substring(begin, i)) : 0;
		begin = ++i;

		i = getNextVersionDigitEndOffset(i, max, version);
		minor = (i > begin)? Integer.parseInt(version.substring(begin, i)) : 0;
		begin = ++i;

		i = getNextVersionDigitEndOffset(i, max, version);
		patch = (i > begin)? Integer.parseInt(version.substring(begin, i)) : 0;
		begin = ++i;

		i = getNextVersionDigitEndOffset(i, max, version);
		build = (i > begin)? Integer.parseInt(version.substring(begin, i)) : 0;
	}

	public Version(int major, int minor, int patch, int build) {
		this.major = major;
		this.minor = minor;
		this.patch = patch;
		this.build = build;
	}

	private int getNextVersionDigitEndOffset(int i, int max, String version) {
		while (i < max) {
			if (! Character.isDigit(version.charAt(i))) {
				break;
			}
			i++;
		}
		return i;
	}

	@Override
    public int compareTo(Version other) {
        if (this.major != other.major) {
			return Integer.compare(this.major, other.major);
		}
        if (this.minor != other.minor) {
			return Integer.compare(this.minor, other.minor);
		} 
        if (this.patch != other.patch) {
			return Integer.compare(this.patch, other.patch);
		}

        return Integer.compare(this.build, other.build);
    }

    public boolean isGreaterOrEqual(Version otherVersion) {
        return this.compareTo(otherVersion) >= 0;
    }

    public boolean isLessThan(Version otherVersion) {
        return this.compareTo(otherVersion) < 0;
    }

    public boolean isLessThanOrEqual(Version otherVersion) {
        return this.compareTo(otherVersion) <= 0;
    }

    public boolean isGreaterThan(Version otherVersion) {
        return this.compareTo(otherVersion) > 0;
    }

    public boolean isGreaterThan(int major, int minor, int patch, int build) {
        return this.isGreaterThan(new Version(major, minor, patch, build));
    }

    public boolean isGreaterOrEqual(int major, int minor, int patch, int build) {
        return this.compareTo(new Version(major, minor, patch, build)) >= 0;
    }

    public boolean isLessThan(int major, int minor, int patch, int build) {
        return this.compareTo(new Version(major, minor, patch, build)) < 0;
    }

	@Override
	public String toString() {
		return major + "." + minor + "." + patch + "." + build;
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
		return major == other.major && minor == other.minor && patch == other.patch;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + major;
		result = prime * result + minor;
		result = prime * result + patch;
		return result;
	}
}
