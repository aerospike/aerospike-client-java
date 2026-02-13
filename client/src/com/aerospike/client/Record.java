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
package com.aerospike.client;

import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;

import com.aerospike.client.Value.GeoJSONValue;
import com.aerospike.client.Value.HLLValue;

/**
 * Holds the bins (name/value pairs), generation, and expiration returned for a single record from the server.
 *
 * <p>Records are the result of get, query, and scan operations. Use the {@code get*} methods to read bin values
 * by name; each method returns a typed value or a default (e.g. 0 for numeric, false for boolean) if the bin is missing.
 *
 * <p><b>Example:</b>
 * <pre>{@code
 * Record rec = client.get(policy, key);
 * if (rec != null) {
 *     String name = rec.getString("name");
 *     long count = rec.getLong("count");
 * }
 * }</pre>
 *
 * @see AerospikeClient#get(com.aerospike.client.policy.Policy, Key)
 * @see #getValue(String)
 */
public final class Record {
	/**
	 * Map of bin names to values; may be {@code null} if no bins were requested or returned.
	 */
	public final Map<String,Object> bins;

	/**
	 * Record modification count (generation); incremented by the server on each write.
	 */
	public final int generation;

	/**
	 * Record expiration in seconds from Jan 01 2010 00:00:00 GMT; 0 means never expire.
	 */
	public final int expiration;

	/**
	 * Constructs a record with the given bins, generation, and expiration.
	 *
	 * @param bins map of bin names to values; may be {@code null}
	 * @param generation modification count from the server
	 * @param expiration expiration in seconds from Jan 01 2010 00:00:00 GMT; 0 for never expire
	 */
	public Record(
		Map<String,Object> bins,
		int generation,
		int expiration
	) {
		this.bins = bins;
		this.generation = generation;
		this.expiration = expiration;
	}

	/**
	 * Returns the bin value for the given bin name, or {@code null} if the bin is not present.
	 *
	 * @param name the bin name; must not be {@code null}
	 * @return the value, or {@code null} if the bin is missing or {@link #bins} is {@code null}
	 * @see #getString(String)
	 * @see #getLong(String)
	 */
	public Object getValue(String name) {
		return (bins == null)? null : bins.get(name);
	}

	/**
	 * Returns the bin value as a {@link String}, or {@code null} if the bin is missing or not a string.
	 *
	 * @param name the bin name; must not be {@code null}
	 * @return the string value, or {@code null}
	 */
	public String getString(String name) {
		return (String)getValue(name);
	}

	/**
	 * Returns the bin value as a byte array, or {@code null} if the bin is missing or not bytes.
	 *
	 * @param name the bin name; must not be {@code null}
	 * @return the byte array, or {@code null}
	 */
	public byte[] getBytes(String name) {
		return (byte[])getValue(name);
	}

	/**
	 * Returns the bin value as a {@code double}; returns 0.0 if the bin is missing or not numeric.
	 *
	 * @param name the bin name; must not be {@code null}
	 * @return the double value, or 0.0 if absent or not numeric
	 */
	public double getDouble(String name) {
		// The server may return number as double or long.
		// Convert bits if returned as long.
		Object result = getValue(name);
		return (result instanceof Double)? (Double)result : (result != null)? Double.longBitsToDouble((Long)result) : 0.0;
	}

	/**
	 * Returns the bin value as a {@code float}; equivalent to casting {@link #getDouble(String)}.
	 *
	 * @param name the bin name; must not be {@code null}
	 * @return the float value, or 0.0f if absent or not numeric
	 */
	public float getFloat(String name) {
		return (float)getDouble(name);
	}

	/**
	 * Returns the bin value as a {@code long}; returns 0 if the bin is missing or not numeric.
	 *
	 * @param name the bin name; must not be {@code null}
	 * @return the long value, or 0 if absent or not numeric
	 */
	public long getLong(String name) {
		// The server always returns numbers as longs if bin found.
		// If bin not found, the result will be null.  Convert null to zero.
		Object result = getValue(name);
		return (result != null)? (Long)result : 0;
	}

	/**
	 * Returns the bin value as an {@code int}; equivalent to casting {@link #getLong(String)}.
	 *
	 * @param name the bin name; must not be {@code null}
	 * @return the int value, or 0 if absent or not numeric
	 */
	public int getInt(String name) {
		// The server always returns numbers as longs, so get long and cast.
		return (int)getLong(name);
	}

	/**
	 * Returns the bin value as a {@code short}; equivalent to casting {@link #getLong(String)}.
	 *
	 * @param name the bin name; must not be {@code null}
	 * @return the short value, or 0 if absent or not numeric
	 */
	public short getShort(String name) {
		// The server always returns numbers as longs, so get long and cast.
		return (short)getLong(name);
	}

	/**
	 * Returns the bin value as a {@code byte}; equivalent to casting {@link #getLong(String)}.
	 *
	 * @param name the bin name; must not be {@code null}
	 * @return the byte value, or 0 if absent or not numeric
	 */
	public byte getByte(String name) {
		// The server always returns numbers as longs, so get long and cast.
		return (byte)getLong(name);
	}

	/**
	 * Returns the bin value as a {@code boolean}; returns {@code false} if the bin is missing or zero.
	 *
	 * <p>Accepts both server boolean type and legacy long (0/1) representation.
	 *
	 * @param name the bin name; must not be {@code null}
	 * @return the boolean value, or {@code false} if absent or zero
	 */
	public boolean getBoolean(String name) {
		// The server may return boolean as boolean or long (created by older clients).
		Object result = getValue(name);

		if (result instanceof Boolean) {
			return (Boolean)result;
		}

		if (result != null) {
			long v = (Long)result;
			return v != 0;
		}
		return false;
	}

	/**
	 * Returns the bin value as a list, or {@code null} if the bin is missing or not a list.
	 *
	 * @param name the bin name; must not be {@code null}
	 * @return the list, or {@code null}
	 */
	public List<?> getList(String name) {
		return (List<?>)getValue(name);
	}

	/**
	 * Returns the bin value as a map, or {@code null} if the bin is missing or not a map.
	 *
	 * @param name the bin name; must not be {@code null}
	 * @return the map, or {@code null}
	 */
	public Map<?,?> getMap(String name) {
		return (Map<?,?>)getValue(name);
	}

	/**
	 * Returns the value returned by a UDF execute when used in a batch operation.
	 *
	 * <p>This reads the special "SUCCESS" bin written by the server for UDF results.
	 *
	 * @return the UDF return value, or {@code null} if not present or not a UDF result record
	 * @see #getUDFError()
	 */
	public Object getUDFResult() {
		return getValue("SUCCESS");
	}

	/**
	 * Returns the error string returned by a UDF execute when the UDF failed in a batch operation.
	 *
	 * <p>This reads the special "FAILURE" bin written by the server when the UDF raises an error.
	 *
	 * @return the UDF error message, or {@code null} if no error occurred
	 * @see #getUDFResult()
	 */
	public String getUDFError() {
		return getString("FAILURE");
	}

	/**
	 * Returns the bin value as a GeoJSON string; retained for backward compatibility.
	 *
	 * @param name the bin name; must not be {@code null}
	 * @return the GeoJSON string, or {@code null}
	 * @deprecated Use {@link #getGeoJSONString(String)} instead
	 */
	@Deprecated
	public String getGeoJSON(String name) {
		return getGeoJSONString(name);
	}

	/**
	 * Returns the bin value as a GeoJSON string representation, or {@code null} if the bin is missing.
	 *
	 * @param name the bin name; must not be {@code null}
	 * @return the GeoJSON string, or {@code null}
	 */
	public String getGeoJSONString(String name) {
		Object value = getValue(name);
		return (value != null) ? value.toString() : null;
	}

	/**
	 * Returns the bin value as a {@link GeoJSONValue}, or {@code null} if the bin is missing or not GeoJSON.
	 *
	 * @param name the bin name; must not be {@code null}
	 * @return the GeoJSON value, or {@code null}
	 */
	public GeoJSONValue getGeoJSONValue(String name) {
		return (GeoJSONValue)getValue(name);
	}

	/**
	 * Returns the bin value as an {@link HLLValue}, or {@code null} if the bin is missing or not an HLL.
	 *
	 * @param name the bin name; must not be {@code null}
	 * @return the HLL value, or {@code null}
	 */
	public HLLValue getHLLValue(String name) {
		return (HLLValue)getValue(name);
	}

	/**
	 * Converts the record expiration (seconds from Jan 01 2010 00:00:00 GMT) to TTL in seconds from now.
	 *
	 * <p>Returns -1 if the record never expires (server expiration 0). Otherwise returns the remaining seconds
	 * until expiration, or at least 1 if already past expiration (to avoid legacy "never expire" interpretation).
	 *
	 * @return TTL in seconds from now, -1 for never expire, or ≥1 for remaining/expired
	 */
	public int getTimeToLive() {
		// This is the server's flag indicating the record never expires.
		if (expiration == 0) {
			// Convert to client-side convention for "never expires".
			return -1;
		}

		// Subtract epoch difference (1970/1/1 GMT to 2010/1/1 GMT) from current time.
		// Handle server's unsigned int ttl with java's usage of long for time.
		int now = (int)((System.currentTimeMillis() - 1262304000000L) / 1000);

		// Record may not have expired on server, but delay or clock differences may
		// cause it to look expired on client. Floor at 1, not 0, to avoid old
		// "never expires" interpretation.
		return ((expiration < 0 && now >= 0) || expiration > now) ? expiration - now : 1;
	}

	/**
	 * Returns a string representation of this record (generation, expiration, and bins).
	 *
	 * @return a string representation of this record
	 */
	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder(500);
		sb.append("(gen:");
		sb.append(generation);
		sb.append("),(exp:");
		sb.append(expiration);
		sb.append("),(bins:");

		if (bins != null) {
			boolean sep = false;

			for (Entry<String,Object> entry : bins.entrySet()) {
				if (sep) {
					sb.append(',');
				}
				else {
					sep = true;
				}

				if (sb.length() > 1000) {
					sb.append("...");
					break;
				}

				sb.append('(');
				sb.append(entry.getKey());
				sb.append(':');
				sb.append(entry.getValue());
				sb.append(')');
			}
		}
		else {
			sb.append("null");
		}
		sb.append(')');
		return sb.toString();
	}

	@Override
	public int hashCode() {
		return Objects.hash(bins, generation, expiration);
	}

	/**
	 * Compares this record to the specified object for equality (bins, generation, expiration).
	 *
	 * @param obj the object to compare to
	 * @return {@code true} if equal, {@code false} otherwise
	 */
	@Override
	public boolean equals(Object obj) {
		if (this == obj) {
			return true;
		}
		if (obj == null) {
			return false;
		}
		if (getClass() != obj.getClass()) {
			return false;
		}
		Record other = (Record) obj;
		if (expiration != other.expiration) {
			return false;
		}
		if (generation != other.generation) {
			return false;
		}
		if (bins == null) {
			return other.bins == null;
		} else {
			return bins.equals(other.bins);
		}
	}
}
