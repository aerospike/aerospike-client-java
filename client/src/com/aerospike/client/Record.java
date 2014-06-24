/* 
 * Copyright 2012-2014 Aerospike, Inc.
 *
 * Portions may be licensed to Aerospike, Inc. under one or more contributor
 * license agreements.
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

/**
 * Container object for records.  Records are equivalent to rows.
 */
public final class Record {
	/**
	 * Map of requested name/value bins.
	 */
	public final Map<String,Object> bins;
	
	/**
	 * List of all duplicate records (if any) for a given key.  Duplicates are only created when
	 * the server configuration option "allow-versions" is true (default is false) and client
	 * RecordExistsAction.DUPLICATE policy flag is set and there is a generation error.
	 * Almost always null.
	 */
	public final List<Map<String,Object>> duplicates;
	
	/**
	 * Record modification count.
	 */
	public final int generation;
	
	/**
	 * Date record will expire, in seconds from Jan 01 2010 00:00:00 GMT
	 */
	public final int expiration;

	/**
	 * Initialize record.
	 */
	public Record(
		Map<String,Object> bins,
		List<Map<String,Object>> duplicates,
		int generation,
		int expiration
	) {
		this.bins = bins;
		this.duplicates = duplicates;
		this.generation = generation;
		this.expiration = expiration;
	}
	
	/**
	 * Get bin value given bin name.
	 * Enter empty string ("") for servers configured as single-bin.
	 */
	public Object getValue(String name) {
		return (bins == null)? null : bins.get(name);
	}
	
	/**
	 * Return string representation of record.
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
				sb.append('(');
				sb.append(entry.getKey());
				sb.append(':');
				sb.append(entry.getValue());
				sb.append(')');
				
				if (sb.length() > 1000) {
					sb.append("...");
					break;
				}
			}
		}
		else {
			sb.append("null");
		}
		sb.append(')');
		return sb.toString();
	}
}
