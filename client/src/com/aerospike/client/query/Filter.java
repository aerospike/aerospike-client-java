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
package com.aerospike.client.query;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.Value;
import com.aerospike.client.command.Buffer;
import com.aerospike.client.command.ParticleType;

/**
 * Query filter definition.
 *
 * Currently, only one filter is allowed in a Statement, and must be on bin which has a secondary index defined.
 */
public final class Filter {
	/**
	 * Create long equality filter for query.
	 *
	 * @param name			bin name
	 * @param value			filter value
	 * @return				filter instance
	 */
	public static Filter equal(String name, long value) {
		Value val = Value.get(value);
		return new Filter(name, IndexCollectionType.DEFAULT, val.getType(), val, val);
	}

	/**
	 * Create string equality filter for query.
	 *
	 * @param name			bin name
	 * @param value			filter value
	 * @return				filter instance
	 */
	public static Filter equal(String name, String value) {
		Value val = Value.get(value);
		return new Filter(name, IndexCollectionType.DEFAULT, val.getType(), val, val);
	}

	/**
	 * Create contains number filter for query on collection index.
	 *
	 * @param name			bin name
	 * @param type			index collection type
	 * @param value			filter value
	 * @return				filter instance
	 */
	public static Filter contains(String name, IndexCollectionType type, long value) {
		Value val = Value.get(value);
		return new Filter(name, type, val.getType(), val, val);
	}

	/**
	 * Create contains string filter for query on collection index.
	 *
	 * @param name			bin name
	 * @param type			index collection type
	 * @param value			filter value
	 * @return				filter instance
	 */
	public static Filter contains(String name, IndexCollectionType type, String value) {
		Value val = Value.get(value);
		return new Filter(name, type, val.getType(), val, val);
	}

	/**
	 * Create range filter for query.
	 * Range arguments must be longs or integers which can be cast to longs.
	 * String ranges are not supported.
	 *
	 * @param name			bin name
	 * @param begin			filter begin value inclusive
	 * @param end			filter end value inclusive
	 * @return				filter instance
	 */
	public static Filter range(String name, long begin, long end) {
		return new Filter(name, IndexCollectionType.DEFAULT, ParticleType.INTEGER, Value.get(begin), Value.get(end));
	}

	/**
	 * Create range filter for query on collection index.
	 * Range arguments must be longs or integers which can be cast to longs.
	 * String ranges are not supported.
	 *
	 * @param name			bin name
	 * @param type			index collection type
	 * @param begin			filter begin value inclusive
	 * @param end			filter end value inclusive
	 * @return				filter instance
	 */
	public static Filter range(String name, IndexCollectionType type, long begin, long end) {
		return new Filter(name, type, ParticleType.INTEGER, Value.get(begin), Value.get(end));
	}

	/**
	 * Create geospatial "within region" filter for query.
	 *
	 * @param name			bin name
	 * @param region		GeoJSON region
	 * @return				filter instance
	 */
	public static Filter geoWithinRegion(String name, String region) {
		return new Filter(name, IndexCollectionType.DEFAULT, ParticleType.GEOJSON, Value.get(region), Value.get(region));
	}

	/**
	 * Create geospatial "within region" filter for query on collection index.
	 *
	 * @param name			bin name
	 * @param type			index collection type
	 * @param region		GeoJSON region
	 * @return				filter instance
	 */
	public static Filter geoWithinRegion(String name, IndexCollectionType type, String region) {
		return new Filter(name, type, ParticleType.GEOJSON, Value.get(region), Value.get(region));
	}

	/**
	 * Create geospatial "within radius" filter for query.
	 *
	 * @param name			bin name
	 * @param lng			longitude
	 * @param lat			latitude
	 * @param radius 		radius (meters)
	 * @return				filter instance
	 */
	public static Filter geoWithinRadius(String name, double lng, double lat, double radius) {
		String rgnstr =
				String.format("{ \"type\": \"AeroCircle\", "
							  + "\"coordinates\": [[%.8f, %.8f], %f] }",
							  lng, lat, radius);
		return new Filter(name, IndexCollectionType.DEFAULT, ParticleType.GEOJSON, Value.get(rgnstr), Value.get(rgnstr));
	}

	/**
	 * Create geospatial "within radius" filter for query on collection index.
	 *
	 * @param name			bin name
	 * @param type			index collection type
	 * @param lng			longitude
	 * @param lat			latitude
	 * @param radius 		radius (meters)
	 * @return				filter instance
	 */
	public static Filter geoWithinRadius(String name, IndexCollectionType type, double lng, double lat, double radius) {
		String rgnstr =
				String.format("{ \"type\": \"AeroCircle\", "
							  + "\"coordinates\": [[%.8f, %.8f], %f] }",
							  lng, lat, radius);
		return new Filter(name, type, ParticleType.GEOJSON, Value.get(rgnstr), Value.get(rgnstr));
	}

	/**
	 * Create geospatial "containing point" filter for query.
	 *
	 * @param name			bin name
	 * @param point			GeoJSON point
	 * @return				filter instance
	 */
	public static Filter geoContains(String name, String point) {
		return new Filter(name, IndexCollectionType.DEFAULT, ParticleType.GEOJSON, Value.get(point), Value.get(point));
	}

	/**
	 * Create geospatial "containing point" filter for query on collection index.
	 *
	 * @param name			bin name
	 * @param type			index collection type
	 * @param point			GeoJSON point.
	 * @return				filter instance
	 */
	public static Filter geoContains(String name, IndexCollectionType type, String point) {
		return new Filter(name, type, ParticleType.GEOJSON, Value.get(point), Value.get(point));
	}

	private final String name;
	private final IndexCollectionType colType;
	private final int valType;
	private final Value begin;
	private final Value end;

	private Filter(String name, IndexCollectionType colType, int valType, Value begin, Value end) {
		this.name = name;
		this.valType = valType;
		this.colType = colType;
		this.begin = begin;
		this.end = end;
	}

	/**
	 * Estimate filter's byte send when sending command to server.
	 * For internal use only.
	 */
	public int estimateSize() throws AerospikeException {
		// bin name size(1) + particle type size(1) + begin particle size(4) + end particle size(4) = 10
		return Buffer.estimateSizeUtf8(name) + begin.estimateSize() + end.estimateSize() + 10;
	}

	/**
	 * Write filter to send command buffer.
	 * For internal use only.
	 */
	public int write(byte[] buf, int offset) throws AerospikeException {
		// Write name.
		int len = Buffer.stringToUtf8(name, buf, offset + 1);
		buf[offset] = (byte)len;
		offset += len + 1;

		// Write particle type.
		buf[offset++] = (byte)valType;

		// Write filter begin.
		len = begin.write(buf, offset + 4);
		Buffer.intToBytes(len, buf, offset);
		offset += len + 4;

		// Write filter end.
		len = end.write(buf, offset + 4);
		Buffer.intToBytes(len, buf, offset);
		offset += len + 4;

		return offset;
	}

	/**
	 * Retrieve index collection type.
	 * For internal use only.
	 */
	public IndexCollectionType getCollectionType() {
		return colType;
	}

	/**
	 * Check for Filter equality.
	 */
	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		Filter other = (Filter) obj;
		if (begin == null) {
			if (other.begin != null)
				return false;
		} else if (!begin.equals(other.begin))
			return false;
		if (end == null) {
			if (other.end != null)
				return false;
		} else if (!end.equals(other.end))
			return false;
		if (name == null) {
			if (other.name != null)
				return false;
		} else if (!name.equals(other.name))
			return false;
		if (colType != other.colType)
			return false;
		if (valType != other.valType)
			return false;
		return true;
	}

	/**
	 * Generate Filter hashCode.
	 */
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((begin == null) ? 0 : begin.hashCode());
		result = prime * result + ((end == null) ? 0 : end.hashCode());
		result = prime * result + ((name == null) ? 0 : name.hashCode());
		result = prime * result + ((colType == null) ? 0 : colType.hashCode());
		result = prime * result + valType;
		return result;
	}
}
