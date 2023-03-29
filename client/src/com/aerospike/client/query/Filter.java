/*
 * Copyright 2012-2022 Aerospike, Inc.
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

import java.util.Arrays;

import com.aerospike.client.Value;
import com.aerospike.client.cdt.CTX;
import com.aerospike.client.command.Buffer;
import com.aerospike.client.command.ParticleType;
import com.aerospike.client.util.Pack;

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
	 * @param ctx			optional context for elements within a CDT
	 * @return				filter instance
	 */
	public static Filter equal(String name, long value, CTX... ctx) {
		Value val = Value.get(value);
		return new Filter(name, IndexCollectionType.DEFAULT, val.getType(), val, val, ctx);
	}

	/**
	 * Create string equality filter for query.
	 *
	 * @param name			bin name
	 * @param value			filter value
	 * @param ctx			optional context for elements within a CDT
	 * @return				filter instance
	 */
	public static Filter equal(String name, String value, CTX... ctx) {
		Value val = Value.get(value);
		return new Filter(name, IndexCollectionType.DEFAULT, val.getType(), val, val, ctx);
	}

	/**
	 * Create contains number filter for query on collection index.
	 *
	 * @param name			bin name
	 * @param type			index collection type
	 * @param value			filter value
	 * @param ctx			optional context for elements within a CDT
	 * @return				filter instance
	 */
	public static Filter contains(String name, IndexCollectionType type, long value, CTX... ctx) {
		Value val = Value.get(value);
		return new Filter(name, type, val.getType(), val, val, ctx);
	}

	/**
	 * Create contains string filter for query on collection index.
	 *
	 * @param name			bin name
	 * @param type			index collection type
	 * @param value			filter value
	 * @param ctx			optional context for elements within a CDT
	 * @return				filter instance
	 */
	public static Filter contains(String name, IndexCollectionType type, String value, CTX... ctx) {
		Value val = Value.get(value);
		return new Filter(name, type, val.getType(), val, val, ctx);
	}

	/**
	 * Create range filter for query.
	 * Range arguments must be longs or integers which can be cast to longs.
	 * String ranges are not supported.
	 *
	 * @param name			bin name
	 * @param begin			filter begin value inclusive
	 * @param end			filter end value inclusive
	 * @param ctx			optional context for elements within a CDT
	 * @return				filter instance
	 */
	public static Filter range(String name, long begin, long end, CTX... ctx) {
		return new Filter(name, IndexCollectionType.DEFAULT, ParticleType.INTEGER, Value.get(begin), Value.get(end), ctx);
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
	 * @param ctx			optional context for elements within a CDT
	 * @return				filter instance
	 */
	public static Filter range(String name, IndexCollectionType type, long begin, long end, CTX... ctx) {
		return new Filter(name, type, ParticleType.INTEGER, Value.get(begin), Value.get(end), ctx);
	}

	/**
	 * Create geospatial "within region" filter for query.
	 *
	 * @param name			bin name
	 * @param region		GeoJSON region
	 * @param ctx			optional context for elements within a CDT
	 * @return				filter instance
	 */
	public static Filter geoWithinRegion(String name, String region, CTX... ctx) {
		return new Filter(name, IndexCollectionType.DEFAULT, ParticleType.GEOJSON, Value.get(region), Value.get(region), ctx);
	}

	/**
	 * Create geospatial "within region" filter for query on collection index.
	 *
	 * @param name			bin name
	 * @param type			index collection type
	 * @param region		GeoJSON region
	 * @param ctx			optional context for elements within a CDT
	 * @return				filter instance
	 */
	public static Filter geoWithinRegion(String name, IndexCollectionType type, String region, CTX... ctx) {
		return new Filter(name, type, ParticleType.GEOJSON, Value.get(region), Value.get(region), ctx);
	}

	/**
	 * Create geospatial "within radius" filter for query.
	 *
	 * @param name			bin name
	 * @param lng			longitude
	 * @param lat			latitude
	 * @param radius 		radius (meters)
	 * @param ctx			optional context for elements within a CDT
	 * @return				filter instance
	 */
	public static Filter geoWithinRadius(String name, double lng, double lat, double radius, CTX... ctx) {
		String rgnstr =
				String.format("{ \"type\": \"AeroCircle\", "
							  + "\"coordinates\": [[%.8f, %.8f], %f] }",
							  lng, lat, radius);
		return new Filter(name, IndexCollectionType.DEFAULT, ParticleType.GEOJSON, Value.get(rgnstr), Value.get(rgnstr), ctx);
	}

	/**
	 * Create geospatial "within radius" filter for query on collection index.
	 *
	 * @param name			bin name
	 * @param type			index collection type
	 * @param lng			longitude
	 * @param lat			latitude
	 * @param radius 		radius (meters)
	 * @param ctx			optional context for elements within a CDT
	 * @return				filter instance
	 */
	public static Filter geoWithinRadius(String name, IndexCollectionType type, double lng, double lat, double radius, CTX... ctx) {
		String rgnstr =
				String.format("{ \"type\": \"AeroCircle\", "
							  + "\"coordinates\": [[%.8f, %.8f], %f] }",
							  lng, lat, radius);
		return new Filter(name, type, ParticleType.GEOJSON, Value.get(rgnstr), Value.get(rgnstr), ctx);
	}

	/**
	 * Create geospatial "containing point" filter for query.
	 *
	 * @param name			bin name
	 * @param point			GeoJSON point
	 * @param ctx			optional context for elements within a CDT
	 * @return				filter instance
	 */
	public static Filter geoContains(String name, String point, CTX... ctx) {
		return new Filter(name, IndexCollectionType.DEFAULT, ParticleType.GEOJSON, Value.get(point), Value.get(point), ctx);
	}

	/**
	 * Create geospatial "containing point" filter for query on collection index.
	 *
	 * @param name			bin name
	 * @param type			index collection type
	 * @param point			GeoJSON point
	 * @param ctx			optional context for elements within a CDT
	 * @return				filter instance
	 */
	public static Filter geoContains(String name, IndexCollectionType type, String point, CTX... ctx) {
		return new Filter(name, type, ParticleType.GEOJSON, Value.get(point), Value.get(point), ctx);
	}

	private final String name;
	private final IndexCollectionType colType;
	private final byte[] packedCtx;
	private final int valType;
	private final Value begin;
	private final Value end;

	private Filter(String name, IndexCollectionType colType, int valType, Value begin, Value end, CTX[] ctx) {
		this(name, colType, valType, begin, end, (ctx != null && ctx.length > 0) ? Pack.pack(ctx) : null);
	}

	Filter(String name, IndexCollectionType colType, int valType, Value begin
		, Value end, byte[] packagedCtx) {
		this.name = name;
		this.colType = colType;
		this.valType = valType;
		this.begin = begin;
		this.end = end;
		this.packedCtx = packagedCtx;
	}

	/**
	 * Estimate filter's byte send when sending command to server.
	 * For internal use only.
	 */
	public int estimateSize() {
		// bin name size(1) + particle type size(1) + begin particle size(4) + end particle size(4) = 10
		return Buffer.estimateSizeUtf8(name) + begin.estimateSize() + end.estimateSize() + 10;
	}

	/**
	 * Write filter to send command buffer.
	 * For internal use only.
	 */
	public int write(byte[] buf, int offset) {
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
	 * Filter name.
	 * For internal use only.
	 */
	public String getName() {
		return name;
	}

	/**
	 * Index collection type.
	 * For internal use only.
	 */
	public IndexCollectionType getColType() {
		return colType;
	}

	/**
	 * Filter begin value.
	 * For internal use only.
	 */
	public Value getBegin() {
		return begin;
	}

	/**
	 * Filter begin value.
	 * For internal use only.
	 */
	public Value getEnd() {
		return end;
	}

	/**
	 * Filter Value type.
	 * For internal use only.
	 */
	public int getValType() {
		return valType;
	}

	/**
	 * Retrieve packed Context.
	 * For internal use only.
	 */
	public byte[] getPackedCtx() {
		return packedCtx;
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
		if (colType != other.colType)
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
		if (!Arrays.equals(packedCtx, other.packedCtx))
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
		result = prime * result + ((colType == null) ? 0 : colType.hashCode());
		result = prime * result + ((end == null) ? 0 : end.hashCode());
		result = prime * result + ((name == null) ? 0 : name.hashCode());
		result = prime * result + Arrays.hashCode(packedCtx);
		result = prime * result + valType;
		return result;
	}
}
