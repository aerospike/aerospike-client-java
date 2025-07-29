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
package com.aerospike.client.query;

import java.util.Arrays;

import com.aerospike.client.Value;
import com.aerospike.client.cdt.CTX;
import com.aerospike.client.command.Buffer;
import com.aerospike.client.command.ParticleType;
import com.aerospike.client.exp.Expression;
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
	 * Create long equality filter for query with Expression.
	 *
	 * @param expression	the Expression
	 * @param value			filter value
	 * @return				filter instance
	 */
	public static Filter equal(Expression expression, long value) {
		Value val = Value.get(value);
		return new Filter(null, expression.getBytes(), IndexCollectionType.DEFAULT, val.getType(), val, val);
	}

	/**
	 * Create long equality filter for query with index name.
	 *
	 * @param indexName		the name that was assigned to the Secondary Index (SI)
	 * @param value			filter value
	 * @return				filter instance
	 */
	public static Filter equalByIndex(String indexName, long value) {
		Value val = Value.get(value);
		return new Filter(indexName, null, IndexCollectionType.DEFAULT, val.getType(), val, val);
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
	 * Create string equality filter for query with Expression.
	 *
	 * @param expression	the Expression
	 * @param value			filter value
	 * @return				filter instance
	 */
	public static Filter equal(Expression expression, String value) {
		Value val = Value.get(value);
		return new Filter(null, expression.getBytes(), IndexCollectionType.DEFAULT, val.getType(), val, val);
	}

	/**
	 * Create string equality filter for query with index name.
	 *
	 * @param indexName		the name that was assigned to the Secondary Index (SI)
	 * @param value			filter value
	 * @return				filter instance
	 */
	public static Filter equalByIndex(String indexName, String value) {
		Value val = Value.get(value);
		return new Filter(indexName, null, IndexCollectionType.DEFAULT, val.getType(), val, val);
	}

	/**
	 * Create blob equality filter for query.
	 * Requires server version 7.0+.
	 *
	 * @param name			bin name
	 * @param value			filter value
	 * @param ctx			optional context for elements within a CDT
	 * @return				filter instance
	 */
	public static Filter equal(String name, byte[] value, CTX... ctx) {
		Value val = Value.get(value);
		return new Filter(name, IndexCollectionType.DEFAULT, val.getType(), val, val, ctx);
	}

	/**
	 * Create blob equality filter for query with Expression.
	 *
	 * @param expression	the Expression
	 * @param value			filter value
	 * @return				filter instance
	 */
	public static Filter equal(Expression expression, byte[] value) {
		Value val = Value.get(value);
		return new Filter(null, expression.getBytes(), IndexCollectionType.DEFAULT, val.getType(), val, val);
	}

	/**
	 * Create blob equality filter for query with index name.
	 *
	 * @param indexName		the name that was assigned to the Secondary Index (SI)
	 * @param value			filter value
	 * @return				filter instance
	 */
	public static Filter equalByIndex(String indexName, byte[] value) {
		Value val = Value.get(value);
		return new Filter(indexName, null, IndexCollectionType.DEFAULT, val.getType(), val, val);
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
	 * Create contains number filter for query on collection index with Expression.
	 *
	 * @param expression	expression to use for the filter
	 * @param type			index collection type
	 * @param value			filter value
	 * @param ctx			optional context for elements within a CDT
	 * @return				filter instance
	 */
	public static Filter contains(Expression expression, IndexCollectionType type, long value) {
		Value val = Value.get(value);
		return new Filter(null, expression.getBytes(), type, val.getType(), val, val);
	}

	/**
	 * Create contains number filter for query on collection index with index name.
	 *
	 * @param indexName		index name
	 * @param type			index collection type
	 * @param value			filter value
	 * @param ctx			optional context for elements within a CDT
	 * @return				filter instance
	 */
	public static Filter containsByIndex(String indexName, IndexCollectionType type, long value) {
		Value val = Value.get(value);
		return new Filter(indexName, null, type, val.getType(), val, val);
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
	 * Create contains string filter for query on collection index with Expression.
	 *
	 * @param expression	expression to use for the filter
	 * @param type			index collection type
	 * @param value			filter value
	 * @param ctx			optional context for elements within a CDT
	 * @return				filter instance
	 */
	public static Filter contains(Expression expression, IndexCollectionType type, String value) {
		Value val = Value.get(value);
		return new Filter(null, expression.getBytes(), type, val.getType(), val, val);
	}

	/**
	 * Create contains string filter for query on collection index with index name.
	 *
	 * @param indexName		index name
	 * @param type			index collection type
	 * @param value			filter value
	 * @param ctx			optional context for elements within a CDT
	 * @return				filter instance
	 */
	public static Filter containsByIndex(String indexName, IndexCollectionType type, String value) {
		Value val = Value.get(value);
		return new Filter(indexName, null, type, val.getType(), val, val);
	}

	/**
	 * Create contains byte[] filter for query on collection index.
	 *
	 * @param name			bin name
	 * @param type			index collection type
	 * @param value			filter value
	 * @param ctx			optional context for elements within a CDT
	 * @return				filter instance
	 */
	public static Filter contains(String name, IndexCollectionType type, byte[] value, CTX... ctx) {
		Value val = Value.get(value);
		return new Filter(name, type, val.getType(), val, val, ctx);
	}

	/**
	 * Create contains byte[] filter for query on collection index with expression.
	 *
	 * @param expression	expression to use for the filter
	 * @param type			index collection type
	 * @param value			filter value
	 * @param ctx			optional context for elements within a CDT
	 * @return				filter instance
	 */
	public static Filter contains(Expression expression, IndexCollectionType type, byte[] value) {
		Value val = Value.get(value);
		return new Filter(null, expression.getBytes(), type, val.getType(), val, val);
	}

	/**
	 * Create contains byte[] filter for query on collection index with index name.
	 *
	 * @param indexName		index name
	 * @param type			index collection type
	 * @param value			filter value
	 * @param ctx			optional context for elements within a CDT
	 * @return				filter instance
	 */
	public static Filter containsByIndex(String indexName, IndexCollectionType type, byte[] value) {
		Value val = Value.get(value);
		return new Filter(indexName, null, type, val.getType(), val, val);
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
	 * Create a range filter for an Expression-based Secondary Index (SI) query, with the actual Expression that was
	 * used to create the SI.
	 * Range arguments must be longs or integers which can be cast to longs.
	 * String ranges are not supported.
	 *
	 * @param exp			the Expression
	 * @param begin			filter begin value inclusive
	 * @param end			filter end value inclusive
	 * @return				filter instance
	 */
	public static Filter range(Expression exp, long begin, long end) {
		return new Filter(null, exp.getBytes(), IndexCollectionType.DEFAULT, ParticleType.INTEGER,
				Value.get(begin), Value.get(end));
	}

	/**
	 * Create a range filter for an Expression-based Secondary Index (SI) query, using the SI name
	 * Range arguments must be longs or integers which can be cast to longs.
	 * String ranges are not supported.
	 *
	 * @param indexName	    the name that was assigned to the SI
	 * @param begin			filter begin value inclusive
	 * @param end			filter end value inclusive
	 * @return				filter instance
	 */
	public static Filter rangeByIndex(String indexName, long begin, long end) {
		return new Filter(indexName, null, IndexCollectionType.DEFAULT, ParticleType.INTEGER,
				Value.get(begin), Value.get(end));
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
	 * Create range filter for query on collection index with expression.
	 * Range arguments must be longs or integers which can be cast to longs.
	 * String ranges are not supported.
	 *
	 * @param expression	the Expression to use for the filter
	 * @param type			index collection type
	 * @param begin			filter begin value inclusive
	 * @param end			filter end value inclusive
	 * @param ctx			optional context for elements within a CDT
	 * @return				filter instance
	 */
	public static Filter range(Expression exp, IndexCollectionType type, long begin, long end) {
		return new Filter(null, exp.getBytes(), type, ParticleType.INTEGER, Value.get(begin), Value.get(end));
	}

	/**
	 * Create range filter for query on collection index with index name.
	 * Range arguments must be longs or integers which can be cast to longs.
	 * String ranges are not supported.
	 *
	 * @param indexName		index name
	 * @param type			index collection type
	 * @param begin			filter begin value inclusive
	 * @param end			filter end value inclusive
	 * @param ctx			optional context for elements within a CDT
	 * @return				filter instance
	 */
	public static Filter rangeByIndex(String indexName, IndexCollectionType type, long begin, long end) {
		return new Filter(indexName, null, type, ParticleType.INTEGER, Value.get(begin), Value.get(end));
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
	 * Create geospatial "within region" filter for query with expression.
	 *
	 * @param expression	the Expression to use for the filter
	 * @param region		GeoJSON region
	 * @param ctx			optional context for elements within a CDT
	 * @return				filter instance
	 */
	public static Filter geoWithinRegion(Expression expression, String region) {
		return new Filter(null, expression.getBytes(), IndexCollectionType.DEFAULT, ParticleType.GEOJSON, Value.get(region), Value.get(region));
	}

	/**
	 * Create geospatial "within region" filter for query with index name.
	 *
	 * @param indexName	    index name
	 * @param region		GeoJSON region
	 * @param ctx			optional context for elements within a CDT
	 * @return				filter instance
	 */
	public static Filter geoWithinRegionByIndex(String indexName, String region) {
		return new Filter(indexName, null, IndexCollectionType.DEFAULT, ParticleType.GEOJSON, Value.get(region), Value.get(region));
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
	 * Create geospatial "within region" filter for query on collection index with expression.
	 *
	 * @param expression	the Expression to use for the filter
	 * @param type			index collection type
	 * @param region		GeoJSON region
	 * @param ctx			optional context for elements within a CDT
	 * @return				filter instance
	 */
	public static Filter geoWithinRegion(Expression expression, IndexCollectionType type, String region) {
		return new Filter(null, expression.getBytes(), type, ParticleType.GEOJSON, Value.get(region), Value.get(region));
	}

	/**
	 * Create geospatial "within region" filter for query on collection index with index name.
	 *
	 * @param indexName	    index name
	 * @param type			index collection type
	 * @param region		GeoJSON region
	 * @param ctx			optional context for elements within a CDT
	 * @return				filter instance
	 */
	public static Filter geoWithinRegionByIndex(String indexName, IndexCollectionType type, String region) {
		return new Filter(indexName, null, type, ParticleType.GEOJSON, Value.get(region), Value.get(region));
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
	 * Create geospatial "within radius" filter for query with expression.
	 *
	 * @param expression	the Expression to use for the filter
	 * @param lng			longitude
	 * @param lat			latitude
	 * @param radius 		radius (meters)
	 * @param ctx			optional context for elements within a CDT
	 * @return				filter instance
	 */
	public static Filter geoWithinRadius(Expression expression, double lng, double lat, double radius) {
		String rgnstr =
				String.format("{ \"type\": \"AeroCircle\", "
							  + "\"coordinates\": [[%.8f, %.8f], %f] }",
							  lng, lat, radius);
		return new Filter(null, expression.getBytes(), IndexCollectionType.DEFAULT, ParticleType.GEOJSON, Value.get(rgnstr), Value.get(rgnstr));
	}

	/**
	 * Create geospatial "within radius" filter for query with index name.
	 *
	 * @param indexName		index name
	 * @param lng			longitude
	 * @param lat			latitude
	 * @param radius 		radius (meters)
	 * @param ctx			optional context for elements within a CDT
	 * @return				filter instance
	 */
	public static Filter geoWithinRadiusByIndex(String indexName, double lng, double lat, double radius) {
		String rgnstr =
				String.format("{ \"type\": \"AeroCircle\", "
							  + "\"coordinates\": [[%.8f, %.8f], %f] }",
							  lng, lat, radius);
		return new Filter(indexName, null, IndexCollectionType.DEFAULT, ParticleType.GEOJSON, Value.get(rgnstr), Value.get(rgnstr));
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
	 * Create geospatial "within radius" filter for query on collection index with expression.
	 *
	 * @param expression	the Expression to use for the filter
	 * @param type			index collection type
	 * @param lng			longitude
	 * @param lat			latitude
	 * @param radius 		radius (meters)
	 * @param ctx			optional context for elements within a CDT
	 * @return				filter instance
	 */
	public static Filter geoWithinRadius(Expression expression, IndexCollectionType type, double lng, double lat, double radius) {
		String rgnstr =
				String.format("{ \"type\": \"AeroCircle\", "
							  + "\"coordinates\": [[%.8f, %.8f], %f] }",
							  lng, lat, radius);
		return new Filter(null, expression.getBytes(), type, ParticleType.GEOJSON, Value.get(rgnstr), Value.get(rgnstr));
	}

	/**
	 * Create geospatial "within radius" filter for query on collection index with index name.
	 *
	 * @param indexName		index name
	 * @param type			index collection type
	 * @param lng			longitude
	 * @param lat			latitude
	 * @param radius 		radius (meters)
	 * @param ctx			optional context for elements within a CDT
	 * @return				filter instance
	 */
	public static Filter geoWithinRadiusByIndex(String indexName, IndexCollectionType type, double lng, double lat, double radius) {
		String rgnstr =
				String.format("{ \"type\": \"AeroCircle\", "
							  + "\"coordinates\": [[%.8f, %.8f], %f] }",
							  lng, lat, radius);
		return new Filter(indexName, null, type, ParticleType.GEOJSON, Value.get(rgnstr), Value.get(rgnstr));
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
	 * Create geospatial "containing point" filter for query with expression.
	 *
	 * @param expression	the Expression to use for the filter
	 * @param point			GeoJSON point
	 * @param ctx			optional context for elements within a CDT
	 * @return				filter instance
	 */
	public static Filter geoContains(Expression expression, String point) {
		return new Filter(null, expression.getBytes(), IndexCollectionType.DEFAULT, ParticleType.GEOJSON, Value.get(point), Value.get(point));
	}

	/**
	 * Create geospatial "containing point" filter for query with index name.
	 *
	 * @param indexName	    index name
	 * @param point			GeoJSON point
	 * @param ctx			optional context for elements within a CDT
	 * @return				filter instance
	 */
	public static Filter geoContainsByIndex(String indexName, String point) {
		return new Filter(indexName, null, IndexCollectionType.DEFAULT, ParticleType.GEOJSON, Value.get(point), Value.get(point));
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

	/**
	 * Create geospatial "containing point" filter for query on collection index with expression.
	 *
	 * @param expression	the Expression to use for the filter
	 * @param type			index collection type
	 * @param point			GeoJSON point
	 * @param ctx			optional context for elements within a CDT
	 * @return				filter instance
	 */
	public static Filter geoContains(Expression expression, IndexCollectionType type, String point) {
		return new Filter(null, expression.getBytes(), type, ParticleType.GEOJSON, Value.get(point), Value.get(point));
	}

	/**
	 * Create geospatial "containing point" filter for query on collection index with index name.
	 *
	 * @param indexName	    index name
	 * @param type			index collection type
	 * @param point			GeoJSON point
	 * @param ctx			optional context for elements within a CDT
	 * @return				filter instance
	 */
	public static Filter geoContainsByIndex(String indexName, IndexCollectionType type, String point) {
		return new Filter(indexName, null, type, ParticleType.GEOJSON, Value.get(point), Value.get(point));
	}

	private final String name;
	private final String indexName;
	private final IndexCollectionType colType;
	private final byte[] packedCtx;
	private final int valType;
	private final Value begin;
	private final Value end;
	private final byte[] packedExp;

	private Filter(String name, IndexCollectionType colType, int valType, Value begin, Value end, CTX[] ctx) {
		this(name, null, colType, valType, begin, end, (ctx != null && ctx.length > 0) ? Pack.pack(ctx) : null, null);
	}
	private Filter(String indexName, byte[] exp, IndexCollectionType colType, int valType, Value begin, Value end) {
		this(null, indexName, colType, valType, begin, end, null, exp);
	}

	Filter(String name, String indexName, IndexCollectionType colType, int valType, Value begin, Value end,
		   byte[] packedCtx, byte[] packedExp) {
		this.name = name;
		this.indexName = indexName;
		this.colType = colType;
		this.valType = valType;
		this.begin = begin;
		this.end = end;
		this.packedCtx = packedCtx;
		this.packedExp = packedExp;
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
	 * Index name.
	 * For internal use only.
	 */
	public String getIndexName() {
		return indexName;
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
	 * Retrieve packed Expression.
	 * For internal use only.
	 */
	public byte[] getPackedExp() {
		return packedExp;
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
