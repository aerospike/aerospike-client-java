/*
 * Copyright 2012-2017 Aerospike, Inc.
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

import java.util.Calendar;

import com.aerospike.client.command.Buffer;

/**
 * Predicate expression filter.
 * Predicate expression filters are applied on the query results on the server.
 * Predicate expression filters may occur on any bin in the record.
 */
public abstract class PredExp {
	/**
	 * Create "and" expression.
	 * 
	 * @param nexp	number of expressions to perform "and" operation.  Usually two.
	 */
	public static PredExp and(int nexp) {
		return new AndOr(AND, nexp);
	}
	
	/**
	 * Create "or" expression.
	 * 
	 * @param nexp	number of expressions to perform "or" operation.  Usually two.
	 */
	public static PredExp or(int nexp) {
		return new AndOr(OR, nexp);
	}
	
	/**
	 * Create "not" expression.
	 */
	public static PredExp not() {
		return new Op(NOT);
	}

	/**
	 * Create Calendar value expressed in nanoseconds since 1970-01-01 epoch as 64 bit integer.
	 */
	public static PredExp integerValue(Calendar val) {
		return new IntegerValue(val.getTimeInMillis() * NANOS_PER_MILLIS, INTEGER_VALUE);
	}

	/**
	 * Create 64 bit integer value.
	 */
	public static PredExp integerValue(long val) {
		return new IntegerValue(val, INTEGER_VALUE);
	}
	
	/**
	 * Create string value.
	 */	
	public static PredExp stringValue(String val) {
		return new StringValue(val, STRING_VALUE);
	}

	/**
	 * Create geospatial json string value.
	 */	
	public static PredExp geoJSONValue(String val) {
		return new StringValue(val, GEOJSON_VALUE);
	}

	/**
	 * Create 64 bit integer bin predicate.
	 */	
	public static PredExp integerBin(String name) {
		return new StringValue(name, INTEGER_BIN);
	}

	/**
	 * Create string bin predicate.
	 */	
	public static PredExp stringBin(String name) {
		return new StringValue(name, STRING_BIN);
	}

	/**
	 * Create geospatial bin predicate.
	 */	
	public static PredExp geoJSONBin(String name) {
		return new StringValue(name, GEOJSON_BIN);
	}

	/**
	 * Create list bin predicate.
	 */	
	public static PredExp listBin(String name) {
		return new StringValue(name, LIST_BIN);
	}

	/**
	 * Create map bin predicate.
	 */	
	public static PredExp mapBin(String name) {
		return new StringValue(name, MAP_BIN);
	}

	/**
	 * Create 64 bit integer variable used in list/map iterations.
	 */
	public static PredExp integerVar(String name) {
		return new StringValue(name, INTEGER_VAR);
	}
	
	/**
	 * Create string variable used in list/map iterations.
	 */	
	public static PredExp stringVar(String name) {
		return new StringValue(name, STRING_VAR);
	}

	/**
	 * Create geospatial json string variable used in list/map iterations.
	 */	
	public static PredExp geoJSONVar(String name) {
		return new StringValue(name, GEOJSON_VAR);
	}

	/**
	 * Create record size predicate.
	 */	
	public static PredExp recSize() {
		return new Op(RECSIZE);
	}
	
	/**
	 * Create record last update time predicate expressed in nanoseconds since 1970-01-01 epoch as 64 bit integer.
	 * Example:
	 * <pre>
	 * // Record last update time >= 2017-01-15
	 * PredExp.integerValue(new GregorianCalendar(2017, 0, 15))
	 * PredExp.lastUpdate()
	 * PredExp.integerGreaterEq()
     * </pre>
	 */	
	public static PredExp lastUpdate() {
		return new Op(LAST_UPDATE);	
	}

	/**
	 * Create record expiration time predicate expressed in nanoseconds since 1970-01-01 epoch as 64 bit integer.
	 * Example:
	 * <pre>
	 * // Record expires on 2020-01-01
	 * PredExp.integerValue(new GregorianCalendar(2020, 0, 1))
	 * PredExp.voidTime()
	 * PredExp.integerGreaterEq()
	 * PredExp.integerValue(new GregorianCalendar(2020, 0, 2))
	 * PredExp.voidTime()
	 * PredExp.integerLess()
	 * PredExp.and(2)
     * </pre>
	 */	
	public static PredExp voidTime() {
		return new Op(VOID_TIME);	
	}

	/**
	 * Create 64 bit integer "=" operation predicate.
	 */	
	public static PredExp integerEqual() {
		return new Op(INTEGER_EQUAL);
	}

	/**
	 * Create 64 bit integer "!=" operation predicate.
	 */	
	public static PredExp integerUnequal() {
		return new Op(INTEGER_UNEQUAL);
	}

	/**
	 * Create 64 bit integer ">" operation predicate.
	 */	
	public static PredExp integerGreater() {
		return new Op(INTEGER_GREATER);
	}

	/**
	 * Create 64 bit integer ">=" operation predicate.
	 */	
	public static PredExp integerGreaterEq() {
		return new Op(INTEGER_GREATEREQ);
	}

	/**
	 * Create 64 bit integer "<" operation predicate.
	 */	
	public static PredExp integerLess() {
		return new Op(INTEGER_LESS);
	}

	/**
	 * Create 64 bit integer "<=" operation predicate.
	 */	
	public static PredExp integerLessEq() {
		return new Op(INTEGER_LESSEQ);
	}

	/**
	 * Create string "=" operation predicate.
	 */	
	public static PredExp stringEqual() {
		return new Op(STRING_EQUAL);
	}

	/**
	 * Create string "!=" operation predicate.
	 */	
	public static PredExp stringUnequal() {
		return new Op(STRING_UNEQUAL);
	}

	/**
	 * Create regular expression string operation predicate.  Example:
	 * <pre>
	 * PredExp.stringRegex(RegexFlag.EXTENDED | RegexFlag.ICASE)
	 * </pre>
	 * 
	 * @param flags	regular expression bit flags. See {@link com.aerospike.client.query.RegexFlag}
	 */	
	public static PredExp stringRegex(int flags) {
		return new Regex(STRING_REGEX, flags);
	}

	/**
	 * Create geospatial json "within" predicate.
	 */
	public static PredExp geoJSONWithin() {
		return new Op(GEOJSON_WITHIN);
	}

	/**
	 * Create geospatial json "contains" predicate.
	 */
	public static PredExp geoJSONContains() {
		return new Op(GEOJSON_CONTAINS);
	}
	
	/**
	 * Create list predicate where expression matches for any list item.
	 * Example:
	 * <pre>
	 * // Find records where any list item v = "hello" in list bin x.  
	 * PredExp.stringValue("hello")
	 * PredExp.stringVar("v")
	 * PredExp.stringEqual()
	 * PredExp.listBin("x")
	 * PredExp.listIterateOr("v")
	 * </pre>
	 */
	public static PredExp listIterateOr(String varName) {
		return new StringValue(varName, LIST_ITERATE_OR);
	}

	/**
	 * Create list predicate where expression matches for all list items.
	 * Example:
	 * <pre>
	 * // Find records where all list elements v != "goodbye" in list bin x.  
	 * PredExp.stringValue("goodbye")
	 * PredExp.stringVar("v")
	 * PredExp.stringUnequal()
	 * PredExp.listBin("x")
	 * PredExp.listIterateAnd("v")
	 * </pre>
	 */
	public static PredExp listIterateAnd(String varName) {
		return new StringValue(varName, LIST_ITERATE_AND);
	}

	/**
	 * Create map predicate where expression matches for any map key.
	 * Example:
	 * <pre>
	 * // Find records where any map key k = 7 in map bin m.  
	 * PredExp.integerValue(7)
	 * PredExp.integerVar("k")
	 * PredExp.integerEqual()
	 * PredExp.mapBin("m")
	 * PredExp.mapKeyIterateOr("k")
	 * </pre>
	 */
	public static PredExp mapKeyIterateOr(String varName) {
		return new StringValue(varName, MAPKEY_ITERATE_OR);
	}

	/**
	 * Create map key predicate where expression matches for all map keys.
	 * Example:
	 * <pre>
	 * // Find records where all map keys k < 5 in map bin m.  
	 * PredExp.integerValue(5)
	 * PredExp.integerVar("k")
	 * PredExp.integerLess()
	 * PredExp.mapBin("m")
	 * PredExp.mapKeyIterateAnd("k")
	 * </pre>
	 */
	public static PredExp mapKeyIterateAnd(String varName) {
		return new StringValue(varName, MAPKEY_ITERATE_AND);
	}

	/**
	 * Create map predicate where expression matches for any map value.
	 * <pre>
	 * // Find records where any map value v > 100 in map bin m.  
	 * PredExp.integerValue(100)
	 * PredExp.integerVar("v")
	 * PredExp.integerGreater()
	 * PredExp.mapBin("m")
	 * PredExp.mapValIterateOr("v")
	 * </pre>
	 */
	public static PredExp mapValIterateOr(String varName) {
		return new StringValue(varName, MAPVAL_ITERATE_OR);
	}

	/**
	 * Create map predicate where expression matches for all map values.
	 * Example:
	 * <pre>
	 * // Find records where all map values k > 500 in map bin m.  
	 * PredExp.integerValue(500)
	 * PredExp.integerVar("v")
	 * PredExp.integerGreater()
	 * PredExp.mapBin("m")
	 * PredExp.mapKeyIterateAnd("v")
	 * </pre>
	 */
	public static PredExp mapValIterateAnd(String varName) {
		return new StringValue(varName, MAPVAL_ITERATE_AND);
	}

	private static final int AND = 1;
	private static final int OR = 2;
	private static final int NOT = 3;
	private static final int INTEGER_VALUE = 10;
	private static final int STRING_VALUE = 11;
	private static final int GEOJSON_VALUE = 12;
	private static final int INTEGER_BIN = 100;
	private static final int STRING_BIN = 101;
	private static final int GEOJSON_BIN = 102;
	private static final int LIST_BIN = 103;
	private static final int MAP_BIN = 104;
	private static final int INTEGER_VAR = 120;
	private static final int STRING_VAR = 121;
	private static final int GEOJSON_VAR = 122;	
	private static final int RECSIZE = 150;
	private static final int LAST_UPDATE = 151;
	private static final int VOID_TIME = 152;
	private static final int INTEGER_EQUAL = 200;
	private static final int INTEGER_UNEQUAL = 201;
	private static final int INTEGER_GREATER = 202;
	private static final int INTEGER_GREATEREQ = 203;
	private static final int INTEGER_LESS = 204;
	private static final int INTEGER_LESSEQ = 205;
	private static final int STRING_EQUAL = 210;
	private static final int STRING_UNEQUAL = 211;
	private static final int STRING_REGEX = 212;
	private static final int GEOJSON_WITHIN = 220;
	private static final int GEOJSON_CONTAINS = 221;
	private static final int LIST_ITERATE_OR = 250;
	private static final int MAPKEY_ITERATE_OR = 251;
	private static final int MAPVAL_ITERATE_OR = 252;
	private static final int LIST_ITERATE_AND = 253;
	private static final int MAPKEY_ITERATE_AND = 254;
	private static final int MAPVAL_ITERATE_AND = 255;
	
	private static final long NANOS_PER_MILLIS = 1000000L;

	/**
	 * Estimate size of predicate expressions.
	 * For internal use only.
	 */
	public static int estimateSize(PredExp[] predExp) {
		int size = 0;
		
		for (PredExp pred : predExp) {
			size += pred.estimateSize();
		}
		return size;
	}

	/**
	 * Write predicate expressions to write protocol.
	 * For internal use only.
	 */
	public static int write(PredExp[] predExp, byte[] buf, int offset) {
		for (PredExp pred : predExp) {
			offset = pred.write(buf, offset);
		}
		return offset;
	}
	
	/**
	 * Estimate size of predicate expression.
	 * For internal use only.
	 */
	public abstract int estimateSize();
	
	/**
	 * Write predicate expression to write protocol.
	 * For internal use only.
	 */
	public abstract int write(byte[] buf, int offset);

	private static class IntegerValue extends PredExp {
		private final long value;
		private final int type;
		
		private IntegerValue(long value, int type) {
			this.value = value;
			this.type = type;
		}
		
		public int estimateSize() {
			return 14;
		}

		public int write(byte[] buf, int offset) {
			// Write value type
			Buffer.shortToBytes(type, buf, offset);
			offset += 2;
			
			// Write length
			Buffer.intToBytes(8, buf, offset);
			offset += 4;
			
			// Write value
			Buffer.longToBytes(value, buf, offset);
			offset += 8;
			return offset;
		}
	}
	
	private static class StringValue extends PredExp {
		private final String value;
		private final int type;
		
		public StringValue(String value, int type) {
			this.value = value;
			this.type = type;
		}

		public int estimateSize() {
			 return Buffer.estimateSizeUtf8(value) + 6;
		}

		public int write(byte[] buf, int offset) {
			// Write value type
			Buffer.shortToBytes(type, buf, offset);
			offset += 2;
			
			// Write value
			int len = Buffer.stringToUtf8(value, buf, offset + 4);
			Buffer.intToBytes(len, buf, offset);
			offset += 4 + len;			
			return offset;
		}
	}
	
	private static class AndOr extends PredExp {
		private final int op;
		private final int nexp;
		
		private AndOr(int op, int nexp) {
			this.op = op;
			this.nexp = nexp;
		}
		
		public int estimateSize() {
			 return 8;
		}

		public int write(byte[] buf, int offset) {
			// Write type
			Buffer.shortToBytes(op, buf, offset);
			offset += 2;
			
			// Write length
			Buffer.intToBytes(2, buf, offset);
			offset += 4;		

			// Write predicate count
			Buffer.shortToBytes(nexp, buf, offset);
			offset += 2;
			return offset;
		}
	}

	private static class Op extends PredExp {
		private final int op;
		
		private Op(int op) {
			this.op = op;
		}

		public int estimateSize() {
			 return 6;
		}

		public int write(byte[] buf, int offset) {
			// Write op type
			Buffer.shortToBytes(op, buf, offset);
			offset += 2;
			
			// Write zero length
			Buffer.intToBytes(0, buf, offset);
			offset += 4;		
			return offset;
		}
	}

	private static class Regex extends PredExp {
		private final int op;
		private final int cflags;
		
		private Regex(int op, int cflags) {
			this.op = op;
			this.cflags = cflags; 
		}
		
		public int estimateSize() {
			 return 10;
		}

		public int write(byte[] buf, int offset) {
			// Write op type
			Buffer.shortToBytes(op, buf, offset);
			offset += 2;
			
			// Write length
			Buffer.intToBytes(4, buf, offset);
			offset += 4;		

			// Write predicate count
			Buffer.intToBytes(cflags, buf, offset);
			offset += 4;
			return offset;
		}
	}	
}
