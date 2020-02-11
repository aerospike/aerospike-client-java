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

import java.io.Serializable;
import java.util.Calendar;

import com.aerospike.client.command.Buffer;

/**
 * Predicate expression filter.
 * Predicate expression filters are applied on the query results on the server.
 * Predicate expression filters may occur on any bin in the record.
 */
public abstract class PredExp implements Serializable {
	private static final long serialVersionUID = 8867524802639112680L;

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
		return new GeoJSONValue(val, GEOJSON_VALUE);
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
	 * Create record size on disk predicate.
	 */
	public static PredExp recDeviceSize() {
		return new Op(RECSIZE);
	}

	/**
	 * Create record last update time predicate expressed in nanoseconds since 1970-01-01 epoch as 64 bit integer.
	 * Example:
	 * <pre>
	 * // Record last update time >= 2017-01-15
	 * PredExp.recLastUpdate()
	 * PredExp.integerValue(new GregorianCalendar(2017, 0, 15))
	 * PredExp.integerGreaterEq()
     * </pre>
	 */
	public static PredExp recLastUpdate() {
		return new Op(LAST_UPDATE);
	}

	/**
	 * Create record expiration time predicate expressed in nanoseconds since 1970-01-01 epoch as 64 bit integer.
	 * Example:
	 * <pre>
	 * // Record expires on 2020-01-01
	 * PredExp.recVoidTime()
	 * PredExp.integerValue(new GregorianCalendar(2020, 0, 1))
	 * PredExp.integerGreaterEq()
	 * PredExp.recVoidTime()
	 * PredExp.integerValue(new GregorianCalendar(2020, 0, 2))
	 * PredExp.integerLess()
	 * PredExp.and(2)
     * </pre>
	 */
	public static PredExp recVoidTime() {
		return new Op(VOID_TIME);
	}

	/**
	 * Create a digest modulo record metadata value predicate expression.
	 * The digest modulo expression assumes the value of 4 bytes of the
	 * record's key digest modulo it's argument.
	 * <p>
	 * For example, the following sequence of predicate expressions
	 * selects records that have digest(key) % 3 == 1):
	 * <pre>
	 * PredExp.recDigestModulo(3)
	 * PredExp.integerValue(1)
	 * PredExp.integerEqual()
	 * </pre>
	 */
	public static PredExp recDigestModulo(int mod) {
		return new OpInt(DIGEST_MODULO, mod);
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
		return new OpInt(STRING_REGEX, flags);
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
	 * PredExp.stringVar("v")
	 * PredExp.stringValue("hello")
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
	 * PredExp.stringVar("v")
	 * PredExp.stringValue("goodbye")
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
	 * PredExp.integerVar("k")
	 * PredExp.integerValue(7)
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
	 * PredExp.integerVar("k")
	 * PredExp.integerValue(5)
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
	 * PredExp.integerVar("v")
	 * PredExp.integerValue(100)
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
	 * // Find records where all map values v > 500 in map bin m.
	 * PredExp.integerVar("v")
	 * PredExp.integerValue(500)
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
	private static final int DIGEST_MODULO = 153;
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

	private static final String operationToString(int operation) {
		switch (operation) {
			case AND: return "AND";
			case OR: return "OR";
			case NOT: return "NOT";
			case INTEGER_VALUE: return "INTEGER_VALUE";
			case STRING_VALUE: return "STRING_VALUE";
			case GEOJSON_VALUE: return "GEOJSON_VALUE";
			case INTEGER_BIN: return "INTEGER_BIN";
			case STRING_BIN: return "STRING_BIN";
			case GEOJSON_BIN: return "GEOJSON_BIN";
			case LIST_BIN: return "LIST_BIN";
			case MAP_BIN: return "MAP_BIN";
			case INTEGER_VAR: return "INTEGER_VAR";
			case STRING_VAR: return "STRING_VAR";
			case GEOJSON_VAR: return "GEOJSON_VAR";
			case RECSIZE: return "RECSIZE";
			case LAST_UPDATE: return "LAST_UPDATE";
			case VOID_TIME: return "VOID_TIME";
			case DIGEST_MODULO: return "DIGEST_MODULO";
			case INTEGER_EQUAL: return "INTEGER_EQUAL";
			case INTEGER_UNEQUAL: return "INTEGER_UNEQUAL";
			case INTEGER_GREATER: return "INTEGER_GREATER";
			case INTEGER_GREATEREQ: return "INTEGER_GREATEREQ";
			case INTEGER_LESS: return "INTEGER_LESS";
			case INTEGER_LESSEQ: return "INTEGER_LESSEQ";
			case STRING_EQUAL: return "STRING_EQUAL";
			case STRING_UNEQUAL: return "STRING_UNEQUAL";
			case STRING_REGEX: return "STRING_REGEX";
			case GEOJSON_WITHIN: return "GEOJSON_WITHIN";
			case GEOJSON_CONTAINS: return "GEOJSON_CONTAINS";
			case LIST_ITERATE_OR: return "LIST_ITERATE_OR";
			case MAPKEY_ITERATE_OR: return "MAPKEY_ITERATE_OR";
			case MAPVAL_ITERATE_OR: return "MAPVAL_ITERATE_OR";
			case LIST_ITERATE_AND: return "LIST_ITERATE_AND";
			case MAPKEY_ITERATE_AND: return "MAPKEY_ITERATE_AND";
			case MAPVAL_ITERATE_AND: return "MAPVAL_ITERATE_AND";
			default: return "UNKNOWN";
		}
	}
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

	private static final class IntegerValue extends PredExp {
		private static final long serialVersionUID = 8550700351598958079L;

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
		
		public String toString() {
			return operationToString(type) + "(" + value + ")";
		}
	}

	private static final class StringValue extends PredExp {
		private static final long serialVersionUID = -3524085980377177985L;

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
		
		public String toString() {
			return operationToString(type) + "(\"" + value + "\")";
		}
	}

	private static final class GeoJSONValue extends PredExp {
		private static final long serialVersionUID = 4455928732518557753L;

		private final String value;
		private final int type;

		public GeoJSONValue(String value, int type) {
			this.value = value;
			this.type = type;
		}

		public int estimateSize() {
			// type + len + flags + ncells + jsonstr
			return 2 + 4 + 1 + 2 + Buffer.estimateSizeUtf8(this.value);
		}

		public int write(byte[] buf, int offset) {
			// Write value type
			Buffer.shortToBytes(this.type, buf, offset);
			offset += 2;

			// Write value
			int len = Buffer.stringToUtf8(value, buf, offset + 4 + 1 + 2);
			Buffer.intToBytes(len + 1 + 2, buf, offset);
			offset += 4;

			buf[offset] = 0; // flags
			offset += 1;

			Buffer.shortToBytes(0, buf, offset); // ncells
			offset += 2;

			offset += len;
			return offset;
		}
		
		public String toString() {
			return operationToString(type) + "(" + value + ")";
		}
	}

	private static final class AndOr extends PredExp {
		private static final long serialVersionUID = 8758581924470272519L;

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
		
		public String toString() {
			return operationToString(op) + "(" + nexp + ")";
		}

	}

	private static final class Op extends PredExp {
		private static final long serialVersionUID = 6679095249949169203L;

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
		
		public String toString() {
			return operationToString(op);
		}
	}

	private static final class OpInt extends PredExp {
		private static final long serialVersionUID = -6215457665264383104L;

		private final int op;
		private final int flags;

		private OpInt(int op, int flags) {
			this.op = op;
			this.flags = flags;
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
			Buffer.intToBytes(flags, buf, offset);
			offset += 4;
			return offset;
		}
		
		public String toString() {
			return operationToString(op) + "(" + flags + ")";
		}
	}
}
