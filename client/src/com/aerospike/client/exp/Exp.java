/*
 * Copyright 2012-2024 Aerospike, Inc.
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
package com.aerospike.client.exp;

import java.util.Calendar;
import java.util.List;
import java.util.Map;

import com.aerospike.client.util.Packer;

/**
 * Expression generator.
 */
public abstract class Exp {
	/**
	 * Expression type.
	 */
	public enum Type {
		NIL(0),
		BOOL(1),
		INT(2),
		STRING(3),
		LIST(4),
		MAP(5),
		BLOB(6),
		FLOAT(7),
		GEO(8),
		HLL(9);

		public final int code;

		Type(int code) {
			this.code = code;
		}
	}

	public static final int SELECT_MATCHING_TREE = 0;
	public static final int SELECT_VALUE = 1;
	public static final int SELECT_LIST_VALUE = 1;
	public static final int SELECT_MAP_VALUE = 1;
	public static final int SELECT_MAP_KEY = 2;
	public static final int SELECT_MAP_KEY_VALUE = SELECT_MAP_KEY | SELECT_MAP_VALUE;
	public static final int SELECT_NO_FAIL = 0x10;

	public static final int MODIFY_DEFAULT = 0x00;
    public static final int MODIFY_APPLY = 0x04;
    public static final int MODIFY_NO_FAIL = 0x10;

	public static final int CTX_EXP = 0x04;

	//--------------------------------------------------
	// Build
	//--------------------------------------------------

	/**
	 * Create final expression that contains packed byte instructions used in the wire protocol.
	 */
	public static Expression build(Exp exp) {
		return new Expression(exp);
	}

	/**
	 * Helper method to create Exp objects from unpacked objects
	 */
	public static Exp get(Object obj) {
		if (obj == null) {
			return nil();
		} else if (obj instanceof Boolean) {
			return new Bool((Boolean)obj);
		} else if (obj instanceof Long) {
			return new Int((Long)obj);
		} else if (obj instanceof Double) {
			return new Float((Double)obj);
		} else if (obj instanceof String) {
			return new Str((String)obj);
		} else if (obj instanceof byte[]) {
			return new Blob((byte[])obj);
		} else if (obj instanceof List) {
			List<Object> list = (List<Object>)obj;
			if (!list.isEmpty() && list.get(0) instanceof Number) {
				// This might be a command array, try to reconstruct it
				// For complex expressions, return as ExpBytes
				Packer packer = new Packer();
				packer.packObject(obj);
				packer.createBuffer();
				packer.packObject(obj);
				return new ExpBytes(new Expression(packer.getBuffer()));
			}
			return new ListVal(list);
		} else if (obj instanceof Map) {
			Map<Object, Object> map = (Map<Object, Object>)obj;
			return new MapVal(map);
		} else {
			// For unknown types, wrap as ExpBytes
			Packer packer = new Packer();
			packer.packObject(obj);
			packer.createBuffer();
			packer.packObject(obj);
			return new ExpBytes(new Expression(packer.getBuffer()));
		}
	}

	//--------------------------------------------------
	// Record Key
	//--------------------------------------------------

	/**
	 * Create record key expression of specified type.
	 *
	 * <pre>{@code
	 * // Integer record key >= 100000
	 * Exp.ge(Exp.key(Type.INT), Exp.val(100000))
	 * }</pre>
	 */
	public static Exp key(Type type) {
		return new CmdInt(KEY, type.code);
	}

	/**
	 * Create expression that returns if the primary key is stored in the record meta data
	 * as a boolean expression. This would occur when {@link com.aerospike.client.policy.Policy#sendKey}
	 * is true on record write. This expression usually evaluates quickly because record
	 * meta data is cached in memory.
	 *
	 * <pre>{@code
	 * // Key exists in record meta data
	 * Exp.keyExists()
	 * }</pre>
	 */
	public static Exp keyExists() {
		return new Cmd(KEY_EXISTS);
	}

	//--------------------------------------------------
	// Record Bin
	//--------------------------------------------------

	/**
	 * Create bin expression of specified type.
	 *

	 * <pre>{@code
	 * // String bin "a" == "views"
	 * Exp.eq(Exp.bin("a", Type.STRING), Exp.val("views"))
	 * }</pre>
	 */
	public static Exp bin(String name, Type type) {
		return new Bin(name, type);
	}

	/**
	 * Create 64 bit integer bin expression.
	 *
	 * <pre>{@code
	 * // Integer bin "a" == 200
	 * Exp.eq(Exp.intBin("a"), Exp.val(200))
	 * }</pre>
	 */
	public static Exp intBin(String name) {
		return new Bin(name, Type.INT);
	}

	/**
	 * Create 64 bit float bin expression.
	 *
	 * <pre>{@code
	 * // Float bin "a" >= 1.5
	 * Exp.ge(Exp.floatBin("a"), Exp.val(1.5))
	 * }</pre>
	 */
	public static Exp floatBin(String name) {
		return new Bin(name, Type.FLOAT);
	}

	/**
	 * Create string bin expression.
	 *
	 * <pre>{@code
	 * // String bin "a" == "views"
	 * Exp.eq(Exp.stringBin("a"), Exp.val("views"))
	 * }</pre>
	 */
	public static Exp stringBin(String name) {
		return new Bin(name, Type.STRING);
	}

	/**
	 * Create boolean bin expression.
	 *
	 * <pre>{@code
	 * // Boolean bin "a" == true
	 * Exp.eq(Exp.boolBin("a"), Exp.val(true))
	 * }</pre>
	 */
	public static Exp boolBin(String name) {
		return new Bin(name, Type.BOOL);
	}

	/**
	 * Create byte[] bin expression.
	 *
	 * <pre>{@code
	 * // Blob bin "a" == [1,2,3]
	 * Exp.eq(Exp.blobBin("a"), Exp.val(new byte[] {1, 2, 3}))
	 * }</pre>
	 */
	public static Exp blobBin(String name) {
		return new Bin(name, Type.BLOB);
	}

	/**
	 * Create geospatial bin expression.
	 *
	 * <pre>{@code
	 * // Geo bin "a" == region
	 * String region = "{ \"type\": \"AeroCircle\", \"coordinates\": [[-122.0, 37.5], 50000.0] }";
	 * Exp.geoCompare(Exp.geoBin("loc"), Exp.geo(region))
	 * }</pre>
	 */
	public static Exp geoBin(String name) {
		return new Bin(name, Type.GEO);
	}

	/**
	 * Create list bin expression.
	 *
	 * <pre>{@code
	 * // Bin a[2] == 3
	 * Exp.eq(ListExp.getByIndex(ListReturnType.VALUE, Type.INT, Exp.val(2), Exp.listBin("a")), Exp.val(3))
	 * }</pre>
	 */
	public static Exp listBin(String name) {
		return new Bin(name, Type.LIST);
	}

	/**
	 * Create map bin expression.
	 *
	 * <pre>{@code
	 * // Bin a["key"] == "value"
	 * Exp.eq(
	 *     MapExp.getByKey(MapReturnType.VALUE, Type.STRING, Exp.val("key"), Exp.mapBin("a")),
	 *     Exp.val("value"));
	 * }</pre>
	 */
	public static Exp mapBin(String name) {
		return new Bin(name, Type.MAP);
	}

	/**
	 * Create hll bin expression.
	 *
	 * <pre>{@code
	 * // HLL bin "a" count > 7
	 * Exp.gt(HLLExp.getCount(Exp.hllBin("a")), Exp.val(7))
	 * }</pre>
	 */
	public static Exp hllBin(String name) {
		return new Bin(name, Type.HLL);
	}

	/**
	 * Create expression that returns if bin of specified name exists.
	 *
	 * <pre>{@code
	 * // Bin "a" exists in record
	 * Exp.binExists("a")
	 * }</pre>
	 */
	public static Exp binExists(String name) {
		return Exp.ne(Exp.binType(name), Exp.val(0));
	}

	/**
	 * Create expression that returns bin's integer particle type.
	 * See {@link com.aerospike.client.command.ParticleType}.
	 *
	 * <pre>{@code
	 * // Bin "a" particle type is a list
	 * Exp.eq(Exp.binType("a"), Exp.val(ParticleType.LIST))
	 * }</pre>
	 */
	public static Exp binType(String name) {
		return new CmdStr(BIN_TYPE, name);
	}

	//--------------------------------------------------
	// Misc
	//--------------------------------------------------

	/**
	 * Create expression that returns record set name string. This expression usually
	 * evaluates quickly because record meta data is cached in memory.
	 *
	 * <pre>{@code
	 * // Record set name == "myset"
	 * Exp.eq(Exp.setName(), Exp.val("myset"))
	 * }</pre>
	 */
	public static Exp setName() {
		return new Cmd(SET_NAME);
	}

	/**
	 * Create expression that returns the record size. This expression usually evaluates
	 * quickly because record meta data is cached in memory.
	 * <p>
	 * Requires server version 7.0+. This expression replaces {@link #deviceSize()} and
	 * {@link #memorySize()} since those older expressions are equivalent on server version 7.0+.
	 *
	 * <pre>{@code
	 * // Record size >= 100 KB
	 * Exp.ge(Exp.recordSize(), Exp.val(100 * 1024))
	 * }</pre>
	 */
	public static Exp recordSize() {
		return new Cmd(RECORD_SIZE);
	}

	/**
	 * Create expression that returns record size on disk. If server storage-engine is
	 * memory, then zero is returned. This expression usually evaluates quickly because
	 * record meta data is cached in memory.
	 * <p>
	 * This expression should only be used for server versions less than 7.0. Use
	 * {@link #recordSize()} for server version 7.0+.
	 *
	 * <pre>{@code
	 * // Record device size >= 100 KB
	 * Exp.ge(Exp.deviceSize(), Exp.val(100 * 1024))
	 * }</pre>
	 */
	@Deprecated(since = "9.1.0", forRemoval = true)
	public static Exp deviceSize() {
		return new Cmd(DEVICE_SIZE);
	}

	/**
	 * Create expression that returns record size in memory. If server storage-engine is
	 * not memory nor data-in-memory, then zero is returned. This expression usually evaluates
	 * quickly because record meta data is cached in memory.
	 * <p>
	 * Requires server version between 5.3 inclusive and 7.0 exclusive.
	 * Use {@link #recordSize()} for server version 7.0+.
	 *
	 * <pre>{@code
	 * // Record memory size >= 100 KB
	 * Exp.ge(Exp.memorySize(), Exp.val(100 * 1024))
	 * }</pre>
	 */
	@Deprecated(since = "9.1.0", forRemoval = true)
	public static Exp memorySize() {
		return new Cmd(MEMORY_SIZE);
	}

	/**
	 * Create expression that returns record last update time expressed as 64 bit integer
	 * nanoseconds since 1970-01-01 epoch. This expression usually evaluates quickly because
	 * record meta data is cached in memory.
	 *
	 * <pre>{@code
	 * // Record last update time >= 2020-01-15
	 * Exp.ge(Exp.lastUpdate(), Exp.val(new GregorianCalendar(2020, 0, 15)))
	 * }</pre>
	 */
	public static Exp lastUpdate() {
		return new Cmd(LAST_UPDATE);
	}

	/**
	 * Create expression that returns milliseconds since the record was last updated.
	 * This expression usually evaluates quickly because record meta data is cached in memory.
	 *
	 * <pre>{@code
	 * // Record last updated more than 2 hours ago
	 * Exp.gt(Exp.sinceUpdate(), Exp.val(2 * 60 * 60 * 1000))
	 * }</pre>
	 */
	public static Exp sinceUpdate() {
		return new Cmd(SINCE_UPDATE);
	}

	/**
	 * Create expression that returns record expiration time expressed as 64 bit integer
	 * nanoseconds since 1970-01-01 epoch. This expression usually evaluates quickly because
	 * record meta data is cached in memory.
	 *
	 * <pre>{@code
	 * // Record expires on 2021-01-01
	 * Exp.and(
	 *   Exp.ge(Exp.voidTime(), Exp.val(new GregorianCalendar(2021, 0, 1))),
	 *   Exp.lt(Exp.voidTime(), Exp.val(new GregorianCalendar(2021, 0, 2))))
	 * }</pre>
	 */
	public static Exp voidTime() {
		return new Cmd(VOID_TIME);
	}

	/**
	 * Create expression that returns record expiration time (time to live) in integer seconds.
	 * This expression usually evaluates quickly because record meta data is cached in memory.
	 *
	 * <pre>{@code
	 * // Record expires in less than 1 hour
	 * Exp.lt(Exp.ttl(), Exp.val(60 * 60))
	 * }</pre>
	 */
	public static Exp ttl() {
		return new Cmd(TTL);
	}

	/**
	 * Create expression that returns if record has been deleted and is still in tombstone state.
	 * This expression usually evaluates quickly because record meta data is cached in memory.
	 *
	 * <pre>{@code
	 * // Deleted records that are in tombstone state.
	 * Exp.isTombstone()
	 * }</pre>
	 */
	public static Exp isTombstone() {
		return new Cmd(IS_TOMBSTONE);
	}

	/**
	 * Create expression that returns record digest modulo as integer. This expression usually
	 * evaluates quickly because record meta data is cached in memory.
	 *
	 * <pre>{@code
	 * // Records that have digest(key) % 3 == 1
	 * Exp.eq(Exp.digestModulo(3), Exp.val(1))
	 * }</pre>
	 */
	public static Exp digestModulo(int mod) {
		return new CmdInt(DIGEST_MODULO, mod);
	}

	/**
	 * Create expression that performs a regex match on a string bin or string value expression.
	 *
	 * <pre>{@code
	 * // Select string bin "a" that starts with "prefix" and ends with "suffix".
	 * // Ignore case and do not match newline.
	 * Exp.regexCompare("prefix.*suffix", RegexFlag.ICASE | RegexFlag.NEWLINE, Exp.stringBin("a"))
	 * }</pre>
	 *
	 * @param regex		regular expression string
	 * @param flags		regular expression bit flags. See {@link com.aerospike.client.query.RegexFlag}
	 * @param bin		string bin or string value expression
	 */
	public static Exp regexCompare(String regex, int flags, Exp bin) {
		return new Regex(bin, regex, flags);
	}

	//--------------------------------------------------
	// GEO Spatial
	//--------------------------------------------------

	/**
	 * Create compare geospatial operation.
	 *
	 * <pre>{@code
	 * // Query region within coordinates.
	 * String region =
	 * "{ " +
	 * "  \"type\": \"Polygon\", " +
	 * "  \"coordinates\": [ " +
	 * "    [[-122.500000, 37.000000],[-121.000000, 37.000000], " +
	 * "     [-121.000000, 38.080000],[-122.500000, 38.080000], " +
	 * "     [-122.500000, 37.000000]] " +
	 * "    ] " +
	 * "}";
	 * Exp.geoCompare(Exp.geoBin("a"), Exp.geo(region))
	 * }</pre>
	 */
	public static Exp geoCompare(Exp left, Exp right) {
		return new CmdExp(GEO, left, right);
	}

	/**
	 * Create geospatial json string value.
	 */
	public static Exp geo(String val) {
		return new Geo(val);
	}

	//--------------------------------------------------
	// Value
	//--------------------------------------------------

	/**
	 * Create boolean value.
	 */
	public static Exp val(boolean val) {
		return new Bool(val);
	}

	/**
	 * Create 64 bit integer value.
	 */
	public static Exp val(long val) {
		return new Int(val);
	}

	/**
	 * Create Calendar value expressed in nanoseconds since 1970-01-01 epoch as 64 bit integer.
	 */
	public static Exp val(Calendar val) {
		return new Int(val.getTimeInMillis() * NANOS_PER_MILLIS);
	}

	/**
	 * Create 64 bit floating point value.
	 */
	public static Exp val(double val) {
		return new Float(val);
	}

	/**
	 * Create string value.
	 */
	public static Exp val(String val) {
		return new Str(val);
	}

	/**
	 * Create blob byte[] value.
	 */
	public static Exp val(byte[] val) {
		return new Blob(val);
	}

	/**
	 * Create list value.
	 */
	public static Exp val(List<?> list) {
		return new ListVal(list);
	}

	/**
	 * Create map value. For ordered maps, pass in a TreeMap or a map that implements the SortedMap
	 * interface. For unordered maps, pass in a HashMap.
	 */
	public static Exp val(Map<?,?> map) {
		return new MapVal(map);
	}

	/**
	 * Create nil value.
	 */
	public static Exp nil() {
		return new Nil();
	}

	/**
	 * Create infinity value for use in CDT range expressions.
	 */
	public static Exp inf() {
		return new Infinity();
	}

	/**
	 * Create wildcard value for use in CDT expressions.
	 */
	public static Exp wildcard() {
		return new Wildcard();
	}

	//--------------------------------------------------
	// Boolean Operator
	//--------------------------------------------------

	/**
	 * Create "not" operator expression.
	 *
	 * <pre>{@code
	 * // ! (a == 0 || a == 10)
	 * Exp.not(
	 *   Exp.or(
	 *     Exp.eq(Exp.intBin("a"), Exp.val(0)),
	 *     Exp.eq(Exp.intBin("a"), Exp.val(10))))
	 * }</pre>
	 */
	public static Exp not(Exp exp) {
		return new CmdExp(NOT, exp);
	}

	/**
	 * Create "and" (&amp;&amp;) operator that applies to a variable number of expressions.
	 *
	 * <pre>{@code
	 * // (a > 5 || a == 0) && b < 3
	 * Exp.and(
	 *   Exp.or(
	 *     Exp.gt(Exp.intBin("a"), Exp.val(5)),
	 *     Exp.eq(Exp.intBin("a"), Exp.val(0))),
	 *   Exp.lt(Exp.intBin("b"), Exp.val(3)))
	 * }</pre>
	 */
	public static Exp and(Exp... exps) {
		return new CmdExp(AND, exps);
	}

	/**
	 * Create "or" (||) operator that applies to a variable number of expressions.
	 *
	 * <pre>{@code
	 * // a == 0 || b == 0
	 * Exp.or(
	 *   Exp.eq(Exp.intBin("a"), Exp.val(0)),
	 *   Exp.eq(Exp.intBin("b"), Exp.val(0)));
	 * }</pre>
	 */
	public static Exp or(Exp... exps) {
		return new CmdExp(OR, exps);
	}

	/**
	 * Create expression that returns true if only one of the expressions are true.
	 * Requires server version 5.6.0+.
	 *
	 * <pre>{@code
	 * // exclusive(a == 0, b == 0)
	 * Exp.exclusive(
	 *   Exp.eq(Exp.intBin("a"), Exp.val(0)),
	 *   Exp.eq(Exp.intBin("b"), Exp.val(0)));
	 * }</pre>
	 */
	public static Exp exclusive(Exp... exps) {
		return new CmdExp(EXCLUSIVE, exps);
	}

	/**
	 * Create equal (==) expression.
	 *
	 * <pre>{@code
	 * // a == 11
	 * Exp.eq(Exp.intBin("a"), Exp.val(11))
	 * }</pre>
	 */
	public static Exp eq(Exp left, Exp right) {
		return new CmdExp(EQ, left, right);
	}

	/**
	 * Create not equal (!=) expression
	 *
	 * <pre>{@code
	 * // a != 13
	 * Exp.ne(Exp.intBin("a"), Exp.val(13))
	 * }</pre>
	 */
	public static Exp ne(Exp left, Exp right) {
		return new CmdExp(NE, left, right);
	}

	/**
	 * Create greater than (>) operation.
	 *
	 * <pre>{@code
	 * // a > 8
	 * Exp.gt(Exp.intBin("a"), Exp.val(8))
	 * }</pre>
	 */
	public static Exp gt(Exp left, Exp right) {
		return new CmdExp(GT, left, right);
	}

	/**
	 * Create greater than or equal (>=) operation.
	 *
	 * <pre>{@code
	 * // a >= 88
	 * Exp.ge(Exp.intBin("a"), Exp.val(88))
	 * }</pre>
	 */
	public static Exp ge(Exp left, Exp right) {
		return new CmdExp(GE, left, right);
	}

	/**
	 * Create less than (&lt;) operation.
	 *
	 * <pre>{@code
	 * // a < 1000
	 * Exp.lt(Exp.intBin("a"), Exp.val(1000))
	 * }</pre>
	 */
	public static Exp lt(Exp left, Exp right) {
		return new CmdExp(LT, left, right);
	}

	/**
	 * Create less than or equals (&lt;=) operation.
	 *
	 * <pre>{@code
	 * // a <= 1
	 * Exp.le(Exp.intBin("a"), Exp.val(1))
	 * }</pre>
	 */
	public static Exp le(Exp left, Exp right) {
		return new CmdExp(LE, left, right);
	}

	//--------------------------------------------------
	// Number Operator
	//--------------------------------------------------

	/**
	 * Create "add" (+) operator that applies to a variable number of expressions.
	 * Return sum of all arguments. All arguments must resolve to the same type (integer or float).
	 * Requires server version 5.6.0+.
	 *
	 * <pre>{@code
	 * // a + b + c == 10
	 * Exp.eq(
	 *   Exp.add(Exp.intBin("a"), Exp.intBin("b"), Exp.intBin("c")),
	 *   Exp.val(10));
	 * }</pre>
	 */
	public static Exp add(Exp... exps) {
		return new CmdExp(ADD, exps);
	}

	/**
	 * Create "subtract" (-) operator that applies to a variable number of expressions.
	 * If only one argument is provided, return the negation of that argument.
	 * Otherwise, return the sum of the 2nd to Nth argument subtracted from the 1st
	 * argument. All arguments must resolve to the same type (integer or float).
	 * Requires server version 5.6.0+.
	 *
	 * <pre>{@code
	 * // a - b - c > 10
	 * Exp.gt(
	 *   Exp.sub(Exp.intBin("a"), Exp.intBin("b"), Exp.intBin("c")),
	 *   Exp.val(10));
	 * }</pre>
	 */
	public static Exp sub(Exp... exps) {
		return new CmdExp(SUB, exps);
	}

	/**
	 * Create "multiply" (*) operator that applies to a variable number of expressions.
	 * Return the product of all arguments. If only one argument is supplied, return
	 * that argument. All arguments must resolve to the same type (integer or float).
	 * Requires server version 5.6.0+.
	 *
	 * <pre>{@code
	 * // a * b * c < 100
	 * Exp.lt(
	 *   Exp.mul(Exp.intBin("a"), Exp.intBin("b"), Exp.intBin("c")),
	 *   Exp.val(100));
	 * }</pre>
	 */
	public static Exp mul(Exp... exps) {
		return new CmdExp(MUL, exps);
	}

	/**
	 * Create "divide" (/) operator that applies to a variable number of expressions.
	 * If there is only one argument, returns the reciprocal for that argument.
	 * Otherwise, return the first argument divided by the product of the rest.
	 * All arguments must resolve to the same type (integer or float).
	 * Requires server version 5.6.0+.
	 *
	 * <pre>{@code
	 * // a / b / c > 1
	 * Exp.gt(
	 *   Exp.div(Exp.intBin("a"), Exp.intBin("b"), Exp.intBin("c")),
	 *   Exp.val(1));
	 * }</pre>
	 */
	public static Exp div(Exp... exps) {
		return new CmdExp(DIV, exps);
	}

	/**
	 * Create "power" operator that raises a "base" to the "exponent" power.
	 * All arguments must resolve to floats.
	 * Requires server version 5.6.0+.
	 *
	 * <pre>{@code
	 * // pow(a, 2.0) == 4.0
	 * Exp.eq(
	 *   Exp.pow(Exp.floatBin("a"), Exp.val(2.0)),
	 *   Exp.val(4.0));
	 * }</pre>
	 */
	public static Exp pow(Exp base, Exp exponent) {
		return new CmdExp(POW, base, exponent);
	}

	/**
	 * Create "log" operator for logarithm of "num" with base "base".
	 * All arguments must resolve to floats.
	 * Requires server version 5.6.0+.
	 *
	 * <pre>{@code
	 * // log(a, 2.0) == 4.0
	 * Exp.eq(
	 *   Exp.log(Exp.floatBin("a"), Exp.val(2.0)),
	 *   Exp.val(4.0));
	 * }</pre>
	 */
	public static Exp log(Exp num, Exp base) {
		return new CmdExp(LOG, num, base);
	}

	/**
	 * Create "modulo" (%) operator that determines the remainder of "numerator"
	 * divided by "denominator". All arguments must resolve to integers.
	 * Requires server version 5.6.0+.
	 *
	 * <pre>{@code
	 * // a % 10 == 0
	 * Exp.eq(
	 *   Exp.mod(Exp.intBin("a"), Exp.val(10)),
	 *   Exp.val(0));
	 * }</pre>
	 */
	public static Exp mod(Exp numerator, Exp denominator) {
		return new CmdExp(MOD, numerator, denominator);
	}

	/**
	 * Create operator that returns absolute value of a number.
	 * All arguments must resolve to integer or float.
	 * Requires server version 5.6.0+.
	 *
	 * <pre>{@code
	 * // abs(a) == 1
	 * Exp.eq(
	 *   Exp.abs(Exp.intBin("a")),
	 *   Exp.val(1));
	 * }</pre>
	 */
	public static Exp abs(Exp value) {
		return new CmdExp(ABS, value);
	}

	/**
	 * Create expression that rounds a floating point number down to the closest integer value.
	 * The return type is float. Requires server version 5.6.0+.
	 *
	 * <pre>{@code
	 * // floor(2.95) == 2.0
	 * Exp.eq(
	 *   Exp.floor(Exp.val(2.95)),
	 *   Exp.val(2.0));
	 * }</pre>
	 */
	public static Exp floor(Exp num) {
		return new CmdExp(FLOOR, num);
	}

	/**
	 * Create expression that rounds a floating point number up to the closest integer value.
	 * The return type is float. Requires server version 5.6.0+.
	 *
	 * <pre>{@code
	 * // ceil(2.15) >= 3.0
	 * Exp.ge(
	 *   Exp.ceil(Exp.val(2.15)),
	 *   Exp.val(3.0));
	 * }</pre>
	 */
	public static Exp ceil(Exp num) {
		return new CmdExp(CEIL, num);
	}

	/**
	 * Create expression that converts a float to an integer.
	 * Requires server version 5.6.0+.
	 *
	 * <pre>{@code
	 * // int(2.5) == 2
	 * Exp.eq(
	 *   Exp.toInt(Exp.val(2.5)),
	 *   Exp.val(2));
	 * }</pre>
	 */
	public static Exp toInt(Exp num) {
		return new CmdExp(TO_INT, num);
	}

	/**
	 * Create expression that converts an integer to a float.
	 * Requires server version 5.6.0+.
	 *
	 * <pre>{@code
	 * // float(2) == 2.0
	 * Exp.eq(
	 *   Exp.toFloat(Exp.val(2))),
	 *   Exp.val(2.0));
	 * }</pre>
	 */
	public static Exp toFloat(Exp num) {
		return new CmdExp(TO_FLOAT, num);
	}

	/**
	 * Create integer "and" (&amp;) operator that is applied to two or more integers.
	 * All arguments must resolve to integers.
	 * Requires server version 5.6.0+.
	 *
	 * <pre>{@code
	 * // a & 0xff == 0x11
	 * Exp.eq(
	 *   Exp.intAnd(Exp.intBin("a"), Exp.val(0xff)),
	 *   Exp.val(0x11));
	 * }</pre>
	 */
	public static Exp intAnd(Exp... exps) {
		return new CmdExp(INT_AND, exps);
	}

	/**
	 * Create integer "or" (|) operator that is applied to two or more integers.
	 * All arguments must resolve to integers.
	 * Requires server version 5.6.0+.
	 *
	 * <pre>{@code
	 * // a | 0x10 != 0
	 * Exp.ne(
	 *   Exp.intOr(Exp.intBin("a"), Exp.val(0x10)),
	 *   Exp.val(0));
	 * }</pre>
	 */
	public static Exp intOr(Exp... exps) {
		return new CmdExp(INT_OR, exps);
	}

	/**
	 * Create integer "xor" (^) operator that is applied to two or more integers.
	 * All arguments must resolve to integers.
	 * Requires server version 5.6.0+.
	 *
	 * <pre>{@code
	 * // a ^ b == 16
	 * Exp.eq(
	 *   Exp.intXor(Exp.intBin("a"), Exp.intBin("b")),
	 *   Exp.val(16));
	 * }</pre>
	 */
	public static Exp intXor(Exp... exps) {
		return new CmdExp(INT_XOR, exps);
	}

	/**
	 * Create integer "not" (~) operator.
	 * Requires server version 5.6.0+.
	 *
	 * <pre>{@code
	 * // ~a == 7
	 * Exp.eq(
	 *   Exp.intNot(Exp.intBin("a")),
	 *   Exp.val(7));
	 * }</pre>
	 */
	public static Exp intNot(Exp exp) {
		return new CmdExp(INT_NOT, exp);
	}

	/**
	 * Create integer "left shift" (&lt;&lt;) operator.
	 * Requires server version 5.6.0+.
	 *
	 * <pre>{@code
	 * // a << 8 > 0xff
	 * Exp.gt(
	 *   Exp.lshift(Exp.intBin("a"), Exp.val(8)),
	 *   Exp.val(0xff));
	 * }</pre>
	 */
	public static Exp lshift(Exp value, Exp shift) {
		return new CmdExp(INT_LSHIFT, value, shift);
	}

	/**
	 * Create integer "logical right shift" (>>>) operator.
	 * Requires server version 5.6.0+.
	 *
	 * <pre>{@code
	 * // a >>> 8 > 0xff
	 * Exp.gt(
	 *   Exp.rshift(Exp.intBin("a"), Exp.val(8)),
	 *   Exp.val(0xff));
	 * }</pre>
	 */
	public static Exp rshift(Exp value, Exp shift) {
		return new CmdExp(INT_RSHIFT, value, shift);
	}

	/**
	 * Create integer "arithmetic right shift" (>>) operator.
	 * Requires server version 5.6.0+.
	 *
	 * <pre>{@code
	 * // a >> 8 > 0xff
	 * Exp.gt(
	 *   Exp.arshift(Exp.intBin("a"), Exp.val(8)),
	 *   Exp.val(0xff));
	 * }</pre>
	 */
	public static Exp arshift(Exp value, Exp shift) {
		return new CmdExp(INT_ARSHIFT, value, shift);
	}

	/**
	 * Create expression that returns count of integer bits that are set to 1.
	 * Requires server version 5.6.0+.
	 *
	 * <pre>{@code
	 * // count(a) == 4
	 * Exp.eq(
	 *   Exp.count(Exp.intBin("a")),
	 *   Exp.val(4));
	 * }</pre>
	 */
	public static Exp count(Exp exp) {
		return new CmdExp(INT_COUNT, exp);
	}

	/**
	 * Create expression that scans integer bits from left (most significant bit) to
	 * right (least significant bit), looking for a search bit value. When the
	 * search value is found, the index of that bit (where the most significant bit is
	 * index 0) is returned. If "search" is true, the scan will search for the bit
	 * value 1. If "search" is false it will search for bit value 0.
	 * Requires server version 5.6.0+.
	 *
	 * <pre>{@code
	 * // lscan(a, true) == 4
	 * Exp.eq(
	 *   Exp.lscan(Exp.intBin("a"), Exp.val(true)),
	 *   Exp.val(4));
	 * }</pre>
	 */
	public static Exp lscan(Exp value, Exp search) {
		return new CmdExp(INT_LSCAN, value, search);
	}

	/**
	 * Create expression that scans integer bits from right (least significant bit) to
	 * left (most significant bit), looking for a search bit value. When the
	 * search value is found, the index of that bit (where the most significant bit is
	 * index 0) is returned. If "search" is true, the scan will search for the bit
	 * value 1. If "search" is false it will search for bit value 0.
	 * Requires server version 5.6.0+.
	 *
	 * <pre>{@code
	 * // rscan(a, true) == 4
	 * Exp.eq(
	 *   Exp.rscan(Exp.intBin("a"), Exp.val(true)),
	 *   Exp.val(4));
	 * }</pre>
	 */
	public static Exp rscan(Exp value, Exp search) {
		return new CmdExp(INT_RSCAN, value, search);
	}

	/**
	 * Create expression that returns the minimum value in a variable number of expressions.
	 * All arguments must be the same type (integer or float).
	 * Requires server version 5.6.0+.
	 *
	 * <pre>{@code
	 * // min(a, b, c) > 0
	 * Exp.gt(
	 *   Exp.min(Exp.intBin("a"), Exp.intBin("b"), Exp.intBin("c")),
	 *   Exp.val(0));
	 * }</pre>
	 */
	public static Exp min(Exp... exps) {
		return new CmdExp(MIN, exps);
	}

	/**
	 * Create expression that returns the maximum value in a variable number of expressions.
	 * All arguments must be the same type (integer or float).
	 * Requires server version 5.6.0+.
	 *
	 * <pre>{@code
	 * // max(a, b, c) > 100
	 * Exp.gt(
	 *   Exp.max(Exp.intBin("a"), Exp.intBin("b"), Exp.intBin("c")),
	 *   Exp.val(100));
	 * }</pre>
	 */
	public static Exp max(Exp... exps) {
		return new CmdExp(MAX, exps);
	}

	//--------------------------------------------------
	// Variables
	//--------------------------------------------------

	/**
	 * Conditionally select an action expression from a variable number of expression pairs
	 * followed by a default action expression. Every action expression must return the same type.
	 * The only exception is {@link #unknown()} which can be mixed with other types.
	 * <p>
	 * Requires server version 5.6.0+.
	 *
	 * <pre>{@code
	 * Args Format: bool exp1, action exp1, bool exp2, action exp2, ..., action-default
	 *
	 * // Apply operator based on type.
	 * Exp.cond(
	 *   Exp.eq(Exp.intBin("type"), Exp.val(0)), Exp.add(Exp.intBin("val1"), Exp.intBin("val2")),
	 *   Exp.eq(Exp.intBin("type"), Exp.val(1)), Exp.sub(Exp.intBin("val1"), Exp.intBin("val2")),
	 *   Exp.eq(Exp.intBin("type"), Exp.val(2)), Exp.mul(Exp.intBin("val1"), Exp.intBin("val2")),
	 *   Exp.val(-1));
	 * }</pre>
	 */
	public static Exp cond(Exp... exps) {
		return new CmdExp(COND, exps);
	}

	/**
	 * Define variables and expressions in scope.
	 * Requires server version 5.6.0+.
	 *
	 * <pre>{@code
	 * Args Format: <def1>, <def2>, ..., <exp>
	 * def: {@link Exp#def(String, Exp)}
	 * exp: Scoped expression
	 * }</pre>
	 *
	 * <pre>{@code
	 * // 5 < a < 10
	 * Exp.let(
	 *   Exp.def("x", Exp.intBin("a")),
	 *   Exp.and(
	 *     Exp.lt(Exp.val(5), Exp.var("x")),
	 *     Exp.lt(Exp.var("x"), Exp.val(10))));
	 * }</pre>
	 */
	public static Exp let(Exp... exps) {
		return new Let(exps);
	}

	/**
	 * Assign variable to a {@link Exp#let(Exp...)} expression that can be accessed later.
	 * Requires server version 5.6.0+.
	 *
	 * <pre>{@code
	 * // 5 < a < 10
	 * Exp.let(
	 *   Exp.def("x", Exp.intBin("a")),
	 *   Exp.and(
	 *     Exp.lt(Exp.val(5), Exp.var("x")),
	 *     Exp.lt(Exp.var("x"), Exp.val(10))));
	 * }</pre>
	 */
	public static Exp def(String name, Exp value) {
		return new Def(name, value);
	}

	/**
	 * Retrieve expression value from a variable.
	 * Requires server version 5.6.0+.
	 *
	 * <pre>{@code
	 * // 5 < a < 10
	 * Exp.let(
	 *   Exp.def("x", Exp.intBin("a")),
	 *   Exp.and(
	 *     Exp.lt(Exp.val(5), Exp.var("x")),
	 *     Exp.lt(Exp.var("x"), Exp.val(10))));
	 * }</pre>
	 */
	public static Exp var(String name) {
		return new CmdStr(VAR, name);
	}

	/**
	 * Create expression that references a built-in variable.
	 * Requires server version 8.1.1
	 *
	 * <pre>{@code
	 * Exp.stringLoopVar(LoopVarPart.MAP_KEY)
	 * }</pre>
	 */
	public static Exp stringLoopVar(LoopVarPart part) {
		return new Var(Type.STRING.code, part.id);
	}

	/**
	 * Create expression that references a built-in variable.
	 * Requires server version 8.1.1
	 *
	 * <pre>{@code
	 * Exp.boolLoopVar(LoopVarPart.MAP_KEY)
	 * }</pre>
	 */
	public static Exp boolLoopVar(LoopVarPart part) {
		return new Var(Type.BOOL.code, part.id);
	}

	/**
	 * Create expression that references a built-in variable.
	 * Requires server version 8.1.1
	 *
	 * <pre>{@code
	 * Exp.hllLoopVar(LoopVarPart.MAP_KEY)
	 * }</pre>
	 */
	public static Exp hllLoopVar(LoopVarPart part) {
		return new Var(Type.HLL.code, part.id);
	}

	/**
	 * Create expression that references a built-in variable.
	 * Requires server version 8.1.1
	 * 
	 * <pre>{@code
	 * Exp.intLoopVar(LoopVarPart.MAP_KEY)
	 * }</pre>
	 */
	public static Exp intLoopVar(LoopVarPart part) {
		return new Var(Type.INT.code, part.id);
	}

	/**
	 * Create expression that references a built-in variable.
	 * Requires server version 8.1.1
	 * 
	 * <pre>{@code
	 * Exp.floatLoopVar(LoopVarPart.MAP_KEY)
	 * }</pre>
	 */
	public static Exp floatLoopVar(LoopVarPart part) {
		return new Var(Type.FLOAT.code, part.id);
	}

	/**
	 * Create expression that references a built-in variable.
	 * Requires server version 8.1.1
	 * 
	 * <pre>{@code
	 * Exp.listLoopVar(LoopVarPart.MAP_KEY)
	 * }</pre>
	 */
	public static Exp listLoopVar(LoopVarPart part)	{
		return new Var(Type.LIST.code, part.id);
	}

	/**
	 * Create expression that references a built-in variable.
	 * Requires server version 8.1.1
	 * 
	 * <pre>{@code
	 * Exp.mapLoopVar(LoopVarPart.MAP_KEY)
	 * }</pre>
	 */
	public static Exp mapLoopVar(LoopVarPart part)	{
		return new Var(Type.MAP.code, part.id);
	}

	/**
	 * Create expression that references a built-in variable.
	 * Requires server version 8.1.1
	 * 
	 * <pre>{@code
	 * Exp.blobLoopVar(LoopVarPart.MAP_KEY)
	 * }</pre>
	 */
	public static Exp blobLoopVar(LoopVarPart part)	{
		return new Var(Type.BLOB.code, part.id);
	}

	/**
	 * Create expression that references a built-in variable.
	 * Requires server version 8.1.1
	 * 
	 * <pre>{@code
	 * Exp.nilLoopVar(LoopVarPart.MAP_KEY)
	 * }</pre>
	 */
	public static Exp nilLoopVar(LoopVarPart part)	{
		return new Var(Type.NIL.code, part.id);
	}

	/**
	 * Create expression that references a built-in variable.
	 * Requires server version 8.1.1
	 * 
	 * <pre>{@code
	 * Exp.geoJsonLoopVar(LoopVarPart.MAP_KEY)
	 * }</pre>
	 */
	public static Exp geoJsonLoopVar(LoopVarPart part)	{
		return new Var(Type.GEO.code, part.id);
	}

	/**
     * Creates a result remove expression.
     * Requires server version 8.1.1+.
	 *
	 * <pre>{@code
	 * Exp.removeResults()
	 * }</pre>
	 */
	public static Exp removeResults() {
		return new Cmd(RESULT_REMOVE);
	}

	//--------------------------------------------------
	// Miscellaneous
	//--------------------------------------------------

	/**
	 * Create unknown value. Used to intentionally fail an expression.
	 * The failure can be ignored with {@link com.aerospike.client.exp.ExpWriteFlags#EVAL_NO_FAIL}
	 * or {@link com.aerospike.client.exp.ExpReadFlags#EVAL_NO_FAIL}.
	 * Requires server version 5.6.0+.
	 *
	 * <pre>{@code
	 * // double v = balance - 100.0;
	 * // return (v > 0.0)? v : unknown;
	 * Exp.let(
	 *   Exp.def("v", Exp.sub(Exp.floatBin("balance"), Exp.val(100.0))),
	 *   Exp.cond(
	 *     Exp.ge(Exp.var("v"), Exp.val(0.0)), Exp.var("v"),
	 *     Exp.unknown()));
	 * }</pre>
	 */
	public static Exp unknown() {
		return new Cmd(UNKNOWN);
	}

	/**
	 * Merge precompiled expression into a new expression tree.
	 * Useful for storing common precompiled expressions and then reusing
	 * these expressions as part of a greater expression.
	 *
	 * <pre>{@code
	 * // Merge precompiled expression into new expression.
	 * Expression e = Exp.build(Exp.eq(Exp.intBin("a"), Exp.val(200)));
	 * Expression merged = Exp.build(Exp.and(Exp.expr(e), Exp.eq(Exp.intBin("b"), Exp.val(100))));
	 * }</pre>
	 */
	public static Exp expr(Expression e) {
		return new ExpBytes(e);
	}

	//--------------------------------------------------
	// Internal
	//--------------------------------------------------

	private static final int UNKNOWN = 0;
	private static final int EQ = 1;
	private static final int NE = 2;
	private static final int GT = 3;
	private static final int GE = 4;
	private static final int LT = 5;
	private static final int LE = 6;
	private static final int REGEX = 7;
	private static final int GEO = 8;
	private static final int AND = 16;
	private static final int OR = 17;
	private static final int NOT = 18;
	private static final int EXCLUSIVE = 19;
	private static final int ADD = 20;
	private static final int SUB = 21;
	private static final int MUL = 22;
	private static final int DIV = 23;
	private static final int POW = 24;
	private static final int LOG = 25;
	private static final int MOD = 26;
	private static final int ABS = 27;
	private static final int FLOOR = 28;
	private static final int CEIL = 29;
	private static final int TO_INT = 30;
	private static final int TO_FLOAT = 31;
	private static final int INT_AND = 32;
	private static final int INT_OR = 33;
	private static final int INT_XOR = 34;
	private static final int INT_NOT = 35;
	private static final int INT_LSHIFT = 36;
	private static final int INT_RSHIFT = 37;
	private static final int INT_ARSHIFT = 38;
	private static final int INT_COUNT = 39;
	private static final int INT_LSCAN = 40;
	private static final int INT_RSCAN = 41;
	private static final int MIN = 50;
	private static final int MAX = 51;
	private static final int DIGEST_MODULO = 64;
	private static final int DEVICE_SIZE = 65;
	private static final int LAST_UPDATE = 66;
	private static final int SINCE_UPDATE = 67;
	private static final int VOID_TIME = 68;
	private static final int TTL = 69;
	private static final int SET_NAME = 70;
	private static final int KEY_EXISTS = 71;
	private static final int IS_TOMBSTONE = 72;
	private static final int MEMORY_SIZE = 73;
	private static final int RECORD_SIZE = 74;
	private static final int KEY = 80;
	private static final int BIN = 81;
	private static final int BIN_TYPE = 82;
	private static final int RESULT_REMOVE = 100;
	private static final int VAR_BUILTIN = 122;
	private static final int COND = 123;
	private static final int VAR = 124;
	private static final int LET = 125;
	private static final int QUOTED = 126;
	public static final int CALL = 127;
	public static final int MODIFY = 0x40;
	private static final long NANOS_PER_MILLIS = 1000000L;

	public abstract void pack(Packer packer);

	/**
	 * For internal use only.
	 */
	static class Module extends Exp {
		private final Exp bin;
		private final byte[] bytes;
		private final int retType;
		private final int module;

		public Module(Exp bin, byte[] bytes, int retType, int module) {
			this.bin = bin;
			this.bytes = bytes;
			this.retType = retType;
			this.module = module;
		}

		@Override
		public void pack(Packer packer) {
			packer.packArrayBegin(5);
			packer.packInt(Exp.CALL);
			packer.packInt(retType);
			packer.packInt(module);
			packer.packByteArray(bytes, 0, bytes.length);
			bin.pack(packer);
		}
	}

	private static final class Bin extends Exp {
		private final String name;
		private final Type type;

		public Bin(String name, Type type) {
			this.name = name;
			this.type = type;
		}

		@Override
		public void pack(Packer packer) {
			packer.packArrayBegin(3);
			packer.packInt(BIN);
			packer.packInt(type.code);
			packer.packString(name);
		}
	}

	private static final class Regex extends Exp {
		private final Exp bin;
		private final String regex;
		private final int flags;

		private Regex(Exp bin, String regex, int flags) {
			this.bin = bin;
			this.regex = regex;
			this.flags = flags;
		}

		@Override
		public void pack(Packer packer) {
			packer.packArrayBegin(4);
			packer.packInt(REGEX);
			packer.packInt(flags);
			packer.packString(regex);
			bin.pack(packer);
		}
	}

	private static final class Let extends Exp {
		private final Exp[] exps;

		private Let(Exp... exps) {
			this.exps = exps;
		}

		@Override
		public void pack(Packer packer) {
			// Let wire format: LET <defname1>, <defexp1>, <defname2>, <defexp2>, ..., <scope exp>
			int count = (exps.length - 1) * 2 + 2;
			packer.packArrayBegin(count);
			packer.packInt(LET);

			for (Exp exp : exps) {
				exp.pack(packer);
			}
		}
	}

	private static final class Def extends Exp {
		private final String name;
		private final Exp exp;

		private Def(String name, Exp exp) {
			this.name = name;
			this.exp = exp;
		}

		@Override
		public void pack(Packer packer) {
			packer.packString(name);
			exp.pack(packer);
		}
	}

	private static final class CmdExp extends Exp {
		private final Exp[] exps;
		private final int cmd;

		private CmdExp(int cmd, Exp... exps) {
			this.exps = exps;
			this.cmd = cmd;
		}

		@Override
		public void pack(Packer packer) {
			packer.packArrayBegin(exps.length + 1);
			packer.packInt(cmd);

			for (Exp exp : exps) {
				exp.pack(packer);
			}
		}
	}

	private static final class CmdInt extends Exp {
		private final int cmd;
		private final int val;

		private CmdInt(int cmd, int val) {
			this.cmd = cmd;
			this.val = val;
		}

		@Override
		public void pack(Packer packer) {
			packer.packArrayBegin(2);
			packer.packInt(cmd);
			packer.packInt(val);
		}
	}

	private static final class CmdStr extends Exp {
		private final String str;
		private final int cmd;

		private CmdStr(int cmd, String str) {
			this.str = str;
			this.cmd = cmd;
		}

		@Override
		public void pack(Packer packer) {
			packer.packArrayBegin(2);
			packer.packInt(cmd);
			packer.packString(str);
		}
	}

	private static final class Cmd extends Exp {
		private final int cmd;

		private Cmd(int cmd) {
			this.cmd = cmd;
		}

		@Override
		public void pack(Packer packer) {
			packer.packArrayBegin(1);
			packer.packInt(cmd);
		}
	}

	private static final class Bool extends Exp {
		private final boolean val;

		private Bool(boolean val) {
			this.val = val;
		}

		@Override
		public void pack(Packer packer) {
			packer.packBoolean(val);
		}
	}

	private static final class Int extends Exp {
		private final long val;

		private Int(long val) {
			this.val = val;
		}

		@Override
		public void pack(Packer packer) {
			packer.packLong(val);
		}
	}

	private static final class Float extends Exp {
		private final double val;

		private Float(double val) {
			this.val = val;
		}

		@Override
		public void pack(Packer packer) {
			packer.packDouble(val);

		}
	}

	private static final class Str extends Exp {
		private final String val;

		private Str(String val) {
			this.val = val;
		}

		@Override
		public void pack(Packer packer) {
			packer.packParticleString(val);
		}
	}

	private static final class Geo extends Exp {
		private final String val;

		private Geo(String val) {
			this.val = val;
		}

		@Override
		public void pack(Packer packer) {
			packer.packGeoJSON(val);
		}
	}

	private static final class Blob extends Exp {
		private final byte[] val;

		private Blob(byte[] val) {
			this.val = val;
		}

		@Override
		public void pack(Packer packer) {
			packer.packParticleBytes(val);

		}
	}

	private static final class ListVal extends Exp {
		private final List<?> list;

		private ListVal(List<?> list) {
			this.list = list;
		}

		@Override
		public void pack(Packer packer) {
			// List values need an extra array and QUOTED in order to distinguish
			// between a multiple argument array call and a local list.
			packer.packArrayBegin(2);
			packer.packInt(QUOTED);
			packer.packList(list);
		}
	}

	private static final class MapVal extends Exp {
		private final Map<?,?> map;

		private MapVal(Map<?,?> map) {
			this.map = map;
		}

		@Override
		public void pack(Packer packer) {
			packer.packMap(map);
		}
	}

	private static final class Nil extends Exp {
		@Override
		public void pack(Packer packer) {
			packer.packNil();
		}
	}

	private static final class Infinity extends Exp {
		@Override
		public void pack(Packer packer) {
			packer.packInfinity();
		}
	}

	private static final class Wildcard extends Exp {
		@Override
		public void pack(Packer packer) {
			packer.packWildcard();
		}
	}

	private static final class Var extends Exp {
		private final int type;
		private final int varId;

		private Var(int type, int varId) {
			this.type = type;
			this.varId = varId;
		}

		@Override
		public void pack(Packer packer) {
			packer.packArrayBegin(3);
			packer.packInt(Exp.VAR_BUILTIN);
			packer.packInt(type);
			packer.packInt(varId);
		}
	}

	private static final class ExpBytes extends Exp {
		private final byte[] bytes;

		private ExpBytes(Expression e) {
			this.bytes = e.getBytes();
		}

		@Override
		public void pack(Packer packer) {
			packer.packByteArray(bytes, 0, bytes.length);
		}
	}
}
