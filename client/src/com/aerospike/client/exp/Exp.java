/*
 * Copyright 2012-2021 Aerospike, Inc.
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

	/**
	 * Create final expression that contains packed byte instructions used in the wire protocol.
	 */
	public static Expression build(Exp exp) {
		return new Expression(exp);
	}

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
	 * Exp.geoCompare(Exp.geoBin("loc"), Exp.val(region))
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
	 * Create expression that returns record size on disk. If server storage-engine is
	 * memory, then zero is returned. This expression usually evaluates quickly because
	 * record meta data is cached in memory.
	 *
	 * <pre>{@code
	 * // Record device size >= 100 KB
	 * Exp.ge(Exp.deviceSize(), Exp.val(100 * 1024))
	 * }</pre>
	 */
	public static Exp deviceSize() {
		return new Cmd(DEVICE_SIZE);
	}

	/**
	 * Create expression that returns record size in memory. If server storage-engine is
	 * not memory nor data-in-memory, then zero is returned. This expression usually evaluates
	 * quickly because record meta data is cached in memory.
	 * <p>
	 * This method requires Aerospike Server version >= 5.3.0.
	 *
	 * <pre>{@code
	 * // Record memory size >= 100 KB
	 * Exp.ge(Exp.memorySize(), Exp.val(100 * 1024))
	 * }</pre>
	 */
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
	 * Create map value.
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
	 * Create "and" (&&) operator that applies to a variable number of expressions.
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
	 * Create less than (<) operation.
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
	 * Create less than or equals (<=) operation.
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
	// Internal
	//--------------------------------------------------

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

	private static final int KEY = 80;
	private static final int BIN = 81;
	private static final int BIN_TYPE = 82;
	private static final int QUOTED = 126;
	private static final int CALL = 127;
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
}
