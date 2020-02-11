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
package com.aerospike.client.lua;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.luaj.vm2.Globals;
import org.luaj.vm2.LoadState;
import org.luaj.vm2.LuaBoolean;
import org.luaj.vm2.LuaClosure;
import org.luaj.vm2.LuaDouble;
import org.luaj.vm2.LuaInteger;
import org.luaj.vm2.LuaString;
import org.luaj.vm2.LuaTable;
import org.luaj.vm2.LuaValue;
import org.luaj.vm2.Prototype;
import org.luaj.vm2.compiler.LuaC;
import org.luaj.vm2.lib.CoroutineLib;
import org.luaj.vm2.lib.DebugLib;
import org.luaj.vm2.lib.LibFunction;
import org.luaj.vm2.lib.PackageLib;
import org.luaj.vm2.lib.StringLib;
import org.luaj.vm2.lib.TableLib;
import org.luaj.vm2.lib.jse.JseBaseLib;
import org.luaj.vm2.lib.jse.JseIoLib;
import org.luaj.vm2.lib.jse.JseMathLib;
import org.luaj.vm2.lib.jse.JseOsLib;
import org.luaj.vm2.lib.jse.LuajavaLib;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.Value;
import com.aerospike.client.command.Buffer;
import com.aerospike.client.command.ParticleType;
import com.aerospike.client.query.Statement;

public final class LuaInstance {

	private final Globals globals = new Globals();
	private final LuaTable loadedTable;

	public LuaInstance() throws AerospikeException {
		globals.load(new JseBaseLib());
		globals.load(new PackageLib());
		//globals.load(new Bit32Lib()); // not needed for 5.1 compatibility
		globals.load(new TableLib());
		globals.load(new StringLib());
		globals.load(new CoroutineLib());
		globals.load(new JseMathLib());
		globals.load(new JseIoLib());
		globals.load(new JseOsLib());
		globals.load(new LuajavaLib());
		globals.load(new DebugLib());

		LuaTable packageTable = (LuaTable)globals.get("package");
		loadedTable = (LuaTable)packageTable.get("loaded");

		globals.load(new LuaBytesLib(this));
		globals.load(new LuaListLib(this));
		globals.load(new LuaMapLib(this));
		globals.load(new LuaStreamLib(this));

		LuaC.install(globals);

		ClassLoader resourceLoader = LuaInstance.class.getClassLoader();
		loadSystemPackage(resourceLoader, "compat52");
		loadSystemPackage(resourceLoader, "stream_ops");
		loadSystemPackage(resourceLoader, "aerospike");

		globals.load(new LuaAerospikeLib(this));
		LoadState.install(globals);
	}

	public void registerPackage(String packageName, LuaTable table) {
		globals.set(packageName, table);
		loadedTable.set(packageName, LuaValue.TRUE);
	}

	public LuaValue getPackage(String packageName) {
		return globals.get(packageName);
	}

	public void loadPackage(Statement statement) {
		if (loadedTable.get(statement.getPackageName()).toboolean()) {
			return;
		}

		Prototype prototype;
		if (statement.getResourceLoader() == null || statement.getResourcePath() == null) {
			prototype = LuaCache.loadPackageFromFile(statement.getPackageName());
		}
		else {
			prototype = LuaCache.loadPackageFromResource(statement.getResourceLoader(), statement.getResourcePath(), statement.getPackageName());
		}

		LuaClosure function = new LuaClosure(prototype, globals);
		function.invoke();
		loadedTable.set(statement.getPackageName(), LuaValue.TRUE);
	}

	private void loadSystemPackage(ClassLoader resourceLoader, String packageName) {
		String resourcePath = "udf/" + packageName + ".lua";
		Prototype prototype = LuaCache.loadPackageFromResource(resourceLoader, resourcePath, packageName);
		LuaClosure function = new LuaClosure(prototype, globals);
		function.invoke();
		loadedTable.set(packageName, LuaValue.TRUE);
	}

	public void unloadPackage(String packageName) {
		// Force package reload the next time the package is referenced.
		loadedTable.set(packageName, LuaValue.FALSE);
	}

	public void load(LibFunction function) {
		globals.load(function);
	}

	public void call(String functionName, LuaValue[] args) {
		globals.get(functionName).invoke(args);
	}

	public LuaValue getFunction(String functionName) {
		return globals.get(functionName);
	}

	public LuaValue getLuaValue(int type, byte[] buf, int offset, int len) throws AerospikeException {
		if (len <= 0) {
			return LuaValue.NIL;
		}

		switch (type) {
		case ParticleType.STRING:
	        byte[] copy = new byte[len];
	        System.arraycopy(buf, offset, copy, 0, len);
			return LuaString.valueOf(copy, 0, len);

		case ParticleType.INTEGER:
			if (len <= 4) {
				return LuaInteger.valueOf(Buffer.bytesToInt(buf, offset));
			}

			if (len <= 8) {
				return LuaInteger.valueOf(Buffer.bytesToLong(buf, offset));
			}
			throw new AerospikeException("Lua BigInteger not implemented.");

		case ParticleType.DOUBLE:
			return LuaDouble.valueOf(Buffer.bytesToDouble(buf, offset));

		case ParticleType.BLOB:
	        byte[] blob = new byte[len];
	        System.arraycopy(buf, offset, blob, 0, len);
			return new LuaBytes(this, blob);

		case ParticleType.JBLOB:
			Object object = Buffer.bytesToObject(buf, offset, len);
			return new LuaJavaBlob(object);

		case ParticleType.LIST: {
			LuaUnpacker unpacker = new LuaUnpacker(this, buf, offset, len);
			return unpacker.unpackList();
		}

		case ParticleType.MAP: {
			LuaUnpacker unpacker = new LuaUnpacker(this, buf, offset, len);
			return unpacker.unpackMap();
		}

		case ParticleType.GEOJSON:
			// skip the flags
			int ncells = Buffer.bytesToShort(buf, offset + 1);
			int hdrsz = 1 + 2 + (ncells * 8);
			return new LuaGeoJSON(new String(buf, offset + hdrsz, len - hdrsz));

		default:
			return LuaValue.NIL;
		}
	}

	public LuaList getLuaList(List<?> list) {
		List<LuaValue> luaList = new ArrayList<LuaValue>();

		for (Object obj : list) {
			luaList.add(getLuaValue(obj));
		}
		return new LuaList(this, luaList);
	}

	public LuaList getLuaList(Value[] array) {
		List<LuaValue> luaList = new ArrayList<LuaValue>();

		for (Value value : array) {
			luaList.add(value.getLuaValue(this));
		}
		return new LuaList(this, luaList);
	}

	public LuaMap getLuaMap(Map<?,?> map) {
		Map<LuaValue,LuaValue> luaMap = new HashMap<LuaValue,LuaValue>(map.size());

		for (Map.Entry<?,?> entry : map.entrySet()) {
			LuaValue key = getLuaValue(entry.getKey());
			LuaValue value = getLuaValue(entry.getValue());
			luaMap.put(key, value);
		}
		return new LuaMap(this, luaMap);
	}

	public LuaValue getLuaValue(Object obj) {
		if (obj == null) {
			return LuaValue.NIL;
		}

		if (obj instanceof LuaValue) {
			return (LuaValue) obj;
		}

		if (obj instanceof Value) {
			Value value = (Value) obj;
			return value.getLuaValue(this);
		}

		if (obj instanceof byte[]) {
			return new LuaBytes(this, (byte[]) obj);
		}

		if (obj instanceof String) {
			return LuaString.valueOf((String) obj);
		}

		if (obj instanceof Integer) {
			return LuaInteger.valueOf((Integer) obj);
		}

		if (obj instanceof Long) {
			return LuaInteger.valueOf((Long) obj);
		}

		if (obj instanceof Double) {
			return LuaDouble.valueOf((Double) obj);
		}

		if (obj instanceof Float) {
			return LuaDouble.valueOf((Float) obj);
		}

		if (obj instanceof Boolean) {
			return LuaBoolean.valueOf((Boolean) obj);
		}

		if (obj instanceof List<?>) {
			return getLuaList((List<?>) obj);
		}

		if (obj instanceof Map<?,?>) {
			return getLuaMap((Map<?,?>) obj);
		}

		return LuaValue.NIL;
	}
}
