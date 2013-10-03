/*
 * Aerospike Client - Java Library
 *
 * Copyright 2013 by Aerospike, Inc. All rights reserved.
 *
 * Availability of this source code to partners and customers includes
 * redistribution rights covered by individual contract. Please check your
 * contract for exact rights and responsibilities.
 */
package com.aerospike.client.lua;

import java.util.List;
import java.util.Map;

import org.luaj.vm2.Globals;
import org.luaj.vm2.LuaClosure;
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
import com.aerospike.client.command.Buffer;
import com.aerospike.client.command.ParticleType;

public final class LuaInstance {
	
	private final Globals globals = new Globals();
	private final PackageLib packageLib;
	
	public LuaInstance() throws AerospikeException {
		globals.load(new JseBaseLib());
		packageLib = new PackageLib();
		globals.load(packageLib);
		//globals.load(new Bit32Lib()); // not needed for 5.1 compatibility	
		globals.load(new TableLib());
		globals.load(new StringLib());
		globals.load(new CoroutineLib());
		globals.load(new JseMathLib());
		globals.load(new JseIoLib());
		globals.load(new JseOsLib());
		globals.load(new LuajavaLib());
		globals.load(new DebugLib());
		globals.load(new LuaBytesLib(this));
		globals.load(new LuaListLib(this));
		globals.load(new LuaMapLib(this));
		globals.load(new LuaStreamLib(this));
		LuaC.install();
		globals.compiler = LuaC.instance;

		load("compat52", true);
		load("as", true);
		load("stream_ops", true);
		load("aerospike", true);
		
		globals.load(new LuaAerospikeLib(this));
	}

	public void registerPackage(String packageName, LuaTable table) {
		globals.set(packageName, table);
		packageLib.loaded.set(packageName, table);
	}

	public LuaValue getPackage(String packageName) {
		return globals.get(packageName);
	}
	
	public void load(LibFunction function) {
		globals.load(function);
	}
	
	public void load(String packageName, boolean system) throws AerospikeException {
		if (packageLib.loaded.get(packageName).toboolean()) {
			return;
		}
		
		Prototype prototype = LuaCache.loadPackage(packageName, system);
		LuaClosure function = new LuaClosure(prototype, globals);
		function.invoke();
		
		packageLib.loaded.set(packageName, globals);
	}

	public void call(String functionName, LuaValue[] args) {
		globals.get(functionName).invoke(args);
	}
	
	public LuaValue getFunction(String functionName) {
		return globals.get(functionName);
	}

	public LuaValue getValue(int type, byte[] buf, int offset, int len) throws AerospikeException {
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
		
		case ParticleType.BLOB:
	        byte[] blob = new byte[len];
	        System.arraycopy(buf, offset, blob, 0, len);
			return new LuaBytes(this, blob);
			
		case ParticleType.JBLOB:
			Object object = Buffer.bytesToObject(buf, offset, len);
			return new LuaJavaBlob(object);
			
		case ParticleType.LIST: {
			LuaUnpacker unpacker = new LuaUnpacker(this);
			List<LuaValue> list = unpacker.parseList(buf, offset, len);
			return new LuaList(this, list);
		}

		case ParticleType.MAP: {
			LuaUnpacker unpacker = new LuaUnpacker(this);
			Map<LuaValue,LuaValue> map = unpacker.parseMap(buf, offset, len);
			return new LuaMap(this, map);
		}
		
		default:
			return LuaValue.NIL;
		}
	}
}
