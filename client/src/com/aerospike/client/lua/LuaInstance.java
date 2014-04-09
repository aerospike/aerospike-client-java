/*******************************************************************************
 * Copyright 2012-2014 by Aerospike.
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to
 * deal in the Software without restriction, including without limitation the
 * rights to use, copy, modify, merge, publish, distribute, sublicense, and/or
 * sell copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
 * FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS
 * IN THE SOFTWARE.
 ******************************************************************************/
package com.aerospike.client.lua;

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

		load("compat52", true);
		load("as", true);
		load("stream_ops", true);
		load("aerospike", true);
		
		globals.load(new LuaAerospikeLib(this));
	}

	public void registerPackage(String packageName, LuaTable table) {
		globals.set(packageName, table);		
		loadedTable.set(packageName, LuaValue.TRUE);
	}

	public LuaValue getPackage(String packageName) {
		return globals.get(packageName);
	}
	
	public void load(LibFunction function) {
		globals.load(function);
	}
	
	public void load(String packageName, boolean system) throws AerospikeException {
		if (loadedTable.get(packageName).toboolean()) {
			return;
		}
		
		Prototype prototype = LuaCache.loadPackage(packageName, system);
		LuaClosure function = new LuaClosure(prototype, globals);
		function.invoke();
		
		loadedTable.set(packageName, LuaValue.TRUE);
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
			LuaUnpacker unpacker = new LuaUnpacker(this, buf, offset, len);
			return unpacker.unpackList();
		}

		case ParticleType.MAP: {
			LuaUnpacker unpacker = new LuaUnpacker(this, buf, offset, len);
			return unpacker.unpackMap();
		}
		
		default:
			return LuaValue.NIL;
		}
	}
}
