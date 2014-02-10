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

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentHashMap;

import org.luaj.vm2.Prototype;
import org.luaj.vm2.compiler.LuaC;

import com.aerospike.client.AerospikeException;

public final class LuaCache {
	private static final ArrayBlockingQueue<LuaInstance> InstanceQueue = new ArrayBlockingQueue<LuaInstance>(LuaConfig.InstancePoolSize);
	private static final ConcurrentHashMap<String,Prototype> Packages = new ConcurrentHashMap<String,Prototype>();
	
	public static final LuaInstance getInstance() throws AerospikeException {
		LuaInstance instance = InstanceQueue.poll();
		return (instance != null)? instance : new LuaInstance();
	}
			
	public static final void putInstance(LuaInstance instance) {
		InstanceQueue.offer(instance);
	}
	
	public static final Prototype loadPackage(String packageName, boolean system) throws AerospikeException {
		Prototype prototype = Packages.get(packageName);
		
		if (prototype == null) {
			InputStream is = getInputStream(packageName, system);
			prototype = compile(packageName, is);
			Packages.put(packageName, prototype);
		}
		return prototype;
	}
	
	private static InputStream getInputStream(String packageName, boolean system) throws AerospikeException {
		if (system) {
			return getSystemStream(packageName);
		}
		else {
			return getUserStream(packageName);
		}
	}

	private static InputStream getSystemStream(String packageName) throws AerospikeException {
		String path = "udf/" + packageName + ".lua";
		
		try {
			InputStream is = ClassLoader.getSystemResourceAsStream(path);
			
			if (is == null) {
				throw new Exception();
			}
			return is;
		}
		catch (Exception e) {
			throw new AerospikeException("Failed to read resource: " + path);
		}
	}
	
	private static InputStream getUserStream(String packageName) throws AerospikeException {
		File source = new File(LuaConfig.SourceDirectory, packageName + ".lua");
		
		try {
			return new FileInputStream(source);
		}
		catch (Exception e) {
			throw new AerospikeException("Failed to read file: " + source.getAbsolutePath());
		}
	}
	
	private static Prototype compile(String packageName, InputStream is) throws AerospikeException {
		try {
			BufferedInputStream bis = new BufferedInputStream(is);
			
			try {
		        return LuaC.instance.compile(bis, packageName);
			}
			finally {
				is.close();
			}
		}
		catch (Exception e) {
			throw new AerospikeException("Failed to compile: " + packageName);
		}
	}

	public static final void clearPackages() {
		Packages.clear();
	}
}
