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

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteOrder;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentHashMap;

import org.luaj.vm2.LoadState;
import org.luaj.vm2.LuaString;
import org.luaj.vm2.Prototype;
import org.luaj.vm2.compiler.DumpState;
import org.luaj.vm2.compiler.LuaC;

public final class LuaCache {
	private static final ArrayBlockingQueue<LuaInstance> InstanceQueue = new ArrayBlockingQueue<LuaInstance>(LuaConfig.InstancePoolSize);
	private static final ConcurrentHashMap<String,Prototype> Packages = new ConcurrentHashMap<String,Prototype>();
	
	public static final LuaInstance getInstance() throws IOException {
		LuaInstance instance = InstanceQueue.poll();
		return (instance != null)? instance : new LuaInstance();
	}
			
	public static final void putInstance(LuaInstance instance) {
		InstanceQueue.offer(instance);
	}
	
	public static final Prototype loadPackage(String packageName) throws IOException {
		Prototype prototype = Packages.get(packageName);
		
		if (prototype == null) {
			prototype = loadOrCompilePackage(packageName);
			Packages.put(packageName, prototype);
		}
		return prototype;
	}
	
	private static Prototype loadOrCompilePackage(String packageName) throws IOException {
		File source = new File(LuaConfig.SourceDirectory, packageName + ".lua");
		File target = new File(LuaConfig.SourceDirectory, packageName + ".out");
		long sourceLastModified = source.lastModified();			
				
		if (sourceLastModified > target.lastModified()) {
			return compile(packageName, source, target);
		}
		else {
			return loadCompiled(target, packageName);
		}
	}

	private static Prototype compile(String packageName, File source, File target) throws IOException {
        boolean littleEndian = ByteOrder.nativeOrder().equals(ByteOrder.LITTLE_ENDIAN);
		BufferedInputStream is = new BufferedInputStream(new FileInputStream(source));
		
		try {
	        Prototype prototype = LuaC.compile(is, packageName);
			BufferedOutputStream os = new BufferedOutputStream(new FileOutputStream(target));
			
			try {
		        DumpState.dump(prototype, os, true, DumpState.NUMBER_FORMAT_DEFAULT, littleEndian);
			}
			finally {
				os.close();
			}
			return prototype;
		}
		finally {
			is.close();
		}
	}

	private static Prototype loadCompiled(File file, String packageName) throws IOException {	
		BufferedInputStream is = new BufferedInputStream(new FileInputStream(file));
		
		try {
			int firstByte = is.read();
			
			if ( firstByte != '\033')
				throw new IOException("Invalid lua byte code");
			
			Prototype prototype = LoadState.loadBinaryChunk(firstByte, is, packageName);
	        prototype.source = LuaString.valueOf(packageName);
			return prototype;
		}
		finally {
			is.close();
		}
	}
	
	public static final void clearPackages() {
		Packages.clear();
	}
}
