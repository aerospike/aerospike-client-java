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

	/**
	 * Return lua instance from a pool.  If a lua instance is not available,
	 * a new instance will be created.
	 */
	public static final LuaInstance getInstance() throws AerospikeException {
		LuaInstance instance = InstanceQueue.poll();
		return (instance != null)? instance : new LuaInstance();
	}

	/**
	 * Put lua instance back into pool if pool size (LuaConfig.InstancePoolSize)
	 * would not be exceeded.
	 */
	public static final void putInstance(LuaInstance instance) {
		InstanceQueue.offer(instance);
	}

	/**
	 * Load lua package from a file.
	 */
	public static final Prototype loadPackageFromFile(String packageName) throws AerospikeException {
		Prototype prototype = Packages.get(packageName);

		if (prototype == null) {
			File source = new File(LuaConfig.SourceDirectory, packageName + ".lua");

			try {
				InputStream is = new FileInputStream(source);
				prototype = compile(packageName, is);
				Packages.put(packageName, prototype);
			}
			catch (Exception e) {
				throw new AerospikeException("Failed to read file: " + source.getAbsolutePath());
			}
		}
		return prototype;
	}

	/**
	 * Load lua package from a resource.
	 */
	public static final Prototype loadPackageFromResource(ClassLoader resourceLoader, String resourcePath, String packageName) throws AerospikeException {
		Prototype prototype = Packages.get(packageName);

		if (prototype == null) {
			try {
				InputStream is = resourceLoader.getResourceAsStream(resourcePath);

				if (is == null) {
					throw new Exception();
				}
				prototype = compile(packageName, is);
				Packages.put(packageName, prototype);
			}
			catch (Exception e) {
				throw new AerospikeException("Failed to read resource: " + resourcePath);
			}
		}
		return prototype;
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

	/**
	 * Remove lua package from cache and unload package from non-active
	 * lua instances.  This method assumes lua instances are not running
	 * concurrently with this call. Active lua instances will not have
	 * their packages unloaded.
	 */
	public static final void clearPackage(String packageName) {
		Prototype prototype = Packages.remove(packageName);

		if (prototype != null) {
			for (final LuaInstance luaInstance : InstanceQueue){
				luaInstance.unloadPackage(packageName);
			}
		}
	}

	/**
	 * Remove all lua packages and non-active lua instances from cache.
	 * This method assumes lua instances are not running concurrently
	 * with this call.  Active lua instances will not be removed.
	 */
	public static final void clearPackages() {
		InstanceQueue.clear();
		Packages.clear();
	}
}
