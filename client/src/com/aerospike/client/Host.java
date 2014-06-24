/* 
 * Copyright 2012-2014 Aerospike, Inc.
 *
 * Portions may be licensed to Aerospike, Inc. under one or more contributor
 * license agreements.
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
package com.aerospike.client;

/**
 * Host name/port of database server. 
 */
public final class Host {
	/**
	 * Host name or IP address of database server.
	 */
	public final String name;
	
	/**
	 * Port of database server.
	 */
	public final int port;
	
	/**
	 * Initialize host.
	 */
	public Host(String name, int port) {
		this.name = name;
		this.port = port;
	}

	@Override
	public String toString() {
		return name + ':' + port;
	}
	
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = prime + name.hashCode();
		return prime * result + port;
	}

	@Override
	public boolean equals(Object obj) {
		Host other = (Host) obj;
		return this.name.equals(other.name) && this.port == other.port;
	}
}
