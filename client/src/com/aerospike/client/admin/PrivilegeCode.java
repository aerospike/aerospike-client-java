/* 
 * Copyright 2012-2015 Aerospike, Inc.
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
package com.aerospike.client.admin;

import com.aerospike.client.AerospikeException;

/**
 * Permission codes define the type of permission granted for a user's role.
 */
public enum PrivilegeCode {
	/**
	 * User can edit/remove other users.  Global scope only.
	 */
	USER_ADMIN(0),
	
	/**
	 * User can perform systems administration functions on a database that do not involve user
	 * administration.  Global scope only.
	 */
	SYS_ADMIN(1),
	
	/**
	 * User can read data only.
	 */
	READ(10),
	
	/**
	 * User can read and write data.
	 */
	READ_WRITE(11),
	
	/**
	 * User can read and write data through user defined functions.
	 */
	READ_WRITE_UDF(12);
	
	/**
	 * Privilege code ID used in wire protocol.
	 */
	public final int id;
	
	private PrivilegeCode(int id) {
		this.id = id;
	}
	
	/**
	 * Can privilege be scoped with namespace and set.
	 */
	public boolean canScope() {
		return id >= 10;
	}
	
	/**
	 * Convert ID to privilege code enum.
	 */
	public static PrivilegeCode fromId(int id) {
		switch (id) {
		case 0:
			return USER_ADMIN;
		
		case 1:
			return SYS_ADMIN;
		
		case 10:
			return READ;
						
		case 11:
			return READ_WRITE;
			
		case 12:
			return READ_WRITE_UDF;
			
		default:
			throw new AerospikeException("Invalid privilege code: " + id);
		}
	}
}
