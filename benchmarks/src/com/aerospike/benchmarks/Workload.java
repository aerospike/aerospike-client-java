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
package com.aerospike.benchmarks;

/**
 * Benchmark workload type.
 */
public enum Workload {
	/**
	 * Initialize data with sequential key writes.
	 */
	INITIALIZE,
	
	/**
	 * Read/Update. Perform random key, random read all bins or write all bins workload.
	 */
	READ_UPDATE,
	
	/**
	 * Read/Modify/Update. Perform random key, read all bins, write one bin workload.
	 */
	READ_MODIFY_UPDATE,
	
	/**
	 * Read/Modify/Increment. Perform random key, read all bins, increment one integer bin workload.
	 */
	READ_MODIFY_INCREMENT,
	
	/**
	 * Read/Modify/Decrement. Perform random key, read all bins, decrement one integer bin workload.
	 */	
	READ_MODIFY_DECREMENT,
	
	/**
	 * Read the keys(String/Integer) from the File.Read all bins .
	 */	
	READ_FROM_FILE;
}
