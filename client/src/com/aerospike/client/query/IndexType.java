/*
 * Copyright 2012-2023 Aerospike, Inc.
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
package com.aerospike.client.query;

/**
 * Underlying data type of secondary index.
 */
public enum IndexType {
	/**
	 * Number index. Use {@link #INTEGER} for server versions 8.1.3+.
	 */
	NUMERIC,

	/**
	 * String index.
	 */
	STRING,

	/**
	 * byte[] index. Requires server version 7.0+.
	 */
	BLOB,

	/**
	 * 2-dimensional spherical geospatial index.
	 */
	GEO2DSPHERE,

	/**
	 * Integer index. Requires server version 8.1.3+. Use {@link #NUMERIC} for
	 * server versions prior to 8.1.3.
	 */
	INTEGER;
}
