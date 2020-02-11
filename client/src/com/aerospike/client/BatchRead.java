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
package com.aerospike.client;

import java.util.Arrays;

/**
 * Key and bin names used in batch read commands where variables bins are needed for each key.
 */
public final class BatchRead {
	/**
	 * Key.
	 */
	public final Key key;

	/**
	 * Bins to retrieve for this key.
	 */
	public final String[] binNames;

	/**
	 * If true, ignore binNames and read all bins.
	 * If false and binNames are set, read specified binNames.
	 * If false and binNames are not set, read record header (generation, expiration) only.
	 */
	public final boolean readAllBins;

	/**
	 * Record result after batch command has completed.  Will be null if record was not found.
	 */
	public Record record;

	/**
	 * Initialize batch key and bins to retrieve.
	 *
	 * @param key					record key
	 * @param binNames				array of bins to retrieve.
	 */
	public BatchRead(Key key, String[] binNames) {
		this.key = key;
		this.binNames = binNames;
		this.readAllBins = false;
	}

	/**
	 * Initialize batch key and readAllBins indicator.
	 *
	 * @param key					record key
	 * @param readAllBins			should all bins in record be retrieved.
	 */
	public BatchRead(Key key, boolean readAllBins) {
		this.key = key;
		this.binNames = null;
		this.readAllBins = readAllBins;
	}

	/**
	 * Convert BatchRead to string.
	 */
	@Override
	public String toString() {
		return key.toString() + ":" + Arrays.toString(binNames);
	}
}
