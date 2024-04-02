/*
 * Copyright 2012-2024 Aerospike, Inc.
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
package com.aerospike.client.tran;

/**
 * Transaction version entry for a key.
 */
public final class TranVersion {
	private long version;
	private boolean read;
	private boolean write;

	/**
	 * Construct transaction version.
	 */
	public TranVersion(long version, boolean read, boolean write) {
		this.version = version;
		this.read = read;
		this.write = write;
	}

	/**
	 * Signify that a read has occurred with the given version.
	 */
	public void setRead(long version) {
		this.version = version;
		this.read = true;
	}

	/**
	 * Signify that a write has occurred with the given version.
	 */
	public void setWrite(long version) {
		this.version = version;
		this.write = true;
	}

	/**
	 * Return version.
	 */
	public long getVersion() {
		return version;
	}
}
