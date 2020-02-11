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
package com.aerospike.benchmarks;

public enum TransactionalType {
	SINGLE_BIN_READ('r', true),
	SINGLE_BIN_BATCH_READ('b', true, true),
	SINGLE_BIN_UPDATE('u', false),
	SINGLE_BIN_REPLACE('p', false),
	SINGLE_BIN_INCREMENT('i', false),
	SINGLE_BIN_WRITE('w', false),		// either an update or a replace
	MULTI_BIN_READ('R', true),
	MULTI_BIN_BATCH_READ('B', true, true),
	MULTI_BIN_UPDATE('U', false),
	MULTI_BIN_REPLACE('P', false),
	MULTI_BIN_WRITE('W', false);

	private char code;
	private boolean read;
	private boolean batch;
	private TransactionalType(char code, boolean isRead, boolean isBatch) {
		this.code = code;
		this.read = isRead;
		this.batch = isBatch;
	}
	private TransactionalType(char code, boolean isRead) {
		this(code, isRead, false);
	}

	public char getCode() {
		return code;
	}

	public boolean isRead() {
		return this.read;
	}

	public boolean isBatch() {
		return batch;
	}

	public static TransactionalType lookupCode(char code) {
		for (TransactionalType thisItem : TransactionalType.values()) {
			if (thisItem.code == code) {
				return thisItem;
			}
		}
		return null;
	}
}
