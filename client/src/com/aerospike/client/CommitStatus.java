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
package com.aerospike.client;

/**
 * Multi-record transaction (MRT) commit status code.
 */
public enum CommitStatus {
	OK("Commit succeeded"),
	ALREADY_COMMITTED("Already committed"),
	ALREADY_ABORTED("Already aborted"),
	ROLL_FORWARD_ABANDONED("MRT client roll forward abandoned. Server will eventually commit the MRT."),
	CLOSE_ABANDONED("MRT has been rolled forward, but MRT client close was abandoned. Server will eventually close the MRT.");

	public final String str;

	CommitStatus(String str) {
		this.str = str;
	}
}
