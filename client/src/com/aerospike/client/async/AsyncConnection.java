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
package com.aerospike.client.async;

import java.nio.ByteBuffer;

/**
 * Async connection interface.
 */
public interface AsyncConnection {
	/**
	 * Is connection ready for another command.
	 */
	public boolean isValid(ByteBuffer byteBuffer);

	/**
	 * Is connection idle time less than or equal to
	 * {@link com.aerospike.client.policy.ClientPolicy#maxSocketIdle}.
	 */
	public boolean isCurrent();

	/**
	 * Close connection.
	 */
	public void close();
}
