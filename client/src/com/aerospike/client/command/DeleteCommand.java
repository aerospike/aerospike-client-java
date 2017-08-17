/*
 * Copyright 2012-2017 Aerospike, Inc.
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
package com.aerospike.client.command;

import java.io.IOException;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.Key;
import com.aerospike.client.ResultCode;
import com.aerospike.client.cluster.Connection;
import com.aerospike.client.policy.WritePolicy;

public final class DeleteCommand extends SyncCommand {
	private final WritePolicy policy;
	private final Key key;
	private boolean existed;

	public DeleteCommand(WritePolicy policy, Key key) {
		this.policy = policy;
		this.key = key;
	}

	@Override
	protected void writeBuffer() {
		setDelete(policy, key);
	}

	@Override
	protected void parseResult(Connection conn) throws IOException {
		// Read header.		
		conn.readFully(dataBuffer, MSG_TOTAL_HEADER_SIZE);
		int resultCode = dataBuffer[13] & 0xFF;
	
	    if (resultCode != 0 && resultCode != ResultCode.KEY_NOT_FOUND_ERROR) {
	    	throw new AerospikeException(resultCode);        	
	    }        	
		existed = resultCode == 0;
		emptySocket(conn);
	}
	
	public boolean existed() {
		return existed;
	}
}
