/*
 * Copyright 2012-2018 Aerospike, Inc.
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

import java.io.IOException;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.Key;
import com.aerospike.client.command.Buffer;
import com.aerospike.client.command.MultiCommand;
import com.aerospike.client.policy.WritePolicy;

public final class ServerCommand extends MultiCommand {
	private final WritePolicy writePolicy;
	private final Statement statement;
	
	public ServerCommand(WritePolicy policy, Statement statement) {
		super(true);
		this.writePolicy = policy;
		this.statement = statement;
	}
	
	@Override
	protected final void writeBuffer() throws AerospikeException {
		setQuery(writePolicy, statement, true);
	}	
	
	@Override
	protected void parseRow(Key key) throws IOException {		
		// Server commands (Query/Execute UDF) should only send back a return code.
		// Keep parsing logic to empty socket buffer just in case server does
		// send records back.
		for (int i = 0 ; i < opCount; i++) {
    		readBytes(8);	
			int opSize = Buffer.bytesToInt(dataBuffer, 0);
			byte nameSize = dataBuffer[7];
    		
			readBytes(nameSize);
	
			int particleBytesSize = (int) (opSize - (4 + nameSize));
			readBytes(particleBytesSize);
	    }
		
		if (! valid) {
			throw new AerospikeException.QueryTerminated();
		}
	}
}
