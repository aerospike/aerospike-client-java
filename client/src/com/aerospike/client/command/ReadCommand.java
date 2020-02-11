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
package com.aerospike.client.command;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.zip.DataFormatException;
import java.util.zip.Inflater;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.Key;
import com.aerospike.client.Record;
import com.aerospike.client.ResultCode;
import com.aerospike.client.cluster.Cluster;
import com.aerospike.client.cluster.Connection;
import com.aerospike.client.cluster.Node;
import com.aerospike.client.cluster.Partition;
import com.aerospike.client.policy.Policy;

public class ReadCommand extends SyncCommand {
	protected final Key key;
	protected final Partition partition;
	private final String[] binNames;
	private Record record;

	public ReadCommand(Cluster cluster, Policy policy, Key key) {
		super(cluster, policy);
		this.key = key;
		this.binNames = null;
		this.partition = Partition.read(cluster, policy, key);
	}

	public ReadCommand(Cluster cluster, Policy policy, Key key, String[] binNames) {
		super(cluster, policy);
		this.key = key;
		this.binNames = binNames;
		this.partition = Partition.read(cluster, policy, key);
	}

	public ReadCommand(Cluster cluster, Policy policy, Key key, Partition partition) {
		super(cluster, policy);
		this.key = key;
		this.binNames = null;
		this.partition = partition;
	}

	@Override
	protected Node getNode() {
		return partition.getNodeRead(cluster);
	}

	@Override
	protected void writeBuffer() {
		setRead(policy, key, binNames);
	}

	@Override
	protected void parseResult(Connection conn) throws IOException {
		// Read header.
		conn.readFully(dataBuffer, 8, Command.STATE_READ_HEADER);

		long sz = Buffer.bytesToLong(dataBuffer, 0);
		int receiveSize = (int)(sz & 0xFFFFFFFFFFFFL);

		if (receiveSize <= 0) {
			throw new AerospikeException("Invalid receive size: " + receiveSize);
		}

		/*
		byte version = (byte) (((int)(sz >> 56)) & 0xff);
		if (version != MSG_VERSION) {
			if (Log.debugEnabled()) {
				Log.debug("read header: incorrect version.");
			}
		}

		if (type != MSG_TYPE) {
			if (Log.debugEnabled()) {
				Log.debug("read header: incorrect message type, aborting receive");
			}
		}

		if (headerLength != MSG_REMAINING_HEADER_SIZE) {
			if (Log.debugEnabled()) {
				Log.debug("read header: unexpected header size, aborting");
			}
		}*/

		// Read remaining message bytes.
		sizeBuffer(receiveSize);
		conn.readFully(dataBuffer, receiveSize, Command.STATE_READ_DETAIL);
		conn.updateLastUsed();

		long type = (sz >> 48) & 0xff;

		if (type == Command.AS_MSG_TYPE) {
			dataOffset = 5;
		}
		else if (type == Command.MSG_TYPE_COMPRESSED) {
			int usize = (int)Buffer.bytesToLong(dataBuffer, 0);
			byte[] buf = new byte[usize];

			Inflater inf = new Inflater();
			inf.setInput(dataBuffer, 8, receiveSize - 8);
			int rsize;

			try {
				rsize = inf.inflate(buf);
			}
			catch (DataFormatException dfe) {
				throw new AerospikeException.Serialize(dfe);
			}

			if (rsize != usize) {
				throw new AerospikeException("Decompressed size " + rsize + " is not expected " + usize);
			}

			dataBuffer = buf;
			dataOffset = 13;
		}
		else {
			throw new AerospikeException("Invalid proto type: " + type + " Expected: " + Command.AS_MSG_TYPE);
		}

		int resultCode = dataBuffer[dataOffset] & 0xFF;
		dataOffset++;
		int generation = Buffer.bytesToInt(dataBuffer, dataOffset);
		dataOffset += 4;
		int expiration = Buffer.bytesToInt(dataBuffer, dataOffset);
		dataOffset += 8;
		int fieldCount = Buffer.bytesToShort(dataBuffer, dataOffset);
		dataOffset += 2;
		int opCount = Buffer.bytesToShort(dataBuffer, dataOffset);
		dataOffset += 2;

		if (resultCode == 0) {
		    if (opCount == 0) {
		    	// Bin data was not returned.
		    	record = new Record(null, generation, expiration);
		    	return;
		    }
		    record = parseRecord(opCount, fieldCount, generation, expiration);
			return;
		}

		if (resultCode == ResultCode.KEY_NOT_FOUND_ERROR) {
			handleNotFound(resultCode);
			return;
		}

		if (resultCode == ResultCode.FILTERED_OUT) {
			if (policy.failOnFilteredOut) {
				throw new AerospikeException(resultCode);
			}
			return;
		}

		if (resultCode == ResultCode.UDF_BAD_RESPONSE) {
			record = parseRecord(opCount, fieldCount, generation, expiration);
			handleUdfError(resultCode);
			return;
		}

		throw new AerospikeException(resultCode);
	}

	@Override
	protected boolean prepareRetry(boolean timeout) {
		partition.prepareRetryRead(timeout);
		return true;
	}

	protected void handleNotFound(int resultCode) {
		// Do nothing in default case. Record will be null.
	}

	private void handleUdfError(int resultCode) {
		String ret = (String)record.bins.get("FAILURE");

		if (ret == null) {
	    	throw new AerospikeException(resultCode);
		}

		String message;
		int code;

		try {
			String[] list = ret.split(":");
			code = Integer.parseInt(list[2].trim());
			message = list[0] + ':' + list[1] + ' ' + list[3];
		}
		catch (Exception e) {
			// Use generic exception if parse error occurs.
        	throw new AerospikeException(resultCode, ret);
		}

		throw new AerospikeException(code, message);
	}

	private final Record parseRecord(
		int opCount,
		int fieldCount,
		int generation,
		int expiration
	)  {
		Map<String,Object> bins = new HashMap<String,Object>();

		// There can be fields in the response (setname etc).
		// But for now, ignore them. Expose them to the API if needed in the future.
		if (fieldCount > 0) {
			// Just skip over all the fields
			for (int i = 0; i < fieldCount; i++) {
				int fieldSize = Buffer.bytesToInt(dataBuffer, dataOffset);
				dataOffset += 4 + fieldSize;
			}
		}

		for (int i = 0 ; i < opCount; i++) {
			int opSize = Buffer.bytesToInt(dataBuffer, dataOffset);
			byte particleType = dataBuffer[dataOffset + 5];
			byte nameSize = dataBuffer[dataOffset + 7];
			String name = Buffer.utf8ToString(dataBuffer, dataOffset + 8, nameSize);
			dataOffset += 4 + 4 + nameSize;

			int particleBytesSize = opSize - (4 + nameSize);
	        Object value = Buffer.bytesToParticle(particleType, dataBuffer, dataOffset, particleBytesSize);
	        dataOffset += particleBytesSize;
			addBin(bins, name, value);
	    }
	    return new Record(bins, generation, expiration);
	}

    /*
     * The function represents mutability on the Record.
     * The function is overridden in other commands to change
     * the behaviour of adding/modifying key to the map
     * @see com.aerospike.client.command.OperateCommand
     */
    protected void addBin(Map<String, Object> bins, String name, Object value) {
        bins.put(name, value);
    }

	public Record getRecord() {
		return record;
	}
}
