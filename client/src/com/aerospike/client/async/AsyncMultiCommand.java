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
package com.aerospike.client.async;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.Key;
import com.aerospike.client.Record;
import com.aerospike.client.ResultCode;
import com.aerospike.client.command.Buffer;
import com.aerospike.client.command.Command;
import com.aerospike.client.command.FieldType;

public abstract class AsyncMultiCommand extends AsyncCommand {
	
	private final AsyncMultiExecutor parent;
	private final AsyncNode node;
	protected final HashSet<String> binNames;
	protected byte[] receiveBuffer;
	protected int receiveSize;
	protected int receiveOffset;
	protected int resultCode;
	protected int generation;
	protected int expiration;
	protected int fieldCount;
	protected int opCount;
	private final boolean stopOnNotFound;
		
	public AsyncMultiCommand(AsyncMultiExecutor parent, AsyncCluster cluster, AsyncNode node, boolean stopOnNotFound) {
		super(cluster);
		this.parent = parent;
		this.node = node;
		this.stopOnNotFound = stopOnNotFound;
		this.binNames = null;
	}

	public AsyncMultiCommand(AsyncMultiExecutor parent, AsyncCluster cluster, AsyncNode node, boolean stopOnNotFound, HashSet<String> binNames) {
		super(cluster);
		this.parent = parent;
		this.node = node;
		this.stopOnNotFound = stopOnNotFound;
		this.binNames = binNames;
	}

	protected final AsyncNode getNode() {	
		return node;
	}

	protected final void read() throws AerospikeException, IOException {		
		while (true) {
			if (inHeader) {
				if (! conn.read(byteBuffer)) {
					return;
				}
	
				byteBuffer.position(0);
				receiveSize = ((int) (byteBuffer.getLong() & 0xFFFFFFFFFFFFL));
				
		        if (receiveSize <= 0) {
					finish();
					return;
		        }
		        
		        if (receiveBuffer == null || receiveSize > receiveBuffer.length) {
		        	receiveBuffer = new byte[receiveSize];
		        }
				byteBuffer.clear();
	
				if (receiveSize < byteBuffer.capacity()) {
					byteBuffer.limit(receiveSize);
				}
				inHeader = false;
			}
	
			if (! conn.read(byteBuffer)) {
				return;
			}
	
			// Copy byteBuffer to byte[].
			byteBuffer.position(0);
			byteBuffer.get(receiveBuffer, receiveOffset, byteBuffer.limit());
			receiveOffset += byteBuffer.limit();
			byteBuffer.clear();
			
			if (receiveOffset >= receiveSize) {
				if (parseGroup()) {
					finish();
					return;
				}
				// Prepare for next group.
				byteBuffer.limit(8);
				receiveOffset = 0;
				inHeader = true;
			}
			else {
				int remaining = receiveSize - receiveOffset;
					
				if (remaining < byteBuffer.capacity()) {
					byteBuffer.limit(remaining);
				}
			}
		}
	}
		
	private final boolean parseGroup() throws AerospikeException {
		// Parse each message response and add it to the result array
		receiveOffset = 0;
		
		while (receiveOffset < receiveSize) {
			resultCode = receiveBuffer[receiveOffset + 5] & 0xFF;

			if (resultCode != 0) {
				if (resultCode == ResultCode.KEY_NOT_FOUND_ERROR) {
					if (stopOnNotFound) {
						return true;
					}
				}
				else {
					throw new AerospikeException(resultCode);
				}
			}

			// If this is the end marker of the response, do not proceed further
			if ((receiveBuffer[receiveOffset + 3] & Command.INFO3_LAST) != 0) {
				return true;
			}			
			generation = Buffer.bytesToInt(receiveBuffer, receiveOffset + 6);
			expiration = Buffer.bytesToInt(receiveBuffer, receiveOffset + 10);
			fieldCount = Buffer.bytesToShort(receiveBuffer, receiveOffset + 18);
			opCount = Buffer.bytesToShort(receiveBuffer, receiveOffset + 20);

			receiveOffset += Command.MSG_REMAINING_HEADER_SIZE;
			
			Key key = parseKey();
			parseRow(key);
		}
		return false;
	}

	protected final Key parseKey() {
		byte[] digest = null;
		String namespace = null;
		String setName = null;
		
		for (int i = 0; i < fieldCount; i++) {
			int fieldlen = Buffer.bytesToInt(receiveBuffer, receiveOffset);
			receiveOffset += 4;
			
			int fieldtype = receiveBuffer[receiveOffset++];
			int size = fieldlen - 1;
			
			if (fieldtype == FieldType.DIGEST_RIPE) {
				digest = new byte[size];
				System.arraycopy(receiveBuffer, receiveOffset, digest, 0, size);
				receiveOffset += size;
			}
			else if (fieldtype == FieldType.NAMESPACE) {
				namespace = new String(receiveBuffer, receiveOffset, size);
				receiveOffset += size;
			}				
			else if (fieldtype == FieldType.TABLE) {
				setName = new String(receiveBuffer, receiveOffset, size);
				receiveOffset += size;
			}				
		}
		return new Key(namespace, digest, setName);		
	}
	
	protected Record parseRecordWithDuplicates() throws AerospikeException {
		
		Map<String,Object> bins = null;
		ArrayList<Map<String, Object>> duplicates = null;
		
		for (int i = 0 ; i < opCount; i++) {
			int opSize = Buffer.bytesToInt(receiveBuffer, receiveOffset);
			byte particleType = receiveBuffer[receiveOffset+5];
			byte version = receiveBuffer[receiveOffset+6];
			byte nameSize = receiveBuffer[receiveOffset+7];
			String name = Buffer.utf8ToString(receiveBuffer, receiveOffset+8, nameSize);
			receiveOffset += 4 + 4 + nameSize;
	
			int particleBytesSize = (int) (opSize - (4 + nameSize));
	        Object value = Buffer.bytesToParticle(particleType, receiveBuffer, receiveOffset, particleBytesSize);
			receiveOffset += particleBytesSize;
	
			// Currently, the batch command returns all the bins even if a subset of
			// the bins are requested. We have to filter it on the client side.
			// TODO: Filter batch bins on server!
			if (binNames == null || binNames.contains(name)) {
				Map<String,Object> vmap = null;
				
				if (version > 0 || duplicates != null) {
					if (duplicates == null) {
						duplicates = new ArrayList<Map<String,Object>>(4);
						duplicates.add(bins);
						bins = null;
						
						for (int j = 0; j < version; j++) {
							duplicates.add(null);
						}
					} 
					else {
						for (int j = duplicates.size(); j < version + 1; j++) 
							duplicates.add(null);
					}
		
					vmap = duplicates.get(version);
					if (vmap == null) {
						vmap = new HashMap<String,Object>();
						duplicates.set(version, vmap);
					}
				}
				else {
					if (bins == null) {
						bins = new HashMap<String,Object>();
					}
					vmap = bins;
				}
				vmap.put(name, value);
			}
	    }
	
	    // Remove null duplicates just in case there were holes in the version number space.
	    if (duplicates != null) {
	        while (duplicates.remove(null)) {
	        	;
	        }
	    }
	    return new Record(bins, duplicates, generation, expiration);	    
	}

	protected Record parseRecord() throws AerospikeException {		
		Map<String,Object> bins = null;
		
		for (int i = 0 ; i < opCount; i++) {
			int opSize = Buffer.bytesToInt(receiveBuffer, receiveOffset);
			byte particleType = receiveBuffer[receiveOffset+5];
			byte nameSize = receiveBuffer[receiveOffset+7];
			String name = Buffer.utf8ToString(receiveBuffer, receiveOffset+8, nameSize);
			receiveOffset += 4 + 4 + nameSize;
	
			int particleBytesSize = (int) (opSize - (4 + nameSize));
	        Object value = Buffer.bytesToParticle(particleType, receiveBuffer, receiveOffset, particleBytesSize);
			receiveOffset += particleBytesSize;

			if (bins == null) {
				bins = new HashMap<String,Object>();
			}
			bins.put(name, value);
	    }
	    return new Record(bins, null, generation, expiration);	    
	}
	
	@Override
	protected void onSuccess() {
		parent.childSuccess();
	}

	@Override
	protected void onFailure(AerospikeException e) {
		parent.childFailure(e);
	}

	protected abstract void parseRow(Key key) throws AerospikeException;
}
