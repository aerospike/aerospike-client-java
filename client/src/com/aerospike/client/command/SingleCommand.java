/*
 * Aerospike Client - Java Library
 *
 * Copyright 2012 by Aerospike, Inc. All rights reserved.
 *
 * Availability of this source code to partners and customers includes
 * redistribution rights covered by individual contract. Please check your
 * contract for exact rights and responsibilities.
 */
package com.aerospike.client.command;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.Bin;
import com.aerospike.client.Key;
import com.aerospike.client.Operation;
import com.aerospike.client.Record;
import com.aerospike.client.ResultCode;
import com.aerospike.client.cluster.Cluster;
import com.aerospike.client.cluster.Node;
import com.aerospike.client.cluster.Partition;
import com.aerospike.client.policy.WritePolicy;
import com.aerospike.client.util.ThreadLocalData;

public final class SingleCommand extends Command {
	private final Cluster cluster;
	private final Key key;
	private final Partition partition;
	private Record record;
	private int readAttr;
	private int writeAttr;
	private int fieldCount;
	private int resultCode;

	public SingleCommand(Cluster cluster, Key key) {
		this.cluster = cluster;
		this.key = key;
		this.partition = new Partition(key);
		this.sendBuffer = ThreadLocalData.getSendBuffer();
		this.receiveBuffer = ThreadLocalData.getReceiveBuffer();
		estimateKeySize();
	}
	
	public void setRead(int readAttr) {
		this.readAttr = readAttr;
	}

	public void setWrite(int writeAttr) {
		this.writeAttr = writeAttr;
	}
		
	public void estimateKeySize() {
		if (key.namespace != null) {
			sendOffset += Buffer.estimateSizeUtf8(key.namespace) + FIELD_HEADER_SIZE;
			fieldCount++;
		}
		
		if (key.setName != null) {
			sendOffset += Buffer.estimateSizeUtf8(key.setName) + FIELD_HEADER_SIZE;
			fieldCount++;
		}
		
		sendOffset += key.digest.length + FIELD_HEADER_SIZE;
		fieldCount++;
	}

	public void estimateUdfSize(String fileName, String functionName, byte[] bytes) {
		sendOffset += Buffer.estimateSizeUtf8(fileName) + FIELD_HEADER_SIZE;
		fieldCount++;
		
		sendOffset += Buffer.estimateSizeUtf8(functionName) + FIELD_HEADER_SIZE;
		fieldCount++;
		
		sendOffset += bytes.length;
		fieldCount++;
	}

	public void write(WritePolicy policy, Operation.Type operation, Bin[] bins) throws AerospikeException {				
		for (Bin bin : bins) {
			estimateOperationSize(bin);
		}
		writeAttr = Command.INFO2_WRITE;
		begin();
		writeHeader(policy, bins.length);
		writeKey();
				
		for (Bin bin : bins) {
			writeOperation(bin, operation);
		}
		execute(policy);
	}

	public void begin() {
		if (sendOffset > sendBuffer.length) {
			sendBuffer = ThreadLocalData.resizeSendBuffer(sendOffset);
		}
	}
		
	public void resizeReceiveBuffer(int size) {
		if (size > receiveBuffer.length) {
			receiveBuffer = ThreadLocalData.resizeReceiveBuffer(size);
		}
	}

	/**
	 * Header write for write operations.
	 */
	public final void writeHeader(WritePolicy policy, int operationCount) 
		throws AerospikeException {		   			
        // Set flags.
		int generation = 0;
		int expiration = 0;
    	
    	if (policy != null) {
    		switch (policy.recordExistsAction) {
    		case UPDATE:
    			break;
    		case EXPECT_GEN_EQUAL:
        		generation = policy.generation;    			
        		writeAttr |= INFO2_GENERATION;
    			break;
    		case EXPECT_GEN_GT:
        		generation = policy.generation;    			
        		writeAttr |= INFO2_GENERATION_GT;
    			break;
    		case FAIL:
        		writeAttr |= INFO2_WRITE_UNIQUE;
    			break;
    		case DUPLICATE:
        		generation = policy.generation;    			
        		writeAttr |= INFO2_GENERATION_DUP;
    			break;
    		}
    		expiration = policy.expiration;
    	}
		// Write all header data except total size which must be written last. 
		sendBuffer[8]  = MSG_REMAINING_HEADER_SIZE; // Message header length.
		sendBuffer[9]  = (byte)readAttr;
		sendBuffer[10] = (byte)writeAttr;
		sendBuffer[11] = 0; // info3
		sendBuffer[12] = 0; // unused
		sendBuffer[13] = 0; // clear the result code
		Buffer.intToBytes(generation, sendBuffer, 14);
		Buffer.intToBytes(expiration, sendBuffer, 18);		
		
		// Initialize timeout. It will be written later.
		sendBuffer[22] = 0;
		sendBuffer[23] = 0;
		sendBuffer[24] = 0;
		sendBuffer[25] = 0;
		
		Buffer.shortToBytes(fieldCount, sendBuffer, 26);
		Buffer.shortToBytes(operationCount, sendBuffer, 28);		
		sendOffset = MSG_TOTAL_HEADER_SIZE;
	}
	
	/**
	 * Generic header write.
	 */
	public void writeHeader(int operationCount) throws AerospikeException {		
		// Write all header data except total size which must be written last. 
		sendBuffer[8] = MSG_REMAINING_HEADER_SIZE; // Message header length.
		sendBuffer[9] = (byte)readAttr;
		sendBuffer[10] = (byte)writeAttr;
		
		for (int i = 11; i < 26; i++) {
			sendBuffer[i] = 0;
		}
		Buffer.shortToBytes(fieldCount, sendBuffer, 26);
		Buffer.shortToBytes(operationCount, sendBuffer, 28);
		sendOffset = MSG_TOTAL_HEADER_SIZE;
	}

	public void writeKey() throws AerospikeException {
		// Write key into buffer.
		if (key.namespace != null) {
			writeField(key.namespace, FieldType.NAMESPACE);
		}
		
		if (key.setName != null) {
			writeField(key.setName, FieldType.TABLE);
		}
	
		writeField(key.digest, FieldType.DIGEST_RIPE);
	}
	
	protected Node getNode() throws AerospikeException.InvalidNode { 
		return cluster.getNode(partition);
	}

	protected void parseResult(InputStream is) throws AerospikeException, IOException {
		// Read header.		
		readFully(is, receiveBuffer, MSG_TOTAL_HEADER_SIZE);
	
        // A number of these are commented out because we just don't care enough to read
        // that section of the header. If we do care, uncomment and check!        
		long sz = Buffer.bytesToLong(receiveBuffer, 0);
		byte headerLength = receiveBuffer[8];
//		byte info1 = header[9];
//		byte info2 = header[10];
//      byte info3 = header[11];
//      byte unused = header[12];
		resultCode = receiveBuffer[13];
		int generation = Buffer.bytesToInt(receiveBuffer, 14);
		int expiration = Buffer.bytesToInt(receiveBuffer, 18);
//		int transactionTtl = get_ntohl(header, 22);
		int fieldCount = Buffer.bytesToShort(receiveBuffer, 26); // almost certainly 0
		int opCount = Buffer.bytesToShort(receiveBuffer, 28);
		int receiveSize = ((int) (sz & 0xFFFFFFFFFFFFL)) - headerLength;
		/*
		byte version = (byte) (((int)(sz >> 56)) & 0xff);
		if (version != MSG_VERSION) {
			if (Log.debugEnabled()) {
				Log.debug("read header: incorrect version.");
			}
		}
		
		byte type = (byte) (((int)(sz >> 48)) & 0xff);
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
        if (receiveSize > 0) {
        	resizeReceiveBuffer(receiveSize);
    		readFully(is, receiveBuffer, receiveSize);
        }
        
        if (readAttr != 0 || opCount > 0) {
            if (resultCode != 0) {       	
            	if (resultCode == ResultCode.KEY_NOT_FOUND_ERROR) {
            		return;
            	}
            	throw new AerospikeException(resultCode);        	
            }
                      
            if (opCount == 0) {
            	// Bin data was not returned.
            	record = new Record(null, null, generation, expiration);
            	return;
            }
            record = parseRecord(opCount, fieldCount, generation, expiration);            
        }
        else {
            if (resultCode != 0) {
            	if (resultCode == ResultCode.KEY_NOT_FOUND_ERROR && (writeAttr & Command.INFO2_DELETE) != 0) {
            		return;
            	}
            	throw new AerospikeException(resultCode);        	
            }        	
        }
	}
	
	private final Record parseRecord(
		int opCount, 
		int fieldCount, 
		int generation,
		int expiration
	) throws AerospikeException {
		Map<String,Object> bins = null;
		ArrayList<Map<String, Object>> duplicates = null;
	    int receiveOffset = 0;
	
		// There can be fields in the response (setname etc).
		// But for now, ignore them. Expose them to the API if needed in the future.
		if (fieldCount != 0) {
			// Just skip over all the fields
			for (int i = 0; i < fieldCount; i++) {
				int fieldSize = Buffer.bytesToInt(receiveBuffer, receiveOffset);
				receiveOffset += 4 + fieldSize;
			}
		}
	
		for (int i = 0 ; i < opCount; i++) {
			int opSize = Buffer.bytesToInt(receiveBuffer, receiveOffset);
			byte particleType = receiveBuffer[receiveOffset+5];
			byte version = receiveBuffer[receiveOffset+6];
			byte nameSize = receiveBuffer[receiveOffset+7];
			String name = Buffer.utf8ToString(receiveBuffer, receiveOffset+8, nameSize);
			receiveOffset += 4 + 4 + nameSize;
	
			int particleBytesSize = (int) (opSize - (4 + nameSize));
	        Object value = null;
	        
			value = Buffer.bytesToParticle(particleType, receiveBuffer, receiveOffset, particleBytesSize);
			receiveOffset += particleBytesSize;
	
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
	
	    // Remove null duplicates just in case there were holes in the version number space.
	    if (duplicates != null) {
	        while (duplicates.remove(null)) {
	        	;
	        }
	    }
	    return new Record(bins, duplicates, generation, expiration);
	}

	public Record getRecord() {
		return record;
	}
	
	public int getResultCode() {
		return resultCode;
	}
}
