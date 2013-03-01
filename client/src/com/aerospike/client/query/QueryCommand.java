/*
 * Aerospike Client - Java Library
 *
 * Copyright 2012 by Aerospike, Inc. All rights reserved.
 *
 * Availability of this source code to partners and customers includes
 * redistribution rights covered by individual contract. Please check your
 * contract for exact rights and responsibilities.
 */
package com.aerospike.client.query;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.Key;
import com.aerospike.client.Record;
import com.aerospike.client.cluster.Node;
import com.aerospike.client.command.Buffer;
import com.aerospike.client.command.Command;
import com.aerospike.client.command.FieldType;
import com.aerospike.client.command.MultiCommand;
import com.aerospike.client.policy.QueryPolicy;

public final class QueryCommand extends MultiCommand {
	private final RecordSet recordSet;
	private volatile boolean valid;

	public QueryCommand(Node node, RecordSet resultSet) {
		super(node);
		this.recordSet = resultSet;
	}

	public void query(QueryPolicy policy, Statement statement) throws AerospikeException {
		valid = true;
		
		int fieldCount = 0;
		int filterSize = 0;
		int binNameSize = 0;
		
		if (statement.namespace != null) {
			sendOffset += Buffer.estimateSizeUtf8(statement.namespace) + FIELD_HEADER_SIZE;
			fieldCount++;
		}
		
		if (statement.indexName != null) {
			sendOffset += Buffer.estimateSizeUtf8(statement.indexName) + FIELD_HEADER_SIZE;
			fieldCount++;
		}

		if (statement.setName != null) {
			sendOffset += Buffer.estimateSizeUtf8(statement.setName) + FIELD_HEADER_SIZE;
			fieldCount++;
		}
		
		if (statement.filters != null) {
			sendOffset += FIELD_HEADER_SIZE + 1;  // header + num filters
			
			for (Filter filter : statement.filters) {
				filterSize += filter.estimateSize();
			}
			sendOffset += filterSize;
			fieldCount++;
		}
		
		if (statement.binNames != null) {
			sendOffset += FIELD_HEADER_SIZE + 1;  // header + num bin names
			
			for (String binName : statement.binNames) {
				binNameSize += Buffer.estimateSizeUtf8(binName) + 1;
			}
			sendOffset += binNameSize;
			fieldCount++;
		}

		if (statement.taskId > 0) {
			sendOffset += 8 + FIELD_HEADER_SIZE;
			fieldCount++;
		}
		
		begin();
		byte readAttr = Command.INFO1_READ;		
		writeHeader(readAttr, fieldCount, 0);
				
		if (statement.namespace != null) {
			writeField(statement.namespace, FieldType.NAMESPACE);
		}
		
		if (statement.indexName != null) {
			writeField(statement.indexName, FieldType.INDEX_NAME);
		}

		if (statement.setName != null) {
			writeField(statement.setName, FieldType.TABLE);
		}
		
		if (statement.filters != null) {
			writeFieldHeader(filterSize, FieldType.INDEX_RANGE);
	        sendBuffer[sendOffset++] = (byte)statement.filters.length;
			
			for (Filter filter : statement.filters) {
				sendOffset = filter.write(sendBuffer, sendOffset);
			}
		}
		
		if (statement.binNames != null) {
			writeFieldHeader(binNameSize, FieldType.QUERY_BINLIST);
	        sendBuffer[sendOffset++] = (byte)statement.binNames.length;

			for (String binName : statement.binNames) {
				int len = Buffer.stringToUtf8(binName, sendBuffer, sendOffset + 1);
				sendBuffer[sendOffset] = (byte)len;
				sendOffset += len + 1;
			}
		}
		
		if (statement.taskId > 0) {
			writeFieldHeader(8, FieldType.TRAN_ID);
			Buffer.longToBytes(statement.taskId, sendBuffer, sendOffset);
			sendOffset += 8;
		}
		
		execute(policy);
	}

	protected boolean parseRecordResults(int receiveSize) 
		throws AerospikeException, IOException {
		// Read/parse remaining message bytes one record at a time.
		receiveOffset = 0;
		
		while (receiveOffset < receiveSize) {
    		readBytes(MSG_REMAINING_HEADER_SIZE);    		
			int resultCode = receiveBuffer[5];

			if (resultCode != 0) {
				throw new AerospikeException(resultCode);
			}

			byte info3 = receiveBuffer[3];
			
			// If this is the end marker of the response, do not proceed further
			if ((info3 & INFO3_LAST) == INFO3_LAST) {
				return false;
			}
			
			int generation = Buffer.bytesToInt(receiveBuffer, 6);
			int expiration = Buffer.bytesToInt(receiveBuffer, 10);
			int fieldCount = Buffer.bytesToShort(receiveBuffer, 18);
			int opCount = Buffer.bytesToShort(receiveBuffer, 20);
			
			Key key = parseKey(fieldCount);

			// Parse bins.
			Map<String,Object> bins = null;
			
			for (int i = 0 ; i < opCount; i++) {
	    		readBytes(8);	
				int opSize = Buffer.bytesToInt(receiveBuffer, 0);
				byte particleType = receiveBuffer[5];
				byte nameSize = receiveBuffer[7];
	    		
				readBytes(nameSize);
				String name = Buffer.utf8ToString(receiveBuffer, 0, nameSize);
		
				int particleBytesSize = (int) (opSize - (4 + nameSize));
				readBytes(particleBytesSize);
		        Object value = Buffer.bytesToParticle(particleType, receiveBuffer, 0, particleBytesSize);
						
				if (bins == null) {
					bins = new HashMap<String,Object>();
				}
				bins.put(name, value);
		    }
			
			Record record = new Record(bins, null, generation, expiration);
			
			if (! valid) {
				throw new AerospikeException.QueryTerminated();
			}
			
			if (! recordSet.put(new KeyRecord(key, record))) {
				valid = false;
				throw new AerospikeException.QueryTerminated();
			}
		}
		return true;
	}
		
	public void stop() {
		valid = false;
	}
}
