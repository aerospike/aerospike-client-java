/*
 * Aerospike Client - Java Library
 *
 * Copyright 2013 by Aerospike, Inc. All rights reserved.
 *
 * Availability of this source code to partners and customers includes
 * redistribution rights covered by individual contract. Please check your
 * contract for exact rights and responsibilities.
 */
package com.aerospike.client.query;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.cluster.Node;
import com.aerospike.client.command.Buffer;
import com.aerospike.client.command.Command;
import com.aerospike.client.command.FieldType;
import com.aerospike.client.command.MultiCommand;
import com.aerospike.client.policy.Policy;
import com.aerospike.client.util.Packer;

public abstract class QueryCommand extends MultiCommand {
	protected volatile boolean valid = true;

	public QueryCommand(Node node) {
		super(node);
	}

	public void query(Policy policy, Statement statement) throws AerospikeException {
		byte[] functionArgBuffer = null;
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
			sendOffset += FIELD_HEADER_SIZE;
			filterSize++;  // num filters
			
			for (Filter filter : statement.filters) {
				filterSize += filter.estimateSize();
			}
			sendOffset += filterSize;
			fieldCount++;
		}
		else {
			// Calling query with no filters is more efficiently handled by a primary index scan. 
			// Estimate scan options size.
			sendOffset += 2 + FIELD_HEADER_SIZE;
			fieldCount++;
		}
		
		if (statement.binNames != null) {
			sendOffset += FIELD_HEADER_SIZE;
			binNameSize++;  // num bin names
			
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
		
		if (statement.functionName != null) {
			sendOffset += FIELD_HEADER_SIZE + 1;  // udf type
			sendOffset += Buffer.estimateSizeUtf8(statement.packageName) + FIELD_HEADER_SIZE;
			sendOffset += Buffer.estimateSizeUtf8(statement.functionName) + FIELD_HEADER_SIZE;
			
			if (statement.functionArgs.length > 0) {
				functionArgBuffer = Packer.pack(statement.functionArgs);
			}
			else {
				functionArgBuffer = new byte[0];
			}
			sendOffset += FIELD_HEADER_SIZE + functionArgBuffer.length;			
			fieldCount += 4;
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
		else {
			// Calling query with no filters is more efficiently handled by a primary index scan. 
			writeFieldHeader(2, FieldType.SCAN_OPTIONS);
			byte priority = (byte)policy.priority.ordinal();
			priority <<= 4;			
			sendBuffer[sendOffset++] = priority;
			sendBuffer[sendOffset++] = (byte)100;
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
		
		if (statement.functionName != null) {
			writeFieldHeader(1, FieldType.UDF_OP);
			sendBuffer[sendOffset++] = (statement.returnData)? (byte)1 : (byte)2;
			writeField(statement.packageName, FieldType.UDF_PACKAGE_NAME);
			writeField(statement.functionName, FieldType.UDF_FUNCTION);
			writeField(functionArgBuffer, FieldType.UDF_ARGLIST);
		}
		end();
		execute(policy);
	}

	public final void stop() {
		valid = false;
	}
}
