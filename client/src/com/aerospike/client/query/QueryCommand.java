/*******************************************************************************
 * Copyright 2012-2014 by Aerospike.
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to
 * deal in the Software without restriction, including without limitation the
 * rights to use, copy, modify, merge, publish, distribute, sublicense, and/or
 * sell copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
 * FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS
 * IN THE SOFTWARE.
 ******************************************************************************/
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
	private final Policy policy;
	private final Statement statement;

	public QueryCommand(Node node, Policy policy, Statement statement) {
		super(node);
		this.policy = policy;
		this.statement = statement;
	}

	@Override
	protected final Policy getPolicy() {
		return policy;
	}

	@Override
	protected final void writeBuffer() throws AerospikeException {
		byte[] functionArgBuffer = null;
		int fieldCount = 0;
		int filterSize = 0;
		int binNameSize = 0;
		
		begin();
		
		if (statement.namespace != null) {
			dataOffset += Buffer.estimateSizeUtf8(statement.namespace) + FIELD_HEADER_SIZE;
			fieldCount++;
		}
		
		if (statement.indexName != null) {
			dataOffset += Buffer.estimateSizeUtf8(statement.indexName) + FIELD_HEADER_SIZE;
			fieldCount++;
		}

		if (statement.setName != null) {
			dataOffset += Buffer.estimateSizeUtf8(statement.setName) + FIELD_HEADER_SIZE;
			fieldCount++;
		}
		
		if (statement.filters != null) {
			dataOffset += FIELD_HEADER_SIZE;
			filterSize++;  // num filters
			
			for (Filter filter : statement.filters) {
				filterSize += filter.estimateSize();
			}
			dataOffset += filterSize;
			fieldCount++;
		}
		else {
			// Calling query with no filters is more efficiently handled by a primary index scan. 
			// Estimate scan options size.
			dataOffset += 2 + FIELD_HEADER_SIZE;
			fieldCount++;
		}
		
		if (statement.binNames != null) {
			dataOffset += FIELD_HEADER_SIZE;
			binNameSize++;  // num bin names
			
			for (String binName : statement.binNames) {
				binNameSize += Buffer.estimateSizeUtf8(binName) + 1;
			}
			dataOffset += binNameSize;
			fieldCount++;
		}

		if (statement.taskId > 0) {
			dataOffset += 8 + FIELD_HEADER_SIZE;
			fieldCount++;
		}
		
		if (statement.functionName != null) {
			dataOffset += FIELD_HEADER_SIZE + 1;  // udf type
			dataOffset += Buffer.estimateSizeUtf8(statement.packageName) + FIELD_HEADER_SIZE;
			dataOffset += Buffer.estimateSizeUtf8(statement.functionName) + FIELD_HEADER_SIZE;
			
			if (statement.functionArgs.length > 0) {
				functionArgBuffer = Packer.pack(statement.functionArgs);
			}
			else {
				functionArgBuffer = new byte[0];
			}
			dataOffset += FIELD_HEADER_SIZE + functionArgBuffer.length;			
			fieldCount += 4;
		}

		sizeBuffer();
		
		byte readAttr = Command.INFO1_READ;		
		writeHeader(readAttr, 0, fieldCount, 0);
				
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
	        dataBuffer[dataOffset++] = (byte)statement.filters.length;
			
			for (Filter filter : statement.filters) {
				dataOffset = filter.write(dataBuffer, dataOffset);
			}
		}
		else {
			// Calling query with no filters is more efficiently handled by a primary index scan. 
			writeFieldHeader(2, FieldType.SCAN_OPTIONS);
			byte priority = (byte)policy.priority.ordinal();
			priority <<= 4;			
			dataBuffer[dataOffset++] = priority;
			dataBuffer[dataOffset++] = (byte)100;
		}
		
		if (statement.binNames != null) {
			writeFieldHeader(binNameSize, FieldType.QUERY_BINLIST);
	        dataBuffer[dataOffset++] = (byte)statement.binNames.length;

			for (String binName : statement.binNames) {
				int len = Buffer.stringToUtf8(binName, dataBuffer, dataOffset + 1);
				dataBuffer[dataOffset] = (byte)len;
				dataOffset += len + 1;
			}
		}
		
		if (statement.taskId > 0) {
			writeFieldHeader(8, FieldType.TRAN_ID);
			Buffer.longToBytes(statement.taskId, dataBuffer, dataOffset);
			dataOffset += 8;
		}
		
		if (statement.functionName != null) {
			writeFieldHeader(1, FieldType.UDF_OP);
			dataBuffer[dataOffset++] = (statement.returnData)? (byte)1 : (byte)2;
			writeField(statement.packageName, FieldType.UDF_PACKAGE_NAME);
			writeField(statement.functionName, FieldType.UDF_FUNCTION);
			writeField(functionArgBuffer, FieldType.UDF_ARGLIST);
		}
		end();
	}
}
