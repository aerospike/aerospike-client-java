/*
 * Copyright 2012-2023 Aerospike, Inc.
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

import com.aerospike.client.AerospikeException;
import com.aerospike.client.Operation;
import com.aerospike.client.ResultCode;

public final class OperateArgsRead {
	public final Operation[] operations;
	public final int size;
	public final int readAttr;

	public OperateArgsRead(Operation[] operations) {
		this.operations = operations;

		int dataOffset = 0;
		int rattr = 0;
		boolean readBin = false;
		boolean readHeader = false;

		for (Operation operation : operations) {
			switch (operation.type) {
			case BIT_READ:
			case EXP_READ:
			case HLL_READ:
			case MAP_READ:
			case CDT_READ:
			case READ:
				rattr |= Command.INFO1_READ;

				// Read all bins if no bin is specified.
				if (operation.binName == null) {
					rattr |= Command.INFO1_GET_ALL;
				}
				readBin = true;
				break;

			case READ_HEADER:
				rattr |= Command.INFO1_READ;
				readHeader = true;
				break;

			default:
				throw new AerospikeException(ResultCode.PARAMETER_ERROR, "Write operations not allowed in batch read");
			}
			dataOffset += Buffer.estimateSizeUtf8(operation.binName) + Command.OPERATION_HEADER_SIZE;
			dataOffset += operation.value.estimateSize();
		}
		size = dataOffset;

		if (readHeader && ! readBin) {
			rattr |= Command.INFO1_NOBINDATA;
		}
		readAttr = rattr;
	}
}
