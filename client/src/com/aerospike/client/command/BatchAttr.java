/*
 * Copyright 2012-2025 Aerospike, Inc.
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

import com.aerospike.client.Operation;
import com.aerospike.client.exp.Expression;
import com.aerospike.client.policy.BatchDeletePolicy;
import com.aerospike.client.policy.BatchPolicy;
import com.aerospike.client.policy.BatchReadPolicy;
import com.aerospike.client.policy.BatchUDFPolicy;
import com.aerospike.client.policy.BatchWritePolicy;
import com.aerospike.client.policy.CommitLevel;
import com.aerospike.client.policy.Policy;
import com.aerospike.client.policy.ReadModeAP;

public final class BatchAttr {
	public Expression filterExp;
	public int readAttr;
	public int writeAttr;
	public int infoAttr;
	public int txnAttr;
	public int expiration;
	public int opSize;
	public short generation;
	public boolean hasWrite;
	public boolean sendKey;

	public BatchAttr() {
	}

	public BatchAttr(Policy policy, int rattr) {
		setRead(policy);
		this.readAttr |= rattr;
	}

	public BatchAttr(Policy policy, int rattr, Operation[] ops) {
		setRead(policy);
		this.readAttr = rattr;

		if (ops != null) {
			adjustRead(ops);
		}
	}

	public BatchAttr(BatchPolicy rp, BatchWritePolicy wp, Operation[] ops) {
		boolean readAllBins = false;
		boolean readHeader = false;
		boolean hasRead = false;
		boolean hasWriteOp = false;

		for (Operation op : ops) {
			if (op.type.isWrite) {
				hasWriteOp = true;
			}
			else {
				hasRead = true;

				if (op.type == Operation.Type.READ) {
					if (op.binName == null) {
						readAllBins = true;
					}
				}
				else if (op.type == Operation.Type.READ_HEADER) {
					readHeader = true;
				}
			}
		}

		if (hasWriteOp) {
			setWrite(wp);

			if (hasRead) {
				readAttr |= Command.INFO1_READ;

				if (readAllBins) {
					readAttr |= Command.INFO1_GET_ALL;
					// When GET_ALL is specified, RESPOND_ALL_OPS must be disabled.
					writeAttr &= ~Command.INFO2_RESPOND_ALL_OPS;
				}
				else if (readHeader) {
					readAttr |= Command.INFO1_NOBINDATA;
				}
			}
		}
		else {
			setRead(rp);

			if (readAllBins) {
				readAttr |= Command.INFO1_GET_ALL;
			}
			else if (readHeader) {
				readAttr |= Command.INFO1_NOBINDATA;
			}
		}
	}

	public void setRead(Policy rp) {
		filterExp = null;
		readAttr = Command.INFO1_READ;

		if (rp.readModeAP == ReadModeAP.ALL) {
			readAttr |= Command.INFO1_READ_MODE_AP_ALL;
		}

		writeAttr = 0;

		switch (rp.readModeSC) {
		default:
		case SESSION:
			infoAttr = 0;
			break;
		case LINEARIZE:
			infoAttr = Command.INFO3_SC_READ_TYPE;
			break;
		case ALLOW_REPLICA:
			infoAttr = Command.INFO3_SC_READ_RELAX;
			break;
		case ALLOW_UNAVAILABLE:
			infoAttr = Command.INFO3_SC_READ_TYPE | Command.INFO3_SC_READ_RELAX;
			break;
		}
		txnAttr = 0;
		expiration = rp.readTouchTtlPercent;
		generation = 0;
		hasWrite = false;
		sendKey = false;
	}

	public void setRead(BatchReadPolicy rp) {
		filterExp = rp.filterExp;
		readAttr = Command.INFO1_READ;

		if (rp.readModeAP == ReadModeAP.ALL) {
			readAttr |= Command.INFO1_READ_MODE_AP_ALL;
		}

		writeAttr = 0;

		switch (rp.readModeSC) {
		default:
		case SESSION:
			infoAttr = 0;
			break;
		case LINEARIZE:
			infoAttr = Command.INFO3_SC_READ_TYPE;
			break;
		case ALLOW_REPLICA:
			infoAttr = Command.INFO3_SC_READ_RELAX;
			break;
		case ALLOW_UNAVAILABLE:
			infoAttr = Command.INFO3_SC_READ_TYPE | Command.INFO3_SC_READ_RELAX;
			break;
		}
		txnAttr = 0;
		expiration = rp.readTouchTtlPercent;
		generation = 0;
		hasWrite = false;
		sendKey = false;
	}

	public void adjustRead(Operation[] ops) {
		for (Operation op : ops) {
			if (op.type == Operation.Type.READ) {
				if (op.binName == null) {
					readAttr |= Command.INFO1_GET_ALL;
				}
			}
			else if (op.type == Operation.Type.READ_HEADER) {
				readAttr |= Command.INFO1_NOBINDATA;
			}
		}
	}

	public void adjustRead(boolean readAllBins) {
		if (readAllBins) {
			readAttr |= Command.INFO1_GET_ALL;
		}
		else {
			readAttr |= Command.INFO1_NOBINDATA;
		}
	}

	public void setWrite(BatchWritePolicy wp) {
		filterExp = wp.filterExp;
		readAttr = 0;
		writeAttr = Command.INFO2_WRITE | Command.INFO2_RESPOND_ALL_OPS;
		infoAttr = 0;
		txnAttr = 0;
		expiration = wp.expiration;
		hasWrite = true;
		sendKey = wp.sendKey;

		switch (wp.generationPolicy) {
		default:
		case NONE:
			generation = 0;
			break;
		case EXPECT_GEN_EQUAL:
			generation = (short)wp.generation;
			writeAttr |= Command.INFO2_GENERATION;
			break;
		case EXPECT_GEN_GT:
			generation = (short)wp.generation;
			writeAttr |= Command.INFO2_GENERATION_GT;
			break;
		}

		switch (wp.recordExistsAction) {
		case UPDATE:
			break;
		case UPDATE_ONLY:
			infoAttr |= Command.INFO3_UPDATE_ONLY;
			break;
		case REPLACE:
			infoAttr |= Command.INFO3_CREATE_OR_REPLACE;
			break;
		case REPLACE_ONLY:
			infoAttr |= Command.INFO3_REPLACE_ONLY;
			break;
		case CREATE_ONLY:
			writeAttr |= Command.INFO2_CREATE_ONLY;
			break;
		}

		if (wp.durableDelete) {
			writeAttr |= Command.INFO2_DURABLE_DELETE;
		}

		if (wp.onLockingOnly) {
			txnAttr |= Command.INFO4_TXN_ON_LOCKING_ONLY;
		}

		if (wp.commitLevel == CommitLevel.COMMIT_MASTER) {
			infoAttr |= Command.INFO3_COMMIT_MASTER;
		}
	}

	public void adjustWrite(Operation[] ops) {
		for (Operation op : ops) {
			if (! op.type.isWrite) {
				readAttr |= Command.INFO1_READ;

				if (op.type == Operation.Type.READ) {
					if (op.binName == null) {
						readAttr |= Command.INFO1_GET_ALL;
						// When GET_ALL is specified, RESPOND_ALL_OPS must be disabled.
						writeAttr &= ~Command.INFO2_RESPOND_ALL_OPS;
					}
				}
				else if (op.type == Operation.Type.READ_HEADER) {
					readAttr |= Command.INFO1_NOBINDATA;
				}
			}
		}
	}

	public void setUDF(BatchUDFPolicy up) {
		filterExp = up.filterExp;
		readAttr = 0;
		writeAttr = Command.INFO2_WRITE;
		infoAttr = 0;
		txnAttr = 0;
		expiration = up.expiration;
		generation = 0;
		hasWrite = true;
		sendKey = up.sendKey;

		if (up.durableDelete) {
			writeAttr |= Command.INFO2_DURABLE_DELETE;
		}

		if (up.onLockingOnly) {
			txnAttr |= Command.INFO4_TXN_ON_LOCKING_ONLY;
		}

		if (up.commitLevel == CommitLevel.COMMIT_MASTER) {
			infoAttr |= Command.INFO3_COMMIT_MASTER;
		}
	}

	public void setDelete(BatchDeletePolicy dp) {
		filterExp = dp.filterExp;
		readAttr = 0;
		writeAttr = Command.INFO2_WRITE | Command.INFO2_RESPOND_ALL_OPS | Command.INFO2_DELETE;
		infoAttr = 0;
		txnAttr = 0;
		expiration = 0;
		hasWrite = true;
		sendKey = dp.sendKey;

		switch (dp.generationPolicy) {
		default:
		case NONE:
			generation = 0;
			break;
		case EXPECT_GEN_EQUAL:
			generation = (short)dp.generation;
			writeAttr |= Command.INFO2_GENERATION;
			break;
		case EXPECT_GEN_GT:
			generation = (short)dp.generation;
			writeAttr |= Command.INFO2_GENERATION_GT;
			break;
		}

		if (dp.durableDelete) {
			writeAttr |= Command.INFO2_DURABLE_DELETE;
		}

		if (dp.commitLevel == CommitLevel.COMMIT_MASTER) {
			infoAttr |= Command.INFO3_COMMIT_MASTER;
		}
	}

	public void setOpSize(Operation[] ops) {
		int dataOffset = 0;

		for (Operation op : ops) {
			dataOffset += Buffer.estimateSizeUtf8(op.binName) + Command.OPERATION_HEADER_SIZE;
			dataOffset += op.value.estimateSize();
		}
		opSize = dataOffset;
	}

	public void setTxn(int attr) {
		filterExp = null;
		readAttr = 0;
		writeAttr = Command.INFO2_WRITE | Command.INFO2_RESPOND_ALL_OPS | Command.INFO2_DURABLE_DELETE;
		infoAttr = 0;
		txnAttr = attr;
		expiration = 0;
		generation = 0;
		hasWrite = true;
		sendKey = false;
	}
}
