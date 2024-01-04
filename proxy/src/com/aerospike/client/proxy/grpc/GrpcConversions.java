/*
 * Copyright 2012-2024 Aerospike, Inc.
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
package com.aerospike.client.proxy.grpc;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.Operation;
import com.aerospike.client.ResultCode;
import com.aerospike.client.Value;
import com.aerospike.client.policy.Policy;
import com.aerospike.client.policy.QueryPolicy;
import com.aerospike.client.policy.ScanPolicy;
import com.aerospike.client.policy.WritePolicy;
import com.aerospike.client.query.Filter;
import com.aerospike.client.query.PartitionFilter;
import com.aerospike.client.query.PartitionStatus;
import com.aerospike.client.query.Statement;
import com.aerospike.client.util.Packer;
import com.aerospike.proxy.client.Kvs;
import com.google.protobuf.ByteString;

import io.grpc.Status;
import io.grpc.StatusRuntimeException;

/**
 * Conversions from native client objects to Grpc objects.
 */
public class GrpcConversions {
	private static final String ERROR_MESSAGE_SEPARATOR = " -> ";
	public static final int MAX_ERR_MSG_LENGTH = 10 * 1024;

	public static void setRequestPolicy(
		Policy policy,
		Kvs.AerospikeRequestPayload.Builder requestBuilder
	) {
		if (policy instanceof WritePolicy) {
			Kvs.WritePolicy.Builder writePolicyBuilder = Kvs.WritePolicy.newBuilder();

			Kvs.ReadModeAP readModeAP = Kvs.ReadModeAP.valueOf(policy.readModeAP.name());
			writePolicyBuilder.setReadModeAP(readModeAP);

			Kvs.ReadModeSC readModeSC = Kvs.ReadModeSC.valueOf(policy.readModeSC.name());
			writePolicyBuilder.setReadModeSC(readModeSC);

			Kvs.Replica replica = Kvs.Replica.valueOf(policy.replica.name());
			writePolicyBuilder.setReplica(replica);

			requestBuilder.setWritePolicy(writePolicyBuilder.build());
		}
		else {
			Kvs.ReadPolicy.Builder readPolicyBuilder = Kvs.ReadPolicy.newBuilder();

			Kvs.ReadModeAP readModeAP = Kvs.ReadModeAP.valueOf(policy.readModeAP.name());
			readPolicyBuilder.setReadModeAP(readModeAP);

			Kvs.ReadModeSC readModeSC = Kvs.ReadModeSC.valueOf(policy.readModeSC.name());
			readPolicyBuilder.setReadModeSC(readModeSC);

			Kvs.Replica replica = Kvs.Replica.valueOf(policy.replica.name());
			readPolicyBuilder.setReplica(replica);

			requestBuilder.setReadPolicy(readPolicyBuilder.build());
		}
	}

	public static Kvs.ScanPolicy toGrpc(ScanPolicy scanPolicy) {
		// Base policy fields.
		Kvs.ScanPolicy.Builder scanPolicyBuilder = Kvs.ScanPolicy.newBuilder();

		Kvs.ReadModeAP readModeAP = Kvs.ReadModeAP.valueOf(scanPolicy.readModeAP.name());
		scanPolicyBuilder.setReadModeAP(readModeAP);

		Kvs.ReadModeSC readModeSC = Kvs.ReadModeSC.valueOf(scanPolicy.readModeSC.name());
		scanPolicyBuilder.setReadModeSC(readModeSC);

		Kvs.Replica replica = Kvs.Replica.valueOf(scanPolicy.replica.name());
		scanPolicyBuilder.setReplica(replica);

		if (scanPolicy.filterExp != null) {
			scanPolicyBuilder.setExpression(ByteString.copyFrom(scanPolicy.filterExp.getBytes()));
		}

		scanPolicyBuilder.setTotalTimeout(scanPolicy.totalTimeout);
		scanPolicyBuilder.setCompress(scanPolicy.compress);

		// Scan policy specific fields
		scanPolicyBuilder.setMaxRecords(scanPolicy.maxRecords);
		scanPolicyBuilder.setRecordsPerSecond(scanPolicy.recordsPerSecond);
		scanPolicyBuilder.setMaxConcurrentNodes(scanPolicy.maxConcurrentNodes);
		scanPolicyBuilder.setConcurrentNodes(scanPolicy.concurrentNodes);
		scanPolicyBuilder.setIncludeBinData(scanPolicy.includeBinData);
		return scanPolicyBuilder.build();
	}

	public static Kvs.QueryPolicy toGrpc(QueryPolicy queryPolicy) {
		// Base policy fields.
		Kvs.QueryPolicy.Builder queryPolicyBuilder = Kvs.QueryPolicy.newBuilder();

		Kvs.ReadModeAP readModeAP = Kvs.ReadModeAP.valueOf(queryPolicy.readModeAP.name());
		queryPolicyBuilder.setReadModeAP(readModeAP);

		Kvs.ReadModeSC readModeSC = Kvs.ReadModeSC.valueOf(queryPolicy.readModeSC.name());
		queryPolicyBuilder.setReadModeSC(readModeSC);

		Kvs.Replica replica = Kvs.Replica.valueOf(queryPolicy.replica.name());
		queryPolicyBuilder.setReplica(replica);

		if (queryPolicy.filterExp != null) {
			queryPolicyBuilder.setExpression(ByteString.copyFrom(queryPolicy.filterExp.getBytes()));
		}

		queryPolicyBuilder.setTotalTimeout(queryPolicy.totalTimeout);
		queryPolicyBuilder.setCompress(queryPolicy.compress);
		queryPolicyBuilder.setSendKey(queryPolicy.sendKey);

		// Query policy specific fields
		queryPolicyBuilder.setMaxConcurrentNodes(queryPolicy.maxConcurrentNodes);
		queryPolicyBuilder.setRecordQueueSize(queryPolicy.recordQueueSize);
		queryPolicyBuilder.setInfoTimeout(queryPolicy.infoTimeout);
		queryPolicyBuilder.setIncludeBinData(queryPolicy.includeBinData);
		queryPolicyBuilder.setFailOnClusterChange(queryPolicy.failOnClusterChange);
		queryPolicyBuilder.setShortQuery(queryPolicy.shortQuery);
		return queryPolicyBuilder.build();
	}

	/**
	 * Convert a value to packed bytes.
	 *
	 * @param value the value to pack
	 * @return the packed bytes.
	 */
	public static ByteString valueToByteString(Value value) {
		Packer packer = new Packer();
		value.pack(packer); // Calculate buffer size.
		packer.createBuffer();
		value.pack(packer); // Write to buffer.
		return ByteString.copyFrom(packer.getBuffer());
	}

	public static Kvs.Filter toGrpc(Filter filter) {
		Kvs.Filter.Builder builder = Kvs.Filter.newBuilder();

		builder.setName(filter.getName());
		builder.setValType(filter.getValType());

		if (filter.getBegin() != null) {
			Packer packer = new Packer();
			filter.getBegin().pack(packer);
			packer.createBuffer();
			filter.getBegin().pack(packer);
			builder.setBegin(ByteString.copyFrom(packer.getBuffer()));
		}

		if (filter.getBegin() != null) {
			builder.setBegin(valueToByteString(filter.getBegin()));
		}

		if (filter.getEnd() != null) {
			builder.setEnd(valueToByteString(filter.getEnd()));
		}

		if (filter.getPackedCtx() != null) {
			builder.setPackedCtx(ByteString.copyFrom(filter.getPackedCtx()));
		}

		builder.setColType(Kvs.IndexCollectionType.valueOf(filter.getColType().name()));
		return builder.build();
	}

	public static Kvs.Operation toGrpc(Operation operation) {
		Kvs.Operation.Builder builder = Kvs.Operation.newBuilder();
		builder.setType(Kvs.OperationType.valueOf(operation.type.name()));

		if (operation.binName != null) {
			builder.setBinName(operation.binName);
		}

		if (operation.value != null) {
			builder.setValue(valueToByteString(operation.value));
		}
		return builder.build();
	}

	/**
	 * @param statement 	Aerospike client statement
	 * @param taskId    	required non-zero taskId to use for the execution at the proxy
	 * @param maxRecords	max records to return
	 * @return equivalent gRPC {@link com.aerospike.proxy.client.Kvs.Statement}
	 */
	public static Kvs.Statement toGrpc(Statement statement, long taskId, long maxRecords) {
		Kvs.Statement.Builder statementBuilder = Kvs.Statement.newBuilder();
		statementBuilder.setNamespace(statement.getNamespace());

		if (statement.getSetName() != null) {
			statementBuilder.setSetName(statement.getSetName());
		}

		if (statement.getIndexName() != null) {
			statementBuilder.setIndexName(statement.getIndexName());
		}

		if (statement.getBinNames() != null) {
			for (String binName : statement.getBinNames()) {
				statementBuilder.addBinNames(binName);
			}
		}

		if (statement.getFilter() != null) {
			statementBuilder.setFilter(toGrpc(statement.getFilter()));
		}


		if (statement.getPackageName() != null) {
			statementBuilder.setPackageName(statement.getPackageName());
		}

		if (statement.getFunctionName() != null) {
			statementBuilder.setFunctionName(statement.getFunctionName());
		}

		if (statement.getFunctionArgs() != null) {
			for (Value arg : statement.getFunctionArgs()) {
				statementBuilder.addFunctionArgs(valueToByteString(arg));
			}
		}

		if (statement.getOperations() != null) {
			for (Operation operation : statement.getOperations()) {
				statementBuilder.addOperations(toGrpc(operation));
			}
		}

		statementBuilder.setTaskId(taskId);

		statementBuilder.setMaxRecords(maxRecords);
		statementBuilder.setRecordsPerSecond(statement.getRecordsPerSecond());
		return statementBuilder.build();
	}

	public static Kvs.PartitionStatus toGrpc(PartitionStatus ps) {
		Kvs.PartitionStatus.Builder builder = Kvs.PartitionStatus.newBuilder();
		builder.setId(ps.id);
		builder.setBVal(ps.bval);
		builder.setRetry(ps.retry);
		if (ps.digest != null) {
			builder.setDigest(ByteString.copyFrom(ps.digest));
		}
		return builder.build();
	}

	public static Kvs.PartitionFilter toGrpc(PartitionFilter partitionFilter) {
		Kvs.PartitionFilter.Builder builder = Kvs.PartitionFilter.newBuilder();
		builder.setBegin(partitionFilter.getBegin());
		builder.setCount(partitionFilter.getCount());
		builder.setRetry(partitionFilter.isRetry());

		byte[] digest = partitionFilter.getDigest();
		if (digest != null && digest.length > 0) {
			builder.setDigest(ByteString.copyFrom(digest));
		}

		if (partitionFilter.getPartitions() != null) {
			for (PartitionStatus ps : partitionFilter.getPartitions()) {
				builder.addPartitionStatuses(toGrpc(ps));
			}
		}
		return builder.build();
	}

	public static Kvs.BackgroundExecutePolicy toGrpc(WritePolicy writePolicy) {
		// Base policy fields.
		Kvs.BackgroundExecutePolicy.Builder queryPolicyBuilder = Kvs.BackgroundExecutePolicy.newBuilder();

		Kvs.ReadModeAP readModeAP = Kvs.ReadModeAP.valueOf(writePolicy.readModeAP.name());
		queryPolicyBuilder.setReadModeAP(readModeAP);

		Kvs.ReadModeSC readModeSC = Kvs.ReadModeSC.valueOf(writePolicy.readModeSC.name());
		queryPolicyBuilder.setReadModeSC(readModeSC);

		Kvs.Replica replica = Kvs.Replica.valueOf(writePolicy.replica.name());
		queryPolicyBuilder.setReplica(replica);

		if (writePolicy.filterExp != null) {
			queryPolicyBuilder.setExpression(ByteString.copyFrom(writePolicy.filterExp.getBytes()));
		}

		queryPolicyBuilder.setTotalTimeout(writePolicy.totalTimeout);
		queryPolicyBuilder.setCompress(writePolicy.compress);
		queryPolicyBuilder.setSendKey(writePolicy.sendKey);

		// Query policy specific fields
		queryPolicyBuilder.setRecordExistsAction(Kvs.RecordExistsAction.valueOf(writePolicy.recordExistsAction.name()));
		queryPolicyBuilder.setGenerationPolicy(Kvs.GenerationPolicy.valueOf(writePolicy.generationPolicy.name()));
		queryPolicyBuilder.setCommitLevel(Kvs.CommitLevel.valueOf(writePolicy.commitLevel.name()));
		queryPolicyBuilder.setGeneration(writePolicy.generation);
		queryPolicyBuilder.setExpiration(writePolicy.expiration);
		queryPolicyBuilder.setRespondAllOps(writePolicy.respondAllOps);
		queryPolicyBuilder.setDurableDelete(writePolicy.durableDelete);
		queryPolicyBuilder.setXdr(writePolicy.xdr);
		return queryPolicyBuilder.build();
	}

	public static AerospikeException toAerospike(StatusRuntimeException sre, Policy policy, int iteration) {
		Status.Code code = sre.getStatus().getCode();
		int resultCode = ResultCode.CLIENT_ERROR;
		switch (code) {
			case CANCELLED:
			case UNKNOWN:
			case NOT_FOUND:
			case ALREADY_EXISTS:
			case FAILED_PRECONDITION:
			case OUT_OF_RANGE:
			case UNIMPLEMENTED:
			case INTERNAL:
				resultCode = ResultCode.CLIENT_ERROR;
				break;

			case ABORTED:
			case DATA_LOSS:
				resultCode = ResultCode.SERVER_ERROR;
				break;

			case INVALID_ARGUMENT:
				resultCode = ResultCode.SERIALIZE_ERROR;
				break;

			case DEADLINE_EXCEEDED:
				return new AerospikeException.Timeout(policy, iteration);

			case PERMISSION_DENIED:
				resultCode = ResultCode.FAIL_FORBIDDEN;
				break;

			case RESOURCE_EXHAUSTED:
				resultCode = ResultCode.QUOTA_EXCEEDED;
				break;

			case UNAUTHENTICATED:
				resultCode = ResultCode.NOT_AUTHENTICATED;
				break;

			case UNAVAILABLE:
				resultCode = ResultCode.SERVER_NOT_AVAILABLE;
				break;

			case OK:
				resultCode = ResultCode.OK;
				break;
		}

		return new AerospikeException(resultCode, getDisplayMessage(sre, MAX_ERR_MSG_LENGTH), sre);
	}

	/**
	 * Get the error message to display restricting it to some length.
	 */
	public static String getDisplayMessage(Throwable e, int maxMsgLength) {
		if (maxMsgLength <= 0) {
			return "";
		}

		String errorMessage = getMessage(e);
		Throwable rootCause = e.getCause();
		while (rootCause != null) {
			String current = getMessage(rootCause);
			errorMessage = (errorMessage.isEmpty()) ? current
				: errorMessage + ERROR_MESSAGE_SEPARATOR + current;
			rootCause = rootCause.getCause();
		}

		return take(errorMessage, maxMsgLength);
	}

	/**
	 * Take at most first `n` characters from the string.
	 *
	 * @param s input string
	 * @param n number of characters to take.
	 * @return the string that is at most `n` characters in length.
	 */
	private static String take(String s, int n) {
		int trimLength = Math.min(n, s.length());
		if (trimLength <= 0) {
			return "";
		}
		return s.substring(0, trimLength);
	}

	/**
	 * Get error message for [e].
	 */
	private static String getMessage(Throwable e) {
		if (e == null) {
			return "";
		}

		String errorMessage = e.getMessage() != null ? e.getMessage() : "";

		errorMessage = errorMessage.split("\\r?\\n|\\r")[0];
		if (errorMessage.trim().isEmpty()) {
			return e.getClass().getName();
		}
		else {
			return String.format("%s - %s", e.getClass().getName(),
				errorMessage);
		}
	}
}
