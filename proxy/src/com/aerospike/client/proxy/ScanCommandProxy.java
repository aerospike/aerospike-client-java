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

package com.aerospike.client.proxy;

import javax.annotation.Nullable;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.ResultCode;
import com.aerospike.client.command.Command;
import com.aerospike.client.listener.RecordSequenceListener;
import com.aerospike.client.policy.ScanPolicy;
import com.aerospike.client.proxy.grpc.GrpcCallExecutor;
import com.aerospike.client.proxy.grpc.GrpcConversions;
import com.aerospike.client.query.PartitionFilter;
import com.aerospike.proxy.client.Kvs;
import com.aerospike.proxy.client.ScanGrpc;

/**
 * Implements asynchronous scan for the proxy.
 */
public class ScanCommandProxy extends CommandProxy {
	private final String namespace;
	private final String setName;
	private final String[] binNames;
	private final PartitionFilter partitionFilter;
	private final RecordSequenceListener listener;

	public ScanCommandProxy(GrpcCallExecutor executor,
							ScanPolicy scanPolicy,
							String namespace,
							@Nullable
							String setName,
							@Nullable
							String[] binNames,
							@Nullable PartitionFilter partitionFilter,
							RecordSequenceListener listener
	) {
		super(ScanGrpc.getScanStreamingMethod(), executor, scanPolicy);
		this.namespace = namespace;
		this.setName = setName;
		this.binNames = binNames;
		this.partitionFilter = partitionFilter;
		this.listener = listener;
	}

	@Override
	protected void writeCommand(Command command) {
		// Nothing to do since there is no Aerospike payload.
	}

	@Override
	protected void parseResult(Parser parser, boolean isLast) {
		ProxyRecord proxyRecord = parseRecordResult(parser, false, true,
			false);

		if (proxyRecord.resultCode == ResultCode.OK && proxyRecord.key == null) {
			// This is the end of scan marker record.
			listener.onSuccess();
			return;
		}

		try {
			listener.onRecord(proxyRecord.key, proxyRecord.record);
		}
		catch (Throwable t) {
			// Exception thrown from the server.
			// TODO: sent a request to the proxy server to abort the scan.
			logOnSuccessError(t);
		}
	}

	@Override
	protected void onFailure(AerospikeException ae) {
		listener.onFailure(ae);
	}

	@Override
	protected Kvs.AerospikeRequestPayload.Builder getRequestBuilder() {
		// Set the scan parameters in the Aerospike request payload.
		Kvs.AerospikeRequestPayload.Builder builder = Kvs.AerospikeRequestPayload.newBuilder();
		Kvs.ScanRequest.Builder scanRequestBuilder =
			Kvs.ScanRequest.newBuilder();

		scanRequestBuilder.setScanPolicy(GrpcConversions.toGrpc((ScanPolicy)policy));

		scanRequestBuilder.setNamespace(namespace);
		if (setName != null) {
			scanRequestBuilder.setSetName(setName);
		}
		if (binNames != null) {
			for (String binName : binNames) {
				scanRequestBuilder.addBinNames(binName);
			}
		}
		if (partitionFilter != null) {
			scanRequestBuilder.setPartitionFilter(GrpcConversions.toGrpc(partitionFilter));
		}

		builder.setScanRequest(scanRequestBuilder.build());
		return builder;
	}
}
