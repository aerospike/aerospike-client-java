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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Map;
import java.util.zip.DataFormatException;
import java.util.zip.Inflater;

import javax.annotation.Nullable;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.Key;
import com.aerospike.client.Record;
import com.aerospike.client.ResultCode;
import com.aerospike.client.command.Buffer;
import com.aerospike.client.listener.RecordListener;
import com.aerospike.client.policy.Policy;
import com.aerospike.client.proxy.grpc.GrpcCallExecutor;
import com.aerospike.client.proxy.grpc.GrpcStreamingUnaryCall;
import com.aerospike.proxy.client.KVSGrpc;
import com.aerospike.proxy.client.Kvs;
import com.google.protobuf.ByteString;

import io.grpc.stub.StreamObserver;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

public class ReadCommandProxy extends AbstractCommand {
    /**
     * Key of the Aerospike record.
     */
    private final Key key;
    /**
     * The listener to be invoked on command completion.
     */
    private final RecordListener recordListener;
    /**
     * The bins to retrieve for the record. If it is <code>null</code>, then
     * all the bins of the record are retrieved from the server.
     */
    @Nullable
    private final String[] binNames;

    private final Serde serde;

    /**
     * The gRPC call executor.
     */
    private final GrpcCallExecutor grpcCallExecutor;

    /**
     * The request payload, it is created lazily only once.
     */
    private byte[] requestPayload;


    public ReadCommandProxy(GrpcCallExecutor grpcCallExecutor, Policy policy,
                            Key key, @Nullable String[] binNames,
                            RecordListener recordListener) {
        super(policy);
        this.binNames = binNames;
        this.grpcCallExecutor = grpcCallExecutor;
        this.key = key;
        this.recordListener = recordListener;
        this.serde = new Serde();
    }

    @Override
    void sendRequest() {
        // Create request payload once.
        if (requestPayload == null) {
            // TODO: Use a pool of byte arrays?
            ByteArrayOutputStream out = new ByteArrayOutputStream(256);
            try {
                serde.writeGetPayload(out, policy, key, binNames);
                requestPayload = out.toByteArray();
            } catch (IOException e) {
                onFailure(e);
                return;
            }
        }

        grpcCallExecutor.enqueue(new GrpcStreamingUnaryCall(KVSGrpc.getGetStreamingMethod(), requestPayload, policy.totalTimeout, new StreamObserver<Kvs.AerospikeResponsePayload>() {
            @Override
            public void onNext(Kvs.AerospikeResponsePayload value) {
                try {
                    parseResponsePayload(value.getPayload());
                } catch (Throwable t) {
                    ReadCommandProxy.this.onFailure(t);
                }
            }

            @Override
            public void onError(Throwable t) {
                ReadCommandProxy.this.onFailure(t);
            }

            @Override
            public void onCompleted() {
            }
        }));

        // TODO: this should be incremented only when the request has been
        //  put on the wire?
        incrementCommandSentCounter();
    }

    private void parseResponsePayload(ByteString response) {
        // TODO: Avoid conversions between ByteString, ByteArrays and ByteBuf.

        byte[] payloadBytes = response.toByteArray();
        // TODO: Release buffers.
        ByteBuf payloadByteBuf = Unpooled.wrappedBuffer(payloadBytes);

        Serde.ProtoHeader protoHeader = serde.parseProtoHeader(payloadByteBuf);
        if (protoHeader.isCompressed) {
            // TODO: check if decompression logic is correct.

            int usize = (int) Buffer.bytesToLong(payloadBytes,
                    Serde.PROTO_HEADER_SIZE);
            byte[] buf = new byte[usize];

            Inflater inf = new Inflater();
            try {
                inf.setInput(payloadBytes, Serde.PROTO_HEADER_SIZE,
                        (int) protoHeader.size - Serde.PROTO_HEADER_SIZE);
                int rsize;

                try {
                    rsize = inf.inflate(buf);
                } catch (DataFormatException dfe) {
                    throw new AerospikeException.Serialize(dfe);
                }

                if (rsize != usize) {
                    throw new AerospikeException("compressed size " + rsize +
                            " is not expected " + usize);
                }

                payloadBytes = buf;
                payloadByteBuf = Unpooled.wrappedBuffer(payloadBytes);
                payloadByteBuf.skipBytes(Serde.PROTO_HEADER_SIZE);
            } finally {
                inf.end();
            }
        }

        Serde.ProtoMessageHeader header = serde.parseMessageHeader(payloadByteBuf);
        Record record = null;
        if (header.resultCode != ResultCode.OK && header.resultCode != ResultCode.KEY_NOT_FOUND_ERROR) {
            throw new AerospikeException(header.resultCode);
        }

        if (header.resultCode == ResultCode.OK) {
            serde.skipKey(payloadByteBuf, header.numFields);

            Map<String, Object> bins = serde.parseBins(payloadByteBuf,
                    header.numOperations, false);
            record = new Record(bins, header.generation,
                    header.expiration);
        }

        // Success.
        recordListener.onSuccess(key, record);
    }

    @Override
    void allAttemptsFailed(AerospikeException exception) {
        recordListener.onFailure(exception);
    }
}