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

import com.aerospike.client.AerospikeException;
import com.aerospike.client.Bin;
import com.aerospike.client.Key;
import com.aerospike.client.ResultCode;
import com.aerospike.client.listener.WriteListener;
import com.aerospike.client.policy.WritePolicy;
import com.aerospike.client.proxy.grpc.GrpcCallExecutor;
import com.aerospike.client.proxy.grpc.GrpcStreamingUnaryCall;
import com.aerospike.proxy.client.KVSGrpc;
import com.aerospike.proxy.client.Kvs;
import com.google.protobuf.ByteString;

import io.grpc.stub.StreamObserver;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

public class WriteCommandProxy extends AbstractCommand {
    /**
     * The gRPC channel pool.
     */
    private final GrpcCallExecutor grpcCallExecutor;
    /**
     * Key of the Aerospike record.
     */
    private final Key key;
    /**
     * The bins written to Aerospike.
     */
    private final Bin[] bins;
    /**
     * The listener to be invoked on command completion.
     */
    private final WriteListener writeListener;

    private final Serde serde;

    /**
     * The write policy to write the records.
     */
    private final WritePolicy writePolicy;
    /**
     * The request payload, it is created lazily only once.
     */
    private byte[] requestPayload;


    public WriteCommandProxy(GrpcCallExecutor grpcCallExecutor,
                             WritePolicy writePolicy,
                             Key key, WriteListener writeListener, Bin... bins) {
        super(writePolicy);
        this.writePolicy = writePolicy;
        this.grpcCallExecutor = grpcCallExecutor;
        this.key = key;
        this.writeListener = writeListener;
        this.serde = new Serde();
        this.bins = bins;
    }

    @Override
    void sendRequest() {
        // Create request payload once.
        if (requestPayload == null) {
            // TODO: Use a pool of byte arrays?
            ByteArrayOutputStream out = new ByteArrayOutputStream(256);
            try {
                serde.writePutPayload(out, writePolicy, key, bins);
                requestPayload = out.toByteArray();
            } catch (IOException e) {
                onFailure(e);
                return;
            }
        }

        grpcCallExecutor.enqueue(new GrpcStreamingUnaryCall(KVSGrpc.getPutStreamingMethod(),
                requestPayload, policy.totalTimeout, new StreamObserver<Kvs.AerospikeResponsePayload>() {
            @Override
            public void onNext(Kvs.AerospikeResponsePayload value) {
                try {
                    parseResponsePayload(value.getPayload());
                } catch (Throwable t) {
                    WriteCommandProxy.this.onFailure(t);
                }
            }

            @Override
            public void onError(Throwable t) {
                WriteCommandProxy.this.onFailure(t);
            }

            @Override
            public void onCompleted() {
            }
        }));

        incrementCommandSentCounter();
    }

    private void parseResponsePayload(ByteString response) {
        // TODO: Avoid conversions between ByteString, ByteArrays and ByteBuf.

        byte[] payloadBytes = response.toByteArray();
        ByteBuf payloadByteBuf = Unpooled.wrappedBuffer(payloadBytes);

        serde.parseProtoHeader(payloadByteBuf);

        // TODO: is write response never compressed?
        Serde.ProtoMessageHeader header = serde.parseMessageHeader(payloadByteBuf);
        if (header.resultCode == ResultCode.OK ||
                (header.resultCode == ResultCode.FILTERED_OUT &&
                        !writePolicy.failOnFilteredOut)) {
            writeListener.onSuccess(key);
            return;
        }

        throw new AerospikeException(header.resultCode);
    }

    @Override
    void allAttemptsFailed(AerospikeException exception) {
        writeListener.onFailure(exception);
    }
}
