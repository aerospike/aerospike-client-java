package com.aerospike.client.proxy.grpc;

import com.aerospike.proxy.client.Kvs;
import io.grpc.MethodDescriptor;

import java.util.List;

/**
 * A selector of channels to execute Aerospike proxy gRPC calls.
 */
public interface GrpcChannelSelector {
    /**
     * Select a channel for the gRPC method.
     *
     * @param channels channels to select from.
     * @param methodDescriptor the method description of the request.
     * @return the selected channel.
     */
    GrpcChannelExecutor select(List<GrpcChannelExecutor> channels,
                          MethodDescriptor<Kvs.AerospikeRequestPayload, Kvs.AerospikeResponsePayload> methodDescriptor);
}
