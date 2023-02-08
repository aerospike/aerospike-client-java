package com.aerospike.client.proxy.grpc;

import com.aerospike.proxy.client.Kvs;
import io.grpc.MethodDescriptor;

import java.util.List;

/**
 * A selector of streams within a channel to execute Aerospike proxy gRPC calls.
 */
public interface GrpcStreamSelector {
    /**
     * Select a stream for the gRPC method.
     *
     * @param streams streams to select from.
     * @param methodDescriptor the method description of the request.
     * @return the selected stream, <code>null</code> when no stream is
     * selected.
     */
    GrpcStream select(List<GrpcStream> streams,
                      MethodDescriptor<Kvs.AerospikeRequestPayload, Kvs.AerospikeResponsePayload> methodDescriptor);
}
