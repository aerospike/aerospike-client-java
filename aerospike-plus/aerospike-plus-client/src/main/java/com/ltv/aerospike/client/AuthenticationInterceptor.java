/*
 * Copyright (c) 2019 LINE Corporation. All rights reserved.
 * LINE Corporation PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 */

package com.ltv.aerospike.client;

import io.grpc.CallOptions;
import io.grpc.Channel;
import io.grpc.ClientCall;
import io.grpc.ClientInterceptor;
import io.grpc.ForwardingClientCall;
import io.grpc.Metadata;
import io.grpc.Metadata.Key;
import io.grpc.MethodDescriptor;

public class AuthenticationInterceptor implements ClientInterceptor {
    private static org.apache.log4j.Logger log = org.apache.log4j.Logger.getLogger(AuthenticationInterceptor.class.getSimpleName());

    @Override
    public <ReqT, RespT> ClientCall<ReqT, RespT> interceptCall(MethodDescriptor<ReqT, RespT> methodDescriptor, CallOptions callOptions, Channel channel) {
        return new ForwardingClientCall.SimpleForwardingClientCall<ReqT, RespT>(channel.newCall(methodDescriptor, callOptions)) {
            @Override
            public void start(final Listener<RespT> responseListener, final Metadata headers) {
                if(AerospikeClient.authenToken != null)
                    headers.put(Key.of("authen_token", Metadata.ASCII_STRING_MARSHALLER), AerospikeClient.authenToken);
                super.start(responseListener, headers);
            }
        };
    }
}