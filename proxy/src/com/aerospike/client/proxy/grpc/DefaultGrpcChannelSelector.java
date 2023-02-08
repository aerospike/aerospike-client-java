package com.aerospike.client.proxy.grpc;

import com.aerospike.proxy.client.Kvs;
import io.grpc.MethodDescriptor;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Random;

/**
 * A default gRPC stream selector which selects channel by the low and high
 * water mark.
 */
public class DefaultGrpcChannelSelector implements GrpcChannelSelector {
    private final int requestsLowWaterMark;
    private final int requestsHighWaterMark;
    private final Random random = new Random();

    public DefaultGrpcChannelSelector(int requestsLowWaterMark,
                                  int requestsHighWaterMark) {
        this.requestsLowWaterMark = requestsLowWaterMark;
        this.requestsHighWaterMark = requestsHighWaterMark;
    }
    @Override
    public GrpcChannelExecutor select(List<GrpcChannelExecutor> channels, MethodDescriptor<Kvs.AerospikeRequestPayload, Kvs.AerospikeResponsePayload> methodDescriptor) {
        // Sort by channel id. Leave original list as it is.
        channels = new ArrayList<>(channels);
        channels.sort(Comparator.comparingLong(GrpcChannelExecutor::getId));

        // Select the first channel below the low watermark which has
        // executed the least number of requests.
        for (GrpcChannelExecutor channel : channels) {
            if (channel.getOngoingRequests() < requestsLowWaterMark) {
                return channel;
            }
        }


        // FIXME: it might be the case that the channel has opened
        //  maxConcurrentStreams but none of them are for this grpcCall. This
        //  also needs to be checked when selecting the channel.

        // All channels are above the low watermark, select the first channel
        // below the high watermark.
        for (GrpcChannelExecutor channel : channels) {
            if (channel.getOngoingRequests() < requestsHighWaterMark) {
                return channel;
            }
        }

        // TODO: maybe we should use in-flight bytes, number of streams, or
        //  some other parameter to select the channel.
        // All channels are above the high water mark, select random channel.
        return channels.get(random.nextInt(channels.size()));
    }
}
