package com.aerospike.client.proxy.grpc;

import com.aerospike.client.policy.Policy;
import com.aerospike.client.policy.WritePolicy;
import com.aerospike.proxy.client.Kvs;

/**
 * Conversions from native client objects to Grpc objects.
 */
public class GrpcConversions {
    public static void setRequestPolicy(Policy policy,
                                        Kvs.AerospikeRequestPayload.Builder requestBuilder) {
        if (policy instanceof WritePolicy) {
            Kvs.WritePolicy.Builder writePolicyBuilder =
                    Kvs.WritePolicy.newBuilder();
            Kvs.ReadModeAP readModeAP =
                    Kvs.ReadModeAP.valueOf(policy.readModeAP.name());
            writePolicyBuilder.setReadModeAP(readModeAP);
            Kvs.ReadModeSC readModeSC =
                    Kvs.ReadModeSC.valueOf(policy.readModeSC.name());
            writePolicyBuilder.setReadModeSC(readModeSC);
            Kvs.Replica replica =
                    Kvs.Replica.valueOf(policy.replica.name());
            writePolicyBuilder.setReplica(replica);
            requestBuilder.setWritePolicy(writePolicyBuilder.build());
        } else {
            // TODO: Add cases for query and batch policies
            Kvs.ReadPolicy.Builder readPolicyBuilder =
                    Kvs.ReadPolicy.newBuilder();
            Kvs.ReadModeAP readModeAP =
                    Kvs.ReadModeAP.valueOf(policy.readModeAP.name());
            readPolicyBuilder.setReadModeAP(readModeAP);
            Kvs.ReadModeSC readModeSC =
                    Kvs.ReadModeSC.valueOf(policy.readModeSC.name());
            readPolicyBuilder.setReadModeSC(readModeSC);
            Kvs.Replica replica =
                    Kvs.Replica.valueOf(policy.replica.name());
            readPolicyBuilder.setReplica(replica);
            requestBuilder.setReadPolicy(readPolicyBuilder.build());
        }
    }
}
