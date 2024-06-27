package com.aerospike.client.proxy;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.Info;
import com.aerospike.client.ResultCode;
import com.aerospike.client.cluster.Node;
import com.aerospike.client.command.Command;
import com.aerospike.client.listener.InfoListener;
import com.aerospike.client.policy.InfoPolicy;
import com.aerospike.client.policy.Policy;
import com.aerospike.client.policy.ScanPolicy;
import com.aerospike.client.proxy.grpc.GrpcCallExecutor;
import com.aerospike.client.proxy.grpc.GrpcConversions;
import com.aerospike.proxy.client.Kvs;
import com.aerospike.proxy.client.InfoGrpc;

import java.util.HashMap;
import java.util.Map;

public class InfoCommandProxy extends SingleCommandProxy {

    private final InfoListener listener;
    private final String[] commands;
    private Map<String,String> map;

    private final InfoPolicy infoPolicy;

    public InfoCommandProxy(GrpcCallExecutor executor, InfoListener listener, InfoPolicy policy, String... commands) {
        super(InfoGrpc.getInfoMethod(), executor, createPolicy(policy));
        this.infoPolicy = policy;
        this.listener = listener;
        this.commands = commands;
    }

    private static Policy createPolicy(InfoPolicy policy) {
        Policy p = new Policy();

        if (policy == null) {
            p.setTimeout(1000);
        }
        else {
            p.setTimeout(policy.timeout);
        }
        return p;
    }

    @Override
    Kvs.AerospikeRequestPayload.Builder getRequestBuilder() {
        Kvs.AerospikeRequestPayload.Builder builder = Kvs.AerospikeRequestPayload.newBuilder();
        Kvs.InfoRequest.Builder infoRequestBuilder = Kvs.InfoRequest.newBuilder();

        if(commands != null){
            for(String command: commands){
                infoRequestBuilder.addCommands(command);
            }
        }
        infoRequestBuilder.setInfoPolicy(GrpcConversions.toGrpc(infoPolicy));
        builder.setInfoRequest(infoRequestBuilder.build());
        return builder;
    }

    @Override
    void writeCommand(Command command) {
        // Nothing to do since there is no Aerospike payload.
    }

    @Override
    void onFailure(AerospikeException ae) {
        listener.onFailure(ae);
    }

    @Override
    void parseResult(Parser parser) {
        int resultCode = parser.parseResultCode();
        if (resultCode != ResultCode.OK) {
            throw new AerospikeException(resultCode);
        }
        Map<String, String> infoCommandResponse = parser.parseInfoResult();
        try {
            listener.onSuccess(infoCommandResponse);
        }
        catch (Throwable t) {
            logOnSuccessError(t);
        }
    }
}
