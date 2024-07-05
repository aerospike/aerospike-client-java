package com.aerospike.client.proxy;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.ResultCode;
import com.aerospike.client.command.Command;
import com.aerospike.client.listener.InfoListener;
import com.aerospike.client.policy.InfoPolicy;
import com.aerospike.client.policy.Policy;
import com.aerospike.client.proxy.grpc.GrpcCallExecutor;
import com.aerospike.client.proxy.grpc.GrpcConversions;
import com.aerospike.proxy.client.Kvs;
import com.aerospike.proxy.client.InfoGrpc;
import io.grpc.*;

import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class InfoCommandProxy extends SingleCommandProxy {

    private final InfoListener listener;
    private final String[] commands;
    private final InfoPolicy infoPolicy;
    private final GrpcCallExecutor executor;
    final Policy policy;

    public InfoCommandProxy(GrpcCallExecutor executor, InfoListener listener, InfoPolicy policy, String... commands) {
        super(InfoGrpc.getInfoMethod(), executor, createPolicy(policy));
        this.executor = executor;
        this.infoPolicy = policy;
        this.listener = listener;
        this.commands = commands;
        this.policy = createPolicy(policy);
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
    void execute(){
        executeCommand();
    }

    private void executeCommand() {
        Kvs.AerospikeRequestPayload.Builder builder = getRequestBuilder();

        ManagedChannel channel = executor.getChannel();
        InfoGrpc.InfoBlockingStub stub = InfoGrpc.newBlockingStub(channel);
        try{
            Kvs.AerospikeRequestPayload request = builder.build();
            Kvs.AerospikeResponsePayload response = stub.info(request);
            inDoubt |= response.getInDoubt();
            onResponse(response);
        }catch (Throwable t) {
            inDoubt = true;
            onFailure(t);
        } finally {
            // Shut down the channel
            channel.shutdown();
        }
    }


    @Override
    void onFailure(AerospikeException ae) {

    }

    void onFailure(Throwable t) {
        AerospikeException ae;

        try {
            if (t instanceof AerospikeException) {
                ae = (AerospikeException)t;
                ae.setPolicy(policy);
            }
            else if (t instanceof StatusRuntimeException) {
                StatusRuntimeException sre = (StatusRuntimeException)t;
                Status.Code code = sre.getStatus().getCode();

                if (code == Status.Code.UNAVAILABLE) {
                    if (retry()) {
                        return;
                    }
                }
                ae = GrpcConversions.toAerospike(sre, policy, 1);
            }
            else {
                ae = new AerospikeException(ResultCode.CLIENT_ERROR, t);
            }
        }
        catch (AerospikeException ae2) {
            ae = ae2;
        }
        catch (Throwable t2) {
            ae = new AerospikeException(ResultCode.CLIENT_ERROR, t2);
        }

        notifyFailure(ae);
    }

    @Override
    void onResponse(Kvs.AerospikeResponsePayload response){
        String infoResponse = String.valueOf(response.getPayload());
        Map<String, String> infoCommandResponse = createInfoMap(infoResponse);
        try {
            listener.onSuccess(infoCommandResponse);
        }
        catch (Throwable t) {
            logOnSuccessError(t);
        }
    }

    public static Map<String, String> createInfoMap(String byteStringRepresentation) {
        Map<String, String> infoMap = new HashMap<>();

        String contents = getContents(byteStringRepresentation);

        if (contents != null && !contents.isEmpty()) {
            String[] commands = contents.split("\\\\n");

            for (String command : commands) {
                String[] keyValue = command.split("\\\\t", 2);

                if (keyValue.length == 2) {
                    infoMap.put(keyValue[0], keyValue[1]);
                }
            }
        }
        return infoMap;
    }

    public static String getContents(String byteStringRepresentation) {
        String regex = "contents=\"(.*?)\"";

        Pattern pattern = Pattern.compile(regex);
        Matcher matcher = pattern.matcher(byteStringRepresentation);

        if (matcher.find()) {
            return matcher.group(1);
        }

        return null;
    }

    @Override
    void parseResult(Parser parser) {

    }
}
