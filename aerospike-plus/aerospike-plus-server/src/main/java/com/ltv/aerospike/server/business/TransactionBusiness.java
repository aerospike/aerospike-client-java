/*
 * Copyright (c) 2019 LINE Corporation. All rights reserved.
 * LINE Corporation PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 */

package com.ltv.aerospike.server.business;

import java.util.HashMap;
import java.util.List;
import java.util.stream.Collectors;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.Bin;
import com.aerospike.client.Key;
import com.aerospike.client.Record;
import com.aerospike.client.listener.WriteListener;
import com.aerospike.client.policy.RecordExistsAction;
import com.aerospike.client.policy.WritePolicy;
import com.google.gson.internal.LinkedTreeMap;
import com.ltv.aerospike.api.proto.DeleteServices.DeleteRequest;
import com.ltv.aerospike.api.proto.DeleteServices.DeleteResponse;
import com.ltv.aerospike.api.proto.PutServices.PutRequest;
import com.ltv.aerospike.api.proto.PutServices.PutResponse;
import com.ltv.aerospike.api.util.ErrorCode;
import com.ltv.aerospike.server.util.AppConstant;

import io.grpc.stub.StreamObserver;

public class TransactionBusiness extends BaseBusiness {

    public TransactionBusiness(StreamObserver responseObserver) {
        super(responseObserver);
    }

    private Record getSetRecord(String namespace, String set) {
        Record namespaceRecord = SessionBusiness.namespaces.get(namespace);
        return SessionBusiness.sets.get(namespaceRecord.getString(AppConstant.KEY) + "_" + set);
    }

    public void put(PutRequest request) {
        // get request param
        String namespace = request.getNamespace();
        String set = request.getSet();
        String ids = request.getKey();
        WritePolicy policy = new WritePolicy();

        if(request.getExpiration() > 0) {
            policy.expiration = request.getExpiration();
        }

        switch (request.getWritePolicy().getNumber()) {
            case PutRequest.RecordExistsAction.CREATE_ONLY_VALUE:
                policy.recordExistsAction = RecordExistsAction.CREATE_ONLY;
                break;
            case PutRequest.RecordExistsAction.UPDATE_ONLY_VALUE:
                policy.recordExistsAction = RecordExistsAction.UPDATE_ONLY;
                break;
            case PutRequest.RecordExistsAction.REPLACE_VALUE:
                policy.recordExistsAction = RecordExistsAction.REPLACE;
                break;
            case PutRequest.RecordExistsAction.REPLACE_ONLY_VALUE:
                policy.recordExistsAction = RecordExistsAction.REPLACE_ONLY;
                break;
            default:
                policy.recordExistsAction = RecordExistsAction.UPDATE;
        }
        HashMap<String, Object> mapBins = new HashMap();
        mapBins.putAll(request.getStringParamsMap());
        mapBins.putAll(request.getBooleanParamsMap());
        mapBins.putAll(request.getLongParamsMap());
        mapBins.putAll(request.getLongParamsMap());
        mapBins.putAll(request.getFloatParamsMap());
        mapBins.putAll(request.getDoubleParamsMap());
        mapBins.putAll(request.getBytesParamsMap());

        List<Bin> lstBins = mapBins.entrySet().stream().map(x -> new Bin(x.getKey(), x.getValue())).collect(Collectors.toList());
        final Bin[] bins = lstBins.toArray(new Bin[lstBins.size()]);

        // validate null
        if(isNull(responseObserver, namespace, PutResponse.newBuilder().setErrorCode(ErrorCode.INPUT_REQUIRE.getValue()).build())) return;
        if(isNull(responseObserver, set, PutResponse.newBuilder().setErrorCode(ErrorCode.INPUT_REQUIRE.getValue()).build())) return;
        if(isNull(responseObserver, ids, PutResponse.newBuilder().setErrorCode(ErrorCode.INPUT_REQUIRE.getValue()).build())) return;
        if(isNull(responseObserver, bins, PutResponse.newBuilder().setErrorCode(ErrorCode.INPUT_REQUIRE.getValue()).build())) return;

        // validate empty
        final Record namespaceRecord = SessionBusiness.namespaces.get(namespace);
        if(isEmpty(responseObserver, namespaceRecord, PutResponse.newBuilder().setErrorCode(ErrorCode.NOT_EXIST.getValue()).build())) return;
        final Record setRecord = SessionBusiness.sets.get(namespaceRecord.getString(AppConstant.KEY) + "_" + set);
        if(isEmpty(responseObserver, setRecord, PutResponse.newBuilder().setErrorCode(ErrorCode.NOT_EXIST.getValue()).build())) return;

        // validate role
        if(!hasPermission(responseObserver, namespaceRecord, setRecord, AppConstant.PERMISSION_PUT, PutResponse.newBuilder().setErrorCode(ErrorCode.PERMISSION_REQUIRE.getValue()).build())) return;

        final Key tableKey = new Key(AEROSPIKE_NAMESPACE, String.valueOf(getSetRecord(namespace, set).getString(AppConstant.KEY)), (String)ids);
        aeClient.put(eventLoops.next(), new WriteListener() {
            @Override
            public void onSuccess(Key key) {
                response(responseObserver, PutResponse.newBuilder().setErrorCode(ErrorCode.SUCCESS.getValue()).build());
            }

            @Override
            public void onFailure(AerospikeException exception) {
                responseObserver.onError(exception);
                responseObserver.onCompleted();
            }
        }, policy, tableKey, (Bin[]) bins);
    }

    public void delete(DeleteRequest request) {
        // get request param
        String namespace = request.getNamespace();
        String set = request.getSet();
        Object key = request.getKey();

        // validate null
        if(isNull(responseObserver, namespace, DeleteResponse.newBuilder().setErrorCode(ErrorCode.INPUT_REQUIRE.getValue()).build())) return;
        if(isNull(responseObserver, namespace, DeleteResponse.newBuilder().setErrorCode(ErrorCode.INPUT_REQUIRE.getValue()).build())) return;
        if(isNull(responseObserver, set, DeleteResponse.newBuilder().setErrorCode(ErrorCode.INPUT_REQUIRE.getValue()).build())) return;
        if(isNull(responseObserver, key, DeleteResponse.newBuilder().setErrorCode(ErrorCode.INPUT_REQUIRE.getValue()).build())) return;

        // validate empty
        final Record namespaceRecord = SessionBusiness.namespaces.get(namespace);
        if(isEmpty(responseObserver, namespaceRecord, DeleteResponse.newBuilder().setErrorCode(ErrorCode.NOT_EXIST.getValue()).build())) return;
        final Record setRecord = SessionBusiness.sets.get(namespaceRecord.getString(AppConstant.KEY) + "_" + set);
        if(isEmpty(responseObserver, setRecord, DeleteResponse.newBuilder().setErrorCode(ErrorCode.NOT_EXIST.getValue()).build())) return;

        // validate role
        if(!hasPermission(responseObserver, namespaceRecord, setRecord, AppConstant.PERMISSION_DELETE, DeleteResponse.newBuilder().setErrorCode(ErrorCode.PERMISSION_REQUIRE.getValue()).build())) return;

        WritePolicy policy = new WritePolicy();
        policy.expiration = 1;

        final Key deleteKey = new Key(AEROSPIKE_NAMESPACE,
                                      String.valueOf(getSetRecord(namespace, set).getString(AppConstant.KEY)), (String) key);
        aeClient.put(eventLoops.next(), new WriteListener() {
            @Override
            public void onSuccess(Key key) {
                response(responseObserver, DeleteResponse.newBuilder().setErrorCode(ErrorCode.SUCCESS.getValue()).build());
            }

            @Override
            public void onFailure(AerospikeException exception) {
                responseObserver.onError(exception);
                responseObserver.onCompleted();
            }
        }, policy, deleteKey, new Bin(AppConstant.DEL_FLAG, 1));
    }

    public void beginTransaction(final LinkedTreeMap msg) {
        
    }

    public void commit(final LinkedTreeMap msg) {
        
    }

    public void rollback(final LinkedTreeMap msg) {
        
    }
}
