/*
 * Copyright (c) 2019 LINE Corporation. All rights reserved.
 * LINE Corporation PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 */

package com.ltv.aerospike.server.business;

import java.util.HashMap;
import java.util.Map;

import org.apache.log4j.Logger;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.Bin;
import com.aerospike.client.Key;
import com.aerospike.client.Record;
import com.aerospike.client.listener.DeleteListener;
import com.aerospike.client.listener.WriteListener;
import com.aerospike.client.policy.RecordExistsAction;
import com.aerospike.client.policy.WritePolicy;
import com.aerospike.client.query.IndexType;
import com.google.gson.Gson;
import com.ltv.aerospike.api.proto.CreateIndexServices.CreateIndexRequest;
import com.ltv.aerospike.api.proto.CreateIndexServices.CreateIndexResponse;
import com.ltv.aerospike.api.proto.DropIndexServices.DropIndexRequest;
import com.ltv.aerospike.api.proto.DropIndexServices.DropIndexResponse;
import com.ltv.aerospike.api.proto.ShowIndexServices.ShowIndexRequest;
import com.ltv.aerospike.api.proto.ShowIndexServices.ShowIndexResponse;
import com.ltv.aerospike.api.util.ErrorCode;
import com.ltv.aerospike.server.util.AppConstant;

import io.grpc.stub.StreamObserver;

public class IndexBusiness extends BaseBusiness {

    private static Logger log = Logger.getLogger(IndexBusiness.class.getSimpleName());

    public IndexBusiness(StreamObserver responseObserver) {
        super(responseObserver);
    }

    public void showIndex(ShowIndexRequest request) {
        // get request param
        final String namespace = request.getNamespace();

        // validate null
        if(isNull(responseObserver, namespace, ShowIndexResponse.newBuilder().setErrorCode(ErrorCode.INPUT_REQUIRE.getValue()).build())) return;

        // validate empty
        Record namespaceRecord = SessionBusiness.namespaces.get(namespace);
        if(isEmpty(responseObserver, namespaceRecord, ShowIndexResponse.newBuilder().setErrorCode(ErrorCode.NOT_EXIST.getValue()).build())) return;

        // validate role
        if(!hasRole(responseObserver, AppConstant.ROLE_DQL, namespaceRecord, ShowIndexResponse.newBuilder().setErrorCode(ErrorCode.ROLE_REQUIRE.getValue()).build())) return;

        final Map<String, String> indexRecords = new HashMap();
        for(Map.Entry<String, Record> entry : SessionBusiness.indexs.entrySet()) {
            if(namespace.equals(entry.getValue().getString("namespace"))) {
                indexRecords.put(entry.getValue().getString("name"), (new Gson()).toJson(entry.getValue().bins));
            }
        }

        response(responseObserver, ShowIndexResponse.newBuilder()
                                                      .putAllIndexs(indexRecords)
                                                      .setErrorCode(ErrorCode.SUCCESS.getValue()).build());
    }

    public void createIndex(CreateIndexRequest request) {
        // get request param
        final String namespace = request.getNamespace();
        final String set = request.getSet();
        final String bin = request.getBin();
        final String index = request.getIndex();

        // validate null
        if(isNull(responseObserver, namespace, CreateIndexResponse.newBuilder().setErrorCode(ErrorCode.INPUT_REQUIRE.getValue()).build())) return;
        if(isNull(responseObserver, set, CreateIndexResponse.newBuilder().setErrorCode(ErrorCode.INPUT_REQUIRE.getValue()).build())) return;
        if(isNull(responseObserver, bin, CreateIndexResponse.newBuilder().setErrorCode(ErrorCode.INPUT_REQUIRE.getValue()).build())) return;
        if(isNull(responseObserver, index, CreateIndexResponse.newBuilder().setErrorCode(ErrorCode.INPUT_REQUIRE.getValue()).build())) return;
        if(isNull(responseObserver, request.getIndexType(), CreateIndexResponse.newBuilder().setErrorCode(ErrorCode.INPUT_REQUIRE.getValue()).build())) return;

        IndexType indexType = null;
        switch (request.getIndexType().getNumber()) {
            case CreateIndexRequest.IndexType.STRING_VALUE:
                indexType = IndexType.STRING;
                break;
            case CreateIndexRequest.IndexType.GEO2DSPHERE_VALUE:
                indexType = IndexType.GEO2DSPHERE;
                break;
            default:
                indexType = IndexType.NUMERIC;
        }
        final IndexType type = indexType;

        // validate empty
        final Record namespaceRecord = SessionBusiness.namespaces.get(namespace);
        if(isEmpty(responseObserver, namespaceRecord, CreateIndexResponse.newBuilder().setErrorCode(ErrorCode.NOT_EXIST.getValue()).build())) return;
        String oldSetId = namespaceRecord.getString(AppConstant.KEY) + "_" + set;
        final Record setRecord = SessionBusiness.sets.get(oldSetId);
        if(isEmpty(responseObserver, setRecord, CreateIndexResponse.newBuilder().setErrorCode(ErrorCode.NOT_EXIST.getValue()).build())) return;

        // validate role
        if(!hasRole(responseObserver, AppConstant.ROLE_DDL, namespaceRecord, CreateIndexResponse.newBuilder().setErrorCode(ErrorCode.ROLE_REQUIRE.getValue()).build())) return;

        final Key indexKey = new Key(AEROSPIKE_NAMESPACE, AppConstant.TABLE_INDEX_ID, index);
        WritePolicy policy = new WritePolicy();
        policy.recordExistsAction = RecordExistsAction.CREATE_ONLY;
        aeClient.put(eventLoops.next(), new WriteListener() {
             @Override
             public void onSuccess(Key key) {
                 aeClient.createIndex( null, AEROSPIKE_NAMESPACE, String.valueOf(setRecord.getString(AppConstant.KEY)), index, bin, type);
                 SessionBusiness.indexs.put(index, aeClient.get(null, indexKey));
                 response(responseObserver, CreateIndexResponse.newBuilder().setErrorCode(ErrorCode.SUCCESS.getValue()).build());
             }

             @Override
             public void onFailure(AerospikeException exception) {
                 responseObserver.onError(exception);
                 responseObserver.onCompleted();
             }
         }, policy, indexKey,
         new Bin(AppConstant.KEY, index),
         new Bin("name", index),
         new Bin("namespace", namespace),
         new Bin("set", set),
         new Bin("bin", bin),
         new Bin("indexType", indexType.name()));
    }

    public void dropIndex(DropIndexRequest request) {
        // get request param
        String namespace = request.getNamespace();
        String index = request.getIndex();

        // validate null
        if(isNull(responseObserver, namespace, DropIndexResponse.newBuilder().setErrorCode(ErrorCode.INPUT_REQUIRE.getValue()).build())) return;
        if(isNull(responseObserver, index, DropIndexResponse.newBuilder().setErrorCode(ErrorCode.INPUT_REQUIRE.getValue()).build())) return;

        // validate empty
        final Record namespaceRecord = SessionBusiness.namespaces.get(namespace);
        if(isEmpty(responseObserver, namespaceRecord, DropIndexResponse.newBuilder().setErrorCode(ErrorCode.NOT_EXIST.getValue()).build())) return;

        // validate role
        if(!hasRole(responseObserver, AppConstant.ROLE_DDL, namespaceRecord, DropIndexResponse.newBuilder().setErrorCode(ErrorCode.ROLE_REQUIRE.getValue()).build())) return;

        final Key indexKey = new Key(AEROSPIKE_NAMESPACE, AppConstant.TABLE_INDEX_ID, index);
        aeClient.put(eventLoops.next(), new WriteListener() {
            @Override
            public void onSuccess(Key key) {
                aeClient.delete(eventLoops.next(), new DeleteListener() {
                    @Override
                    public void onSuccess(Key key, boolean existed) {
                        SessionBusiness.indexs.remove(index);
                        aeClient.dropIndex(null, AEROSPIKE_NAMESPACE, null, index);
                        response(responseObserver, DropIndexResponse.newBuilder().setErrorCode(ErrorCode.SUCCESS.getValue()).build());
                    }

                    @Override
                    public void onFailure(AerospikeException exception) {
                        responseObserver.onError(exception);
                        responseObserver.onCompleted();
                    }
                }, null, indexKey);
            }

            @Override
            public void onFailure(AerospikeException exception) {
                responseObserver.onError(exception);
                responseObserver.onCompleted();
            }
        }, null, indexKey, new Bin(AppConstant.DEL_FLAG,1));
    }

}
