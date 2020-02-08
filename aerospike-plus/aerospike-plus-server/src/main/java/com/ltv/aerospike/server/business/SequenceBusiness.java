/*
 * Copyright (c) 2019 LINE Corporation. All rights reserved.
 * LINE Corporation PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 */

package com.ltv.aerospike.server.business;

import java.util.Map;
import java.util.stream.Collectors;

import org.apache.log4j.Logger;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.Bin;
import com.aerospike.client.Key;
import com.aerospike.client.Operation;
import com.aerospike.client.Record;
import com.aerospike.client.listener.DeleteListener;
import com.aerospike.client.listener.RecordListener;
import com.aerospike.client.listener.WriteListener;
import com.google.gson.Gson;
import com.ltv.aerospike.api.proto.CreateSequenceServices.CreateSequenceRequest;
import com.ltv.aerospike.api.proto.CreateSequenceServices.CreateSequenceResponse;
import com.ltv.aerospike.api.proto.DropSequenceServices.DropSequenceRequest;
import com.ltv.aerospike.api.proto.DropSequenceServices.DropSequenceResponse;
import com.ltv.aerospike.api.proto.GetSequenceServices.GetSequenceRequest;
import com.ltv.aerospike.api.proto.GetSequenceServices.GetSequenceResponse;
import com.ltv.aerospike.api.proto.ShowSequenceServices.ShowSequenceRequest;
import com.ltv.aerospike.api.proto.ShowSequenceServices.ShowSequenceResponse;
import com.ltv.aerospike.api.util.ErrorCode;
import com.ltv.aerospike.server.util.AppConstant;

import io.grpc.stub.StreamObserver;

public class SequenceBusiness extends BaseBusiness {

    private static Logger log = Logger.getLogger(NamespaceBusiness.class.getSimpleName());
    public SequenceBusiness(StreamObserver responseObserver) {
        super(responseObserver);
    }

    public void showSequence(ShowSequenceRequest request) {
        // get request param
        String namespace = request.getNamespace();

        // validate null
        if(isNull(responseObserver, namespace, ShowSequenceResponse.newBuilder().setErrorCode(ErrorCode.INPUT_REQUIRE.getValue()).build())) return;

        // validate empty
        Record namespaceRecord = SessionBusiness.namespaces.get(namespace);
        if(isEmpty(responseObserver, namespaceRecord, ShowSequenceResponse.newBuilder().setErrorCode(ErrorCode.NOT_EXIST.getValue()).build())) return;

        // validate role
        if(!hasRole(responseObserver, AppConstant.ROLE_DQL, namespaceRecord, ShowSequenceResponse.newBuilder().setErrorCode(ErrorCode.ROLE_REQUIRE.getValue()).build())) return;

        Map<String, String> sequenceRecords = SessionBusiness.sequences.entrySet().stream()
                                 .filter(x -> namespace.equals(x.getValue().getString("namespace")))
                                 .collect(Collectors.toMap(
                                        x -> x.getValue().getString("name"),
                                        x -> (new Gson()).toJson(x.getValue().bins)
                                ));

        response(responseObserver, ShowSequenceResponse.newBuilder()
                                                       .putAllSequences(sequenceRecords)
                                                       .setErrorCode(ErrorCode.SUCCESS.getValue()).build());
    }

    public void createSequence(CreateSequenceRequest request) {
        // get request param
        String namespace = request.getNamespace();
        String sequence = request.getSequence();
        Integer start = request.getStart();

        // validate null
        if(isNull(responseObserver, namespace, CreateSequenceResponse.newBuilder().setErrorCode(ErrorCode.INPUT_REQUIRE.getValue()).build())) return;
        if(isNull(responseObserver, sequence, CreateSequenceResponse.newBuilder().setErrorCode(ErrorCode.INPUT_REQUIRE.getValue()).build())) return;
        if(isNull(responseObserver, start, CreateSequenceResponse.newBuilder().setErrorCode(ErrorCode.INPUT_REQUIRE.getValue()).build())) return;

        // validate empty
        final Record namespaceRecord = SessionBusiness.namespaces.get(namespace);
        if(isEmpty(responseObserver, namespaceRecord, CreateSequenceResponse.newBuilder().setErrorCode(ErrorCode.NOT_EXIST.getValue()).build())) return;

        // validate role
        if(!hasRole(responseObserver, AppConstant.ROLE_DDL, namespaceRecord, CreateSequenceResponse.newBuilder().setErrorCode(ErrorCode.ROLE_REQUIRE.getValue()).build())) return;

        Key sequenceKey = new Key(AEROSPIKE_NAMESPACE, AppConstant.TABLE_SEQUENCE_ID, sequence);
        aeClient.put(eventLoops.next(), new WriteListener() {
             @Override
             public void onSuccess(Key key) {
                 SessionBusiness.sequences.put(sequence, aeClient.get(null, sequenceKey));
                 response(responseObserver, CreateSequenceResponse.newBuilder().setErrorCode(ErrorCode.SUCCESS.getValue()).build());
             }

             @Override
             public void onFailure(AerospikeException exception) {
                responseObserver.onError(exception);
                responseObserver.onCompleted();
             }
         }, null,
         sequenceKey,
         new Bin("namespace", namespace),
         new Bin(AppConstant.KEY, sequence),
         new Bin("name", sequence),
         new Bin("value", 1));
    }

    public void dropSequence(DropSequenceRequest request) {
        // get request param
        String namespace = request.getNamespace();
        String sequence = request.getSequence();

        // validate null
        if(isNull(responseObserver, namespace, DropSequenceResponse.newBuilder().setErrorCode(ErrorCode.INPUT_REQUIRE.getValue()).build())) return;
        if(isNull(responseObserver, sequence, DropSequenceResponse.newBuilder().setErrorCode(ErrorCode.INPUT_REQUIRE.getValue()).build())) return;

        // validate empty
        final Record namespaceRecord = SessionBusiness.namespaces.get(namespace);
        if(isEmpty(responseObserver, namespaceRecord, DropSequenceResponse.newBuilder().setErrorCode(ErrorCode.NOT_EXIST.getValue()).build())) return;

        // validate role
        if(!hasRole(responseObserver, AppConstant.ROLE_DDL, namespaceRecord, DropSequenceResponse.newBuilder().setErrorCode(ErrorCode.ROLE_REQUIRE.getValue()).build())) return;

        final Key sequenceKey = new Key(AEROSPIKE_NAMESPACE, AppConstant.TABLE_SEQUENCE_ID, sequence);
        aeClient.put(eventLoops.next(), new WriteListener() {
            @Override
            public void onSuccess(Key key) {
                aeClient.delete(eventLoops.next(), new DeleteListener() {
                    @Override
                    public void onSuccess(Key key, boolean existed) {
                        SessionBusiness.sequences.remove(sequence);
                        response(responseObserver, DropSequenceResponse.newBuilder().setErrorCode(ErrorCode.SUCCESS.getValue()).build());
                    }

                    @Override
                    public void onFailure(AerospikeException exception) {
                        responseObserver.onError(exception);
                        responseObserver.onCompleted();
                    }
                }, null, sequenceKey);
            }

            @Override
            public void onFailure(AerospikeException exception) {
                responseObserver.onError(exception);
                responseObserver.onCompleted();
            }
        }, null, sequenceKey, new Bin(AppConstant.DEL_FLAG,1));

    };

    public void getSequence(GetSequenceRequest request) {
        // get request param
        String namespace = request.getNamespace();
        String sequence = request.getSequence();

        // validate null
        if(isNull(responseObserver, namespace, GetSequenceResponse.newBuilder().setErrorCode(ErrorCode.INPUT_REQUIRE.getValue()).build())) return;
        if(isNull(responseObserver, sequence, GetSequenceResponse.newBuilder().setErrorCode(ErrorCode.INPUT_REQUIRE.getValue()).build())) return;

        // validate empty
        final Record namespaceRecord = SessionBusiness.namespaces.get(namespace);
        if(isEmpty(responseObserver, namespaceRecord, GetSequenceResponse.newBuilder().setErrorCode(ErrorCode.NOT_EXIST.getValue()).build())) return;

        // validate role
        if(!hasRole(responseObserver, AppConstant.ROLE_DQL, namespaceRecord, GetSequenceResponse.newBuilder().setErrorCode(ErrorCode.ROLE_REQUIRE.getValue()).build())) return;

        final Key setSequence = new Key(AEROSPIKE_NAMESPACE, AppConstant.TABLE_SEQUENCE_ID, sequence);
        aeClient.operate(eventLoops.next(), new RecordListener() {
             @Override
             public void onSuccess(Key key, Record record) {
                 response(responseObserver, GetSequenceResponse.newBuilder().setValue(record.getInt("value")).setErrorCode(ErrorCode.SUCCESS.getValue()).build());
             }

             @Override
             public void onFailure(AerospikeException exception) {
                 responseObserver.onError(exception);
                 responseObserver.onCompleted();
             }
         }, null, setSequence,
         Operation.add(new Bin("value", 1)), Operation.get());
    }
}
