/*
 * Copyright (c) 2019 LINE Corporation. All rights reserved.
 * LINE Corporation PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 */

package com.ltv.aerospike.server.business;

import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.log4j.Logger;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.Bin;
import com.aerospike.client.Key;
import com.aerospike.client.Record;
import com.aerospike.client.listener.RecordSequenceListener;
import com.aerospike.client.listener.WriteListener;
import com.aerospike.client.policy.ScanPolicy;
import com.google.gson.Gson;
import com.ltv.aerospike.api.proto.DropBinServices.DropBinRequest;
import com.ltv.aerospike.api.proto.DropBinServices.DropBinResponse;
import com.ltv.aerospike.api.proto.RenameBinServices.RenameBinRequest;
import com.ltv.aerospike.api.proto.RenameBinServices.RenameBinResponse;
import com.ltv.aerospike.api.proto.ShowBinServices.ShowBinRequest;
import com.ltv.aerospike.api.proto.ShowBinServices.ShowBinResponse;
import com.ltv.aerospike.api.util.ErrorCode;
import com.ltv.aerospike.server.repository.Connector;
import com.ltv.aerospike.server.util.AppConstant;

import io.grpc.stub.StreamObserver;


public class BinBusiness extends BaseBusiness {

    private static Logger log = Logger.getLogger(BinBusiness.class.getSimpleName());
    public BinBusiness(StreamObserver responseObserver) {
        super(responseObserver);
    }

    public void showBin(ShowBinRequest request) {
        // get request param
        final String namespace = request.getNamespace();
        final String set = request.getSet();

        // validate null
        if(isNull(responseObserver, namespace, ShowBinResponse.newBuilder().setErrorCode(ErrorCode.INPUT_REQUIRE.getValue()).build())) return;
        if(isNull(responseObserver, set, ShowBinResponse.newBuilder().setErrorCode(ErrorCode.INPUT_REQUIRE.getValue()).build())) return;

        // validate empty
        final Record namespaceRecord = SessionBusiness.namespaces.get(namespace);
        if(isEmpty(responseObserver, namespaceRecord, ShowBinResponse.newBuilder().setErrorCode(ErrorCode.NOT_EXIST.getValue()).build())) return;
        final Record setRecord = SessionBusiness.sets.get(namespaceRecord.getString(AppConstant.KEY) + "_" + set);
        if(isEmpty(responseObserver, setRecord, ShowBinResponse.newBuilder().setErrorCode(ErrorCode.NOT_EXIST.getValue()).build())) return;

        // validate role
        if(!hasPermission(responseObserver, namespaceRecord, setRecord, AppConstant.PERMISSION_SELECT, ShowBinResponse.newBuilder().setErrorCode(ErrorCode.PERMISSION_REQUIRE.getValue()).build())) return;

        final Map<String, String> result = new HashMap();
        aeClient.scanAll(eventLoops.next(), new RecordSequenceListener() {
               @Override
               public void onRecord(Key key, Record record) throws AerospikeException {
                   if(result.size() < 1000 && record.getLong(AppConstant.DEL_FLAG) != 1) {
                       Map<String, Object> recordData = record.bins.entrySet().stream().collect(Collectors.toMap(
                               x -> x.getKey(),
                               x -> {
                                   if(x.getValue() instanceof Integer || x.getValue() instanceof Long) return String.valueOf(x.getValue());
                                   else return x.getValue();
                               }
                       ));
                       result.put((new Gson()).toJson(key), (new Gson()).toJson(recordData));
                   }
               }

               @Override
               public void onSuccess() {
                   response(responseObserver, ShowBinResponse.newBuilder()
                                                          .putAllBins(result)
                                                             .setErrorCode(ErrorCode.SUCCESS.getValue()).build());
               }

               @Override
               public void onFailure(AerospikeException e) {
                   responseObserver.onError(e);
                   responseObserver.onCompleted();
               }}, new ScanPolicy(), AEROSPIKE_NAMESPACE, String.valueOf(setRecord.getString(AppConstant.KEY)));
    };

    public void renameBin(RenameBinRequest request) {
        // get request param
        final String namespace = request.getNamespace();
        final String set = request.getSet();
        final String oldBin = request.getOldBin();
        final String newBin = request.getNewBin();

        // validate null
        if(isNull(responseObserver, namespace, RenameBinResponse.newBuilder().setErrorCode(ErrorCode.INPUT_REQUIRE.getValue()).build())) return;
        if(isNull(responseObserver, set, RenameBinResponse.newBuilder().setErrorCode(ErrorCode.INPUT_REQUIRE.getValue()).build())) return;
        if(isNull(responseObserver, oldBin, RenameBinResponse.newBuilder().setErrorCode(ErrorCode.INPUT_REQUIRE.getValue()).build())) return;
        if(isNull(responseObserver, newBin, RenameBinResponse.newBuilder().setErrorCode(ErrorCode.INPUT_REQUIRE.getValue()).build())) return;

        // validate empty
        final Record namespaceRecord = SessionBusiness.namespaces.get(namespace);
        if(isEmpty(responseObserver, namespaceRecord, RenameBinResponse.newBuilder().setErrorCode(ErrorCode.NOT_EXIST.getValue()).build())) return;
        final Record setRecord = SessionBusiness.sets.get(namespaceRecord.getString(AppConstant.KEY) + "_" + set);
        if(isEmpty(responseObserver, setRecord, RenameBinResponse.newBuilder().setErrorCode(ErrorCode.NOT_EXIST.getValue()).build())) return;

        // validate role
        if(!hasPermission(responseObserver, namespaceRecord, setRecord, AppConstant.PERMISSION_PUT, ShowBinResponse.newBuilder().setErrorCode(ErrorCode.PERMISSION_REQUIRE.getValue()).build())) return;

        aeClient.scanAll(eventLoops.next(), new RecordSequenceListener() {
            @Override
            public void onRecord(Key key, Record record) throws AerospikeException {
                aeClient.put(eventLoops.next(), new WriteListener() {
                    @Override
                    public void onSuccess(Key key) {
                        aeClient.put(eventLoops.next(), Connector.defaultWriteListener, null, key, Bin.asNull(oldBin));
                    }

                    @Override
                    public void onFailure(AerospikeException exception) {
                        responseObserver.onError(exception);
                        responseObserver.onCompleted();
                    }
                }, null, key, new Bin(newBin, record.getValue(oldBin)));
            }

            @Override
            public void onSuccess() {
                response(responseObserver, RenameBinResponse.newBuilder().setErrorCode(ErrorCode.SUCCESS.getValue()).build());
            }

            @Override
            public void onFailure(AerospikeException e) {
                responseObserver.onError(e);
                responseObserver.onCompleted();
            }}, new ScanPolicy(), AEROSPIKE_NAMESPACE, String.valueOf(setRecord.getString(AppConstant.KEY)));
    }

    public void dropBin(DropBinRequest request) {
        // get request param
        final String namespace = request.getNamespace();
        final String set = request.getSet();
        final String bin = request.getBin();

        // validate null
        if(isNull(responseObserver, namespace, DropBinResponse.newBuilder().setErrorCode(ErrorCode.INPUT_REQUIRE.getValue()).build())) return;
        if(isNull(responseObserver, set, DropBinResponse.newBuilder().setErrorCode(ErrorCode.INPUT_REQUIRE.getValue()).build())) return;
        if(isNull(responseObserver, bin, DropBinResponse.newBuilder().setErrorCode(ErrorCode.INPUT_REQUIRE.getValue()).build())) return;

        // validate empty
        final Record namespaceRecord = SessionBusiness.namespaces.get(namespace);
        if(isEmpty(responseObserver, namespaceRecord, DropBinResponse.newBuilder().setErrorCode(ErrorCode.NOT_EXIST.getValue()).build())) return;
        final Record setRecord = SessionBusiness.sets.get(namespaceRecord.getString(AppConstant.KEY) + "_" + set);
        if(isEmpty(responseObserver, setRecord, DropBinResponse.newBuilder().setErrorCode(ErrorCode.NOT_EXIST.getValue()).build())) return;

        // validate role
        if(!hasPermission(responseObserver, namespaceRecord, setRecord, AppConstant.PERMISSION_DELETE, DropBinResponse.newBuilder().setErrorCode(ErrorCode.PERMISSION_REQUIRE.getValue()).build())) return;

        aeClient.scanAll(eventLoops.next(), new RecordSequenceListener() {
            @Override
            public void onRecord(Key key, Record record) throws AerospikeException {
                aeClient.put(eventLoops.next(), Connector.defaultWriteListener, null, key, Bin.asNull(bin));
            }

            @Override
            public void onSuccess() {
                response(responseObserver, DropBinResponse.newBuilder().setErrorCode(ErrorCode.SUCCESS.getValue()).build());
            }

            @Override
            public void onFailure(AerospikeException e) {
                responseObserver.onError(e);
                responseObserver.onCompleted();
            }}, new ScanPolicy(), AEROSPIKE_NAMESPACE, String.valueOf(setRecord.getString(AppConstant.KEY)));
    };
}
