/*
 * Copyright (c) 2019 LINE Corporation. All rights reserved.
 * LINE Corporation PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 */

package com.ltv.aerospike.server.business;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.Bin;
import com.aerospike.client.Key;
import com.aerospike.client.Operation;
import com.aerospike.client.Record;
import com.aerospike.client.Value;
import com.aerospike.client.listener.RecordListener;
import com.aerospike.client.listener.WriteListener;
import com.aerospike.client.policy.Policy;
import com.aerospike.helper.query.KeyRecordIterator;
import com.aerospike.helper.query.Qualifier;
import com.aerospike.helper.query.Qualifier.FilterOperation;
import com.google.gson.Gson;
import com.ltv.aerospike.api.proto.CreateSetServices.CreateSetRequest;
import com.ltv.aerospike.api.proto.CreateSetServices.CreateSetResponse;
import com.ltv.aerospike.api.proto.DropSetServices.DropSetRequest;
import com.ltv.aerospike.api.proto.DropSetServices.DropSetResponse;
import com.ltv.aerospike.api.proto.RenameSetServices.RenameSetRequest;
import com.ltv.aerospike.api.proto.RenameSetServices.RenameSetResponse;
import com.ltv.aerospike.api.proto.ShowSetServices.ShowSetRequest;
import com.ltv.aerospike.api.proto.ShowSetServices.ShowSetResponse;
import com.ltv.aerospike.api.proto.TruncateSetServices.TruncateSetRequest;
import com.ltv.aerospike.api.proto.TruncateSetServices.TruncateSetResponse;
import com.ltv.aerospike.api.util.ErrorCode;
import com.ltv.aerospike.server.util.AppConstant;

import io.grpc.stub.StreamObserver;

public class SetBusiness extends BaseBusiness {

    private static Logger log = Logger.getLogger(SetBusiness.class.getSimpleName());

    public SetBusiness(StreamObserver responseObserver) {
        super(responseObserver);
    }

    public void showSet(ShowSetRequest request) {
        // get request param
        final String namespace = request.getNamespace();

        // validate null
        if(isNull(responseObserver, namespace, ShowSetResponse.newBuilder().setErrorCode(ErrorCode.INPUT_REQUIRE.getValue()).build())) return;

        // validate empty
        Record namespaceRecord = SessionBusiness.namespaces.get(namespace);
        if(isEmpty(responseObserver, namespaceRecord, ShowSetResponse.newBuilder().setErrorCode(ErrorCode.NOT_EXIST.getValue()).build())) return;

        // validate role
        if(!hasRole(responseObserver, AppConstant.ROLE_DQL, namespaceRecord, ShowSetResponse.newBuilder().setErrorCode(ErrorCode.ROLE_REQUIRE.getValue()).build())) return;

        Map<String, String> setRecords = new HashMap();
        for(Map.Entry<String, Record> entry : SessionBusiness.sets.entrySet()) {
            if(entry.getValue() != null && namespaceRecord.getString(AppConstant.KEY) != null
               && namespaceRecord.getString(AppConstant.KEY).equals(entry.getValue().getString("namespace"))) {
                setRecords.put(entry.getValue().getString("name"), (new Gson()).toJson(entry.getValue().bins));
            }
        }

        response(responseObserver, ShowSetResponse.newBuilder()
                                                  .putAllSets(setRecords)
                                                  .setErrorCode(ErrorCode.SUCCESS.getValue()).build());
    };

    public void createSet(CreateSetRequest request) {
        try {
            // get request param
            final String namespace = request.getNamespace();
            final String set = request.getSet();
            Map<String, String> mapInfo = new HashMap();
            mapInfo.putAll(request.getStringParamsMap());

            // validate null
            if (isNull(responseObserver, set,
                       CreateSetResponse.newBuilder().setErrorCode(ErrorCode.INPUT_REQUIRE.getValue()).build()))
                return;
            if (isNull(responseObserver, namespace,
                       CreateSetResponse.newBuilder().setErrorCode(ErrorCode.INPUT_REQUIRE.getValue()).build()))
                return;

            // validate empty
            final Record namespaceRecord = SessionBusiness.namespaces.get(namespace);
            if (isEmpty(responseObserver, namespaceRecord,
                        CreateSetResponse.newBuilder().setErrorCode(ErrorCode.NOT_EXIST.getValue()).build()))
                return;

            // validate role
            if (!hasRole(responseObserver, AppConstant.ROLE_DDL, namespaceRecord,
                         CreateSetResponse.newBuilder().setErrorCode(ErrorCode.ROLE_REQUIRE.getValue())
                                          .build())) return;

            Record setRecord = SessionBusiness.sets.get(namespaceRecord.getString(AppConstant.KEY) + "_" + set);
            String recordId = null;
            // Check if not exists , create new record
            if (setRecord == null) {
                // reuse deleted set id because Aerospike only delete record and keep set information after call deleted command
                KeyRecordIterator it = queryEngine.select(AEROSPIKE_NAMESPACE,
                                                          AppConstant.TABLE_SET_ID, null,
                                                          new Qualifier(AppConstant.DEL_FLAG,
                                                                        FilterOperation.EQ, Value.get(1)));
                try {
                    if (it.hasNext()) setRecord = it.next().record;
                } finally {
                    it.close();
                }
                recordId = "1";
                if (mapInfo.get("meta") == null) {
                    Map<String, String> meta = new HashMap();
                    meta.put(AppConstant.KEY, AppConstant.STRING);
                    mapInfo.put("meta", new Gson().toJson(meta));
                }
            }
            final String id = recordId;

            // save new set
            if (setRecord != null) {
                createSet(namespaceRecord, setRecord.getString(AppConstant.KEY), set, id, mapInfo);
            } else {
                final Key setSequence = new Key(AEROSPIKE_NAMESPACE, AppConstant.TABLE_SEQUENCE_ID,
                                                AppConstant.TABLE_SET_ID);
                aeClient.operate(eventLoops.next(), new RecordListener() {
                                     @Override
                                     public void onSuccess(Key key, Record record) {
                                         createSet(namespaceRecord, "" + record.getLong("value"), set, id, mapInfo);
                                     }

                                     @Override
                                     public void onFailure(AerospikeException exception) {
                                         responseObserver.onError(exception);
                                         responseObserver.onCompleted();
                                     }
                                 }, null, setSequence,
                                 Operation.add(new Bin("value", 1)), Operation.get());
            }
        } catch (Exception ex) {
            log.error("Create set failed", ex);
        }
    }

    private void createSet(Record namespaceRecord, String setId, String set, String recordId, Map setInfo) {
        // save set information
        Key setKey = new Key(AEROSPIKE_NAMESPACE, AppConstant.TABLE_SET_ID,
                             setId);
        List<Bin> bins = new ArrayList();
        bins.add(new Bin(AppConstant.KEY, setId));
        if(setInfo.get("name") != null) bins.add(new Bin("name", setInfo.get("name")));
        else bins.add(new Bin("name", set));
        bins.add(new Bin("namespace", namespaceRecord.getString(AppConstant.KEY)));
        bins.add(new Bin(AppConstant.DEL_FLAG, 0));
        bins.add(new Bin(AppConstant.UPDATE_AT, new Date().getTime()));
        if(setInfo.get("meta") != null) bins.add(new Bin("meta", setInfo.get("meta")));
        if(setInfo.get(AppConstant.PERMISSION_SELECT) != null) bins.add(new Bin(AppConstant.PERMISSION_SELECT, setInfo.get(AppConstant.PERMISSION_SELECT)));
        if(setInfo.get(AppConstant.PERMISSION_PUT) != null) bins.add(new Bin(AppConstant.PERMISSION_PUT, setInfo.get(AppConstant.PERMISSION_PUT)));
        if(setInfo.get(AppConstant.PERMISSION_DELETE) != null) bins.add(new Bin(AppConstant.PERMISSION_DELETE, setInfo.get(AppConstant.PERMISSION_DELETE)));
        if(recordId != null && !recordId.trim().isEmpty()) bins.add(new Bin(AppConstant.CREATE_AT, new Date().getTime()));

        aeClient.put(null, setKey, bins.toArray(new Bin[bins.size()]));

        if(recordId != null && !recordId.trim().isEmpty()) {
            Key tableKey = new Key(AEROSPIKE_NAMESPACE, setId, recordId);
            aeClient.put(null, tableKey, new Bin(AppConstant.KEY, "1"));
        }

        Policy policy = new Policy();
        policy.setTimeout(500);
        SessionBusiness.sets.put(namespaceRecord.getString(AppConstant.KEY) + "_" + set, aeClient.get(policy, setKey));

        response(responseObserver,
                 CreateSetResponse.newBuilder().setSetId(Integer.parseInt(setId)).setErrorCode(ErrorCode.SUCCESS.getValue()).build());
    }

    public void renameSet(RenameSetRequest request) {
        // get request param
        final String namespace = request.getNamespace();
        final String oldSet = request.getOldSet(); 
        final String newSet = request.getNewSet();

        // validate null
        if(isNull(responseObserver, namespace, RenameSetResponse.newBuilder().setErrorCode(ErrorCode.INPUT_REQUIRE.getValue()).build())) return;
        if(isNull(responseObserver, oldSet, RenameSetResponse.newBuilder().setErrorCode(ErrorCode.INPUT_REQUIRE.getValue()).build())) return;
        if(isNull(responseObserver, newSet, RenameSetResponse.newBuilder().setErrorCode(ErrorCode.INPUT_REQUIRE.getValue()).build())) return;

        // validate empty
        final Record namespaceRecord = SessionBusiness.namespaces.get(namespace);
        if(isEmpty(responseObserver, namespaceRecord, RenameSetResponse.newBuilder().setErrorCode(ErrorCode.NOT_EXIST.getValue()).build())) return;
        String oldSetId = namespaceRecord.getString(AppConstant.KEY) + "_" + oldSet;
        final Record oldSetRecord = SessionBusiness.sets.get(oldSetId);
        if(isEmpty(responseObserver, oldSetRecord, RenameSetResponse.newBuilder().setErrorCode(ErrorCode.NOT_EXIST.getValue()).build())) return;

        // validate role
        if(!hasRole(responseObserver, AppConstant.ROLE_DDL, namespaceRecord, RenameSetResponse.newBuilder().setErrorCode(ErrorCode.ROLE_REQUIRE.getValue()).build())) return;

        // validate duplicate
        if(isDuplicate(responseObserver, SessionBusiness.sets.get(newSet), RenameSetResponse.newBuilder().setErrorCode(ErrorCode.INPUT_DUPLICATE.getValue()).build())) return;

        aeClient.put(eventLoops.next(), new WriteListener() {
             @Override
             public void onSuccess(Key key) {
                 SessionBusiness.sets.put(namespaceRecord.getString(AppConstant.KEY) + "_" + newSet, SessionBusiness.sets.get(oldSetId));
                 SessionBusiness.sets.remove(oldSetId);
                 response(responseObserver, ShowSetResponse.newBuilder().setErrorCode(ErrorCode.SUCCESS.getValue()).build());
             }

             @Override
             public void onFailure(AerospikeException exception) {
                responseObserver.onError(exception);
                responseObserver.onCompleted();
             }
         }, null,
         new Key(AEROSPIKE_NAMESPACE, AppConstant.TABLE_SET_ID,
                 String.valueOf(oldSetRecord.getString(AppConstant.KEY))),
         new Bin("name", newSet));
    }

    public void truncateSet(TruncateSetRequest request) {
        // get request param
        final String namespace = request.getNamespace();
        final String set = request.getSet();

        // validate null
        if(isNull(responseObserver, namespace, TruncateSetResponse.newBuilder().setErrorCode(ErrorCode.INPUT_REQUIRE.getValue()).build())) return;
        if(isNull(responseObserver, set, TruncateSetResponse.newBuilder().setErrorCode(ErrorCode.INPUT_REQUIRE.getValue()).build())) return;

        // validate empty
        final Record namespaceRecord = SessionBusiness.namespaces.get(namespace);
        if(isEmpty(responseObserver, namespaceRecord, TruncateSetResponse.newBuilder().setErrorCode(ErrorCode.NOT_EXIST.getValue()).build())) return;
        final Record setRecord = SessionBusiness.sets.get(namespaceRecord.getString(AppConstant.KEY) + "_" + set);
        if(isEmpty(responseObserver, setRecord, TruncateSetResponse.newBuilder().setErrorCode(ErrorCode.NOT_EXIST.getValue()).build())) return;

        // validate role
        if(!hasRole(responseObserver, AppConstant.ROLE_DDL, namespaceRecord, TruncateSetResponse.newBuilder().setErrorCode(ErrorCode.ROLE_REQUIRE.getValue()).build())) return;

        aeClient.truncate(null, AEROSPIKE_NAMESPACE, String.valueOf(setRecord.getString(AppConstant.KEY)),
                          null);

        response(responseObserver, TruncateSetResponse.newBuilder().setErrorCode(ErrorCode.SUCCESS.getValue()).build());
    }

    public void dropSet(DropSetRequest request) {
        // get request param
        String namespace = request.getNamespace();
        String set = request.getSet();

        // validate null
        if(isNull(responseObserver, namespace, DropSetResponse.newBuilder().setErrorCode(ErrorCode.INPUT_REQUIRE.getValue()).build())) return;
        if(isNull(responseObserver, set, DropSetResponse.newBuilder().setErrorCode(ErrorCode.INPUT_REQUIRE.getValue()).build())) return;

        // validate empty
        final Record namespaceRecord = SessionBusiness.namespaces.get(namespace);
        if(isEmpty(responseObserver, namespaceRecord, DropSetResponse.newBuilder().setErrorCode(ErrorCode.NOT_EXIST.getValue()).build())) return;
        final Record setRecord = SessionBusiness.sets.get(namespaceRecord.getString(AppConstant.KEY) + "_" + set);
        if(isEmpty(responseObserver, setRecord, DropSetResponse.newBuilder().setErrorCode(ErrorCode.NOT_EXIST.getValue()).build())) return;

        // validate role
        if(!hasRole(responseObserver, AppConstant.ROLE_DDL, namespaceRecord, DropSetResponse.newBuilder().setErrorCode(ErrorCode.ROLE_REQUIRE.getValue()).build())) return;

        aeClient.truncate(null, AEROSPIKE_NAMESPACE, String.valueOf(setRecord.getString(AppConstant.KEY)),
                          null);

        aeClient.put(eventLoops.next(), new WriteListener() {
             @Override
             public void onSuccess(Key key) {
                 SessionBusiness.sets.remove(namespaceRecord.getString(AppConstant.KEY) + "_" + set);
                 response(responseObserver, DropSetResponse.newBuilder().setErrorCode(ErrorCode.SUCCESS.getValue()).build());
             }

             @Override
             public void onFailure(AerospikeException exception) {
                 responseObserver.onError(exception);
                 responseObserver.onCompleted();
             }
         },
         null,
         new Key(AEROSPIKE_NAMESPACE, AppConstant.TABLE_SET_ID,
                 String.valueOf(setRecord.getString(AppConstant.KEY))),
         new Bin(AppConstant.DEL_FLAG, 1),
         new Bin("namespace", 0),
         new Bin("name", ""),
         new Bin("grant_select", ""),
         new Bin("grant_put", ""),
         new Bin("grant_delete", "")
         );
    };
}
