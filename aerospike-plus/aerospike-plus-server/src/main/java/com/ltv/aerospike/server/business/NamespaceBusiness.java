/*
 * Copyright (c) 2019 LINE Corporation. All rights reserved.
 * LINE Corporation PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 */

package com.ltv.aerospike.server.business;

import java.util.Date;
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
import com.aerospike.client.policy.Policy;
import com.google.gson.Gson;
import com.ltv.aerospike.api.proto.CreateNamespaceServices.CreateNamespaceRequest;
import com.ltv.aerospike.api.proto.CreateNamespaceServices.CreateNamespaceResponse;
import com.ltv.aerospike.api.proto.DropNamespaceServices.DropNamespaceRequest;
import com.ltv.aerospike.api.proto.DropNamespaceServices.DropNamespaceResponse;
import com.ltv.aerospike.api.proto.RenameNamespaceServices.RenameNamespaceRequest;
import com.ltv.aerospike.api.proto.RenameNamespaceServices.RenameNamespaceResponse;
import com.ltv.aerospike.api.proto.ShowNamespaceServices.ShowNamespaceResponse;
import com.ltv.aerospike.api.proto.TruncateNamespaceServices.TruncateNamespaceRequest;
import com.ltv.aerospike.api.proto.TruncateNamespaceServices.TruncateNamespaceResponse;
import com.ltv.aerospike.api.util.ErrorCode;
import com.ltv.aerospike.server.repository.Connector;
import com.ltv.aerospike.server.util.AppConstant;

import io.grpc.stub.StreamObserver;

public class NamespaceBusiness extends BaseBusiness {

    private static Logger log = Logger.getLogger(NamespaceBusiness.class.getSimpleName());
    public NamespaceBusiness(StreamObserver responseObserver) {
        super(responseObserver);
    }

    public void showNamespace() {
        final Map<String, String> result = SessionBusiness.namespaces.entrySet().stream()
                                  .filter(x -> SessionBusiness.hasRole(AppConstant.AUTHEN_TOKEN_KEY.get(), AppConstant.ROLE_DBA)
                                               || SessionBusiness.hasRole(AppConstant.AUTHEN_TOKEN_KEY.get(), AppConstant.ROLE_OWNER, x.getValue()))
                                  .collect(Collectors.toMap(
                                          x -> x.getValue().getString("name"),
                                          x -> (new Gson()).toJson(x.getValue().bins)
                                  ));

        response(responseObserver, ShowNamespaceResponse.newBuilder()
                                                        .putAllNamespaces(result)
                                                        .setErrorCode(ErrorCode.SUCCESS.getValue()).build());
    };

    public void createNamespace(CreateNamespaceRequest request) {
        // get request param
        final String namespace = request.getNamespace();
        final String token = AppConstant.AUTHEN_TOKEN_KEY.get();

        // validate null
        if(isNull(responseObserver, namespace, CreateNamespaceResponse.newBuilder().setErrorCode(ErrorCode.INPUT_REQUIRE.getValue()).build())) return;

        // validate role
        if(!hasRole(responseObserver, AppConstant.ROLE_DBA, CreateNamespaceResponse.newBuilder().setErrorCode(ErrorCode.ROLE_REQUIRE.getValue()).build())) return;

        // validate duplicate
        if(isDuplicate(responseObserver, SessionBusiness.namespaces.get(namespace), CreateNamespaceResponse.newBuilder().setErrorCode(ErrorCode.INPUT_DUPLICATE.getValue()).build())) return;

        final Key namespaceSequence = new Key(AEROSPIKE_NAMESPACE, AppConstant.TABLE_SEQUENCE_ID, AppConstant.TABLE_NAMESPACE_ID);
        aeClient.operate(eventLoops.next(), new RecordListener() {
             @Override
             public void onSuccess(Key key, Record record) {
                 Key namespaceKey = new Key(AEROSPIKE_NAMESPACE, AppConstant.TABLE_NAMESPACE_ID, "" + record.getLong("value"));
                 aeClient.put(null, namespaceKey,
                              new Bin(AppConstant.KEY, "" + record.getLong("value")),
                              new Bin("name", namespace),
                              new Bin(AppConstant.ROLE_OWNER, SessionBusiness.getUserId(token)),// list of namespace owner
                              new Bin(AppConstant.ROLE_DQL, ""),  // select
                              new Bin(AppConstant.ROLE_DML, ""),  // insert, update, delete, commit, rollback
                              new Bin(AppConstant.ROLE_DDL, ""),  // create, alter, drop, rename, truncate
                              new Bin(AppConstant.ROLE_DCL, ""),  // grant, revoke
                              new Bin(AppConstant.CREATE_AT, new Date().getTime()),
                              new Bin(AppConstant.UPDATE_AT, new Date().getTime())
                 );
                 Policy policy = new Policy();
                 policy.setTimeout(500);
                 Record namespaceRecord = aeClient.get(policy, namespaceKey);
                 SessionBusiness.namespaces.put(namespace, namespaceRecord);
                 response(responseObserver, CreateNamespaceResponse.newBuilder().setErrorCode(ErrorCode.SUCCESS.getValue()).build());
             }

             @Override
             public void onFailure(AerospikeException exception) {
                 responseObserver.onError(exception);
                 responseObserver.onCompleted();
             }
         }, null, namespaceSequence,
         Operation.add(new Bin("value", 1)), Operation.get());
    };

    public void renameNamespace(RenameNamespaceRequest request) {
        // get request param
        final String oldName = request.getOldName();
        final String newName = request.getNewName();

        // validate null
        if (isNull(responseObserver, oldName, RenameNamespaceResponse.newBuilder().setErrorCode(ErrorCode.INPUT_REQUIRE.getValue()).build())) return;
        if (isNull(responseObserver, oldName, RenameNamespaceResponse.newBuilder().setErrorCode(ErrorCode.INPUT_REQUIRE.getValue()).build())) return;

        // validate empty
        Record old = SessionBusiness.namespaces.get(oldName);
        if (isEmpty(responseObserver, old, RenameNamespaceResponse.newBuilder().setErrorCode(ErrorCode.NOT_EXIST.getValue()).build())) return;

        // validate duplicate
        if (isEqual(responseObserver, oldName, newName, RenameNamespaceResponse.newBuilder().setErrorCode(ErrorCode.INPUT_DUPLICATE.getValue()).build())) return;
        if (isDuplicate(responseObserver, SessionBusiness.namespaces.get(newName), RenameNamespaceResponse.newBuilder().setErrorCode(ErrorCode.INPUT_DUPLICATE.getValue()).build())) return;

        // validate role
        if (!hasRole(responseObserver, AppConstant.ROLE_DBA, RenameNamespaceResponse.newBuilder().setErrorCode(ErrorCode.ROLE_REQUIRE.getValue()).build())) return;

        Key oldKey = new Key(AEROSPIKE_NAMESPACE, AppConstant.TABLE_NAMESPACE_ID, "" + old.getString(AppConstant.KEY));
        aeClient.put(eventLoops.next(), new WriteListener() {
             @Override
             public void onSuccess(Key key) {
                 SessionBusiness.namespaces.put(newName, SessionBusiness.namespaces.get(oldName));
                 SessionBusiness.namespaces.remove(oldName);
                 response(responseObserver, RenameNamespaceResponse.newBuilder().setErrorCode(ErrorCode.SUCCESS.getValue()).build());
             }

             @Override
             public void onFailure(AerospikeException exception) {
                 responseObserver.onError(exception);
                 responseObserver.onCompleted();
             }
         }, null,
         oldKey, new Bin("name", newName));
    }

    public void truncateNamespace(TruncateNamespaceRequest request) {
        // get request param
        final String namespace = request.getNamespace();

        // validate null
        if(isNull(responseObserver, namespace, TruncateNamespaceResponse.newBuilder().setErrorCode(ErrorCode.INPUT_REQUIRE.getValue()).build())) return;

        // validate empty
        Record namespaceRecord = SessionBusiness.namespaces.get(namespace);
        if(isEmpty(responseObserver, namespaceRecord, TruncateNamespaceResponse.newBuilder().setErrorCode(ErrorCode.NOT_EXIST.getValue()).build())) return;

        // validate role
        if(!hasRole(responseObserver, AppConstant.ROLE_DDL, namespaceRecord, TruncateNamespaceResponse.newBuilder().setErrorCode(ErrorCode.ROLE_REQUIRE.getValue()).build())) return;

        truncateNamespace(namespaceRecord);
        response(responseObserver, TruncateNamespaceResponse.newBuilder().setErrorCode(ErrorCode.SUCCESS.getValue()).build());
    }

    private void truncateNamespace(Record namespaceRecord) {
        SessionBusiness.sets.entrySet().stream()
                            .filter(x -> namespaceRecord.getString(AppConstant.KEY).equals(x.getValue().getString("namespace")))
                            .forEach(x -> {
                                String setId = String.valueOf(x.getValue().getString(AppConstant.KEY));
                                aeClient.put(eventLoops.next(), Connector.defaultWriteListener,
                                             null, new Key(AEROSPIKE_NAMESPACE, AppConstant.TABLE_SET_ID, setId),
                                             new Bin("del_flag", 1),
                                             new Bin("namespace", 0),
                                             new Bin("name", ""));
                                aeClient.truncate(null, AEROSPIKE_NAMESPACE, setId,null);
                            });
    };

    public void dropNamespace(DropNamespaceRequest request) {
        // get request param
        String namespace = request.getNamespace();

        // validate null
        if(isNull(responseObserver, namespace, DropNamespaceResponse.newBuilder().setErrorCode(ErrorCode.INPUT_REQUIRE.getValue()).build())) return;

        // validate empty
        Record namespaceRecord = SessionBusiness.namespaces.get(namespace);
        if(isEmpty(responseObserver, namespaceRecord, DropNamespaceResponse.newBuilder().setErrorCode(ErrorCode.NOT_EXIST.getValue()).build())) return;

        // validate role
        if(!hasRole(responseObserver, AppConstant.ROLE_DDL, namespaceRecord, DropNamespaceResponse.newBuilder().setErrorCode(ErrorCode.ROLE_REQUIRE.getValue()).build())) return;

        truncateNamespace(namespaceRecord);
        Key deleteKey = new Key(AEROSPIKE_NAMESPACE, AppConstant.TABLE_NAMESPACE_ID,
                                String.valueOf(namespaceRecord.getString(AppConstant.KEY)));

        aeClient.put(eventLoops.next(), new WriteListener() {
            @Override
            public void onSuccess(Key key) {
                aeClient.delete(eventLoops.next(), new DeleteListener() {
                    @Override
                    public void onSuccess(Key key, boolean existed) {
                        SessionBusiness.namespaces.remove(namespace);
                        response(responseObserver, DropNamespaceResponse.newBuilder().setErrorCode(ErrorCode.SUCCESS.getValue()).build());
                    }

                    @Override
                    public void onFailure(AerospikeException exception) {
                        responseObserver.onError(exception);
                        responseObserver.onCompleted();
                    }
                }, null,
                deleteKey);
            }

            @Override
            public void onFailure(AerospikeException exception) {
                responseObserver.onError(exception);
                responseObserver.onCompleted();
            }
        }, null, deleteKey, new Bin(AppConstant.DEL_FLAG,1));
    };
}
