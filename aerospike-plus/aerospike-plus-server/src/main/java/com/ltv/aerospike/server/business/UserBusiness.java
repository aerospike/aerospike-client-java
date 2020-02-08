/*
 * Copyright (c) 2019 LINE Corporation. All rights reserved.
 * LINE Corporation PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 */

package com.ltv.aerospike.server.business;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;

import org.apache.commons.codec.digest.DigestUtils;
import org.apache.log4j.Logger;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.Bin;
import com.aerospike.client.Key;
import com.aerospike.client.Operation;
import com.aerospike.client.Record;
import com.aerospike.client.listener.DeleteListener;
import com.aerospike.client.listener.RecordListener;
import com.aerospike.client.listener.WriteListener;
import com.ltv.aerospike.api.proto.CreateUserServices.CreateUserRequest;
import com.ltv.aerospike.api.proto.CreateUserServices.CreateUserRequest.Role;
import com.ltv.aerospike.api.proto.CreateUserServices.CreateUserResponse;
import com.ltv.aerospike.api.proto.DeleteUserServices.DeleteUserRequest;
import com.ltv.aerospike.api.proto.DeleteUserServices.DeleteUserResponse;
import com.ltv.aerospike.api.proto.DropSequenceServices.DropSequenceResponse;
import com.ltv.aerospike.api.proto.GrantNamespaceServices.GrantNamespaceRequest;
import com.ltv.aerospike.api.proto.GrantNamespaceServices.GrantNamespaceRequest.NamespaceRole;
import com.ltv.aerospike.api.proto.GrantNamespaceServices.GrantNamespaceResponse;
import com.ltv.aerospike.api.proto.GrantSetServices.GrantSetRequest;
import com.ltv.aerospike.api.proto.GrantSetServices.GrantSetRequest.SetPermission;
import com.ltv.aerospike.api.proto.GrantSetServices.GrantSetResponse;
import com.ltv.aerospike.api.proto.UpdateUserServices.UpdateUserRequest;
import com.ltv.aerospike.api.proto.UpdateUserServices.UpdateUserResponse;
import com.ltv.aerospike.api.util.ErrorCode;
import com.ltv.aerospike.server.repository.Connector;
import com.ltv.aerospike.server.run.StartApp;
import com.ltv.aerospike.server.util.AppConstant;

import io.grpc.stub.StreamObserver;

public class UserBusiness extends BaseBusiness {

    private static Logger log = Logger.getLogger(UserBusiness.class.getSimpleName());

    public UserBusiness(StreamObserver responseObserver) {
        super(responseObserver);
    }

    public void create(CreateUserRequest request) {
        // get request param
        final String name = request.getName();
        final String password = DigestUtils.sha256Hex(StartApp.config.getConfig("secret-key") + request.getPassword());
        final Role role = request.getRole();

        // validate null
        if(isNull(responseObserver, name, CreateUserResponse.newBuilder().setErrorCode(ErrorCode.INPUT_REQUIRE.getValue()).build())) return;
        if(isNull(responseObserver, password, CreateUserResponse.newBuilder().setErrorCode(ErrorCode.INPUT_REQUIRE.getValue()).build())) return;
        if(isNull(responseObserver, role, CreateUserResponse.newBuilder().setErrorCode(ErrorCode.INPUT_REQUIRE.getValue()).build())) return;

        // validate role
        if(!hasRole(responseObserver, AppConstant.ROLE_DBA, CreateUserResponse.newBuilder().setErrorCode(ErrorCode.ROLE_REQUIRE.getValue()).build())) return;

        // validate duplicate
        if(isDuplicate(responseObserver, SessionBusiness.users.get(name), CreateUserResponse.newBuilder().setErrorCode(ErrorCode.INPUT_DUPLICATE.getValue()).build())) return;

        final Key userSequence = new Key(AEROSPIKE_NAMESPACE, AppConstant.TABLE_SEQUENCE_ID, AppConstant.TABLE_USER_ID);
        aeClient.operate(eventLoops.next(), new RecordListener() {
             @Override
             public void onSuccess(Key key, Record record) {
                 Key sequenceKey = new Key(AEROSPIKE_NAMESPACE, AppConstant.TABLE_USER_ID, String.valueOf(record.getLong("value")));
                 aeClient.put(eventLoops.next(), Connector.defaultWriteListener, null, sequenceKey,
                              new Bin(AppConstant.KEY, record.getLong("value")),
                              new Bin("name", name),
                              new Bin("password", password),
                              new Bin("role", role),
                              new Bin(AppConstant.CREATE_AT, new Date().getTime()),
                              new Bin(AppConstant.UPDATE_AT, new Date().getTime())
                 );

                 SessionBusiness.users.put(name, aeClient.get(null, sequenceKey));
                 response(responseObserver, CreateUserResponse.newBuilder().setErrorCode(ErrorCode.SUCCESS.getValue()).build());
             }

             @Override
             public void onFailure(AerospikeException exception) {
                 log.error("Get sequence failed: " + exception);
             }
         }, null, userSequence,
         Operation.add(new Bin("value", 1)), Operation.get());
    };

    public void update(UpdateUserRequest request) {
        // get request param
        final String name = request.getName();
        final String password = DigestUtils.sha256Hex(StartApp.config.getConfig("secret-key") + request.getPassword());
        final UpdateUserRequest.Role role = request.getRole();

        // validate null
        if(isNull(responseObserver, name, UpdateUserResponse.newBuilder().setErrorCode(ErrorCode.INPUT_REQUIRE.getValue()).build())) return;
        if(isNull(responseObserver, password, UpdateUserResponse.newBuilder().setErrorCode(ErrorCode.INPUT_REQUIRE.getValue()).build())) return;
        if(isNull(responseObserver, role, UpdateUserResponse.newBuilder().setErrorCode(ErrorCode.INPUT_REQUIRE.getValue()).build())) return;

        // validate empty
        Record userRecord = SessionBusiness.users.get(name);
        if(isEmpty(responseObserver, userRecord, UpdateUserResponse.newBuilder().setErrorCode(ErrorCode.NOT_EXIST.getValue()).build())) return;

        // validate role
        if(!hasRole(responseObserver, AppConstant.ROLE_DBA, UpdateUserResponse.newBuilder().setErrorCode(ErrorCode.ROLE_REQUIRE.getValue()).build())) return;

        Key sequenceKey = new Key(AEROSPIKE_NAMESPACE, AppConstant.TABLE_USER_ID, String.valueOf(userRecord.getString(AppConstant.KEY)));
        aeClient.put(eventLoops.next(), Connector.defaultWriteListener, null, sequenceKey,
                     new Bin("password", password),
                     new Bin("role", role),
                     new Bin(AppConstant.UPDATE_AT, new Date().getTime())
        );

        // Delete old session key to force logout
        SessionBusiness.sessions.entrySet().stream().filter(x -> name.equals(x.getValue().get("userName"))).forEach(x -> {
            SessionBusiness.sessions.remove(x.getKey());
            Key deleteKey = new Key(AEROSPIKE_NAMESPACE, AppConstant.TABLE_SESSION_ID.toString(), x.getKey());
            aeClient.delete(eventLoops.next(), Connector.defaultDeleteListener, null, deleteKey);
        });

        SessionBusiness.users.put(name, aeClient.get(null, sequenceKey));
        response(responseObserver, UpdateUserResponse.newBuilder().setErrorCode(ErrorCode.SUCCESS.getValue()).build());
    };

    public void delete(DeleteUserRequest request) {
        // get request param
        final String name = request.getName();

        // validate null
        if(isNull(responseObserver, name, DeleteUserResponse.newBuilder().setErrorCode(ErrorCode.INPUT_REQUIRE.getValue()).build())) return;

        // validate role
        if(!hasRole(responseObserver, AppConstant.ROLE_DBA, DeleteUserResponse.newBuilder().setErrorCode(ErrorCode.ROLE_REQUIRE.getValue()).build())) return;

        final Key userKey = new Key(AEROSPIKE_NAMESPACE, AppConstant.TABLE_USER_ID, "" + SessionBusiness.users.get(name).getString(AppConstant.KEY));
        aeClient.put(eventLoops.next(), new WriteListener() {
            @Override
            public void onSuccess(Key key) {
                aeClient.delete(eventLoops.next(), new DeleteListener() {
                    @Override
                    public void onSuccess(Key key, boolean existed) {
                        SessionBusiness.users.remove(name);
                        response(responseObserver, DropSequenceResponse.newBuilder().setErrorCode(ErrorCode.SUCCESS.getValue()).build());
                    }

                    @Override
                    public void onFailure(AerospikeException exception) {
                        responseObserver.onError(exception);
                        responseObserver.onCompleted();
                    }
                }, null, userKey);
            }

            @Override
            public void onFailure(AerospikeException exception) {
                responseObserver.onError(exception);
                responseObserver.onCompleted();
            }
        }, null, userKey, new Bin(AppConstant.DEL_FLAG,1));
    };

    public void grantNamespace(GrantNamespaceRequest request) {
        // get request param
        String user = request.getUser();
        String namespace = request.getNamespace();
        NamespaceRole role = request.getRole();
        Boolean isRevoke = request.getIsRevoke();

        // validate null
        if(isNull(responseObserver, user, GrantNamespaceResponse.newBuilder().setErrorCode(ErrorCode.INPUT_REQUIRE.getValue()).build())) return;
        if(isNull(responseObserver, namespace, GrantNamespaceResponse.newBuilder().setErrorCode(ErrorCode.INPUT_REQUIRE.getValue()).build())) return;
        if(isNull(responseObserver, role, GrantNamespaceResponse.newBuilder().setErrorCode(ErrorCode.INPUT_REQUIRE.getValue()).build())) return;
        if(isNull(responseObserver, isRevoke, GrantNamespaceResponse.newBuilder().setErrorCode(ErrorCode.INPUT_REQUIRE.getValue()).build())) return;

        // validate empty
        Record namespaceRecord = SessionBusiness.namespaces.get(namespace);
        if(isEmpty(responseObserver, namespaceRecord, GrantNamespaceResponse.newBuilder().setErrorCode(ErrorCode.NOT_EXIST.getValue()).build())) return;

        // validate role
        if(!hasRole(responseObserver, AppConstant.ROLE_DCL, namespaceRecord, GrantNamespaceResponse.newBuilder().setErrorCode(ErrorCode.ROLE_REQUIRE.getValue()).build())) return;

        final List<Bin> lstRole = new ArrayList();
        List<String> users = Arrays.asList(namespaceRecord.getString(getRoleValue(role)).split(","));
        final String userId = String.valueOf(SessionBusiness.users.get(user).getString(AppConstant.KEY));
        if(users.contains(userId) && isRevoke) users.remove(userId);
        if(!users.contains(userId) && !isRevoke) users.add(userId);
        String strUser = String.join(",", users);
        Bin row = new Bin(getRoleValue(role), strUser);
        lstRole.add(row);
        lstRole.add(new Bin("update_at", new Date().getTime()));

         Key namespaceKey = new Key(AEROSPIKE_NAMESPACE, AppConstant.TABLE_NAMESPACE_ID,
                                    String.valueOf(namespaceRecord.getString(AppConstant.KEY)));
         aeClient.put(eventLoops.next(), Connector.defaultWriteListener, null, namespaceKey,
                      (Bin[])lstRole.toArray());

         SessionBusiness.namespaces.put(namespace, aeClient.get(null, namespaceKey));
         response(responseObserver, GrantNamespaceResponse.newBuilder().setErrorCode(ErrorCode.SUCCESS.getValue()).build());
    }

    private String getRoleValue(NamespaceRole role) {
        switch (role.getNumber()) {
            case NamespaceRole.DDL_VALUE:
                return AppConstant.ROLE_DDL;
            case NamespaceRole.DQL_VALUE:
                return AppConstant.ROLE_DQL;
            case NamespaceRole.DML_VALUE:
                return AppConstant.ROLE_DML;
            case NamespaceRole.OWNER_VALUE:
                return AppConstant.ROLE_OWNER;
            default:
                return null;
        }
    }

    private String getPermissionValue(SetPermission permission) {
        switch (permission.getNumber()) {
            case SetPermission.PUT_VALUE:
                return AppConstant.PERMISSION_PUT;
            case SetPermission.SELECT_VALUE:
                return AppConstant.PERMISSION_SELECT;
            case SetPermission.DELETE_VALUE:
                return AppConstant.PERMISSION_DELETE;
            default:
                return null;
        }
    }

     public void grantSet(GrantSetRequest request) {
        // get request param
        String user = request.getUser();
        String namespace = request.getNamespace();
        String set = request.getSet();
        SetPermission permission = request.getPermission();
        Boolean isRevoke = request.getIsRevoke();

        // validate null
         if(isNull(responseObserver, user, GrantSetResponse.newBuilder().setErrorCode(ErrorCode.INPUT_REQUIRE.getValue()).build())) return;
         if(isNull(responseObserver, namespace, GrantSetResponse.newBuilder().setErrorCode(ErrorCode.INPUT_REQUIRE.getValue()).build())) return;
         if(isNull(responseObserver, set, GrantSetResponse.newBuilder().setErrorCode(ErrorCode.INPUT_REQUIRE.getValue()).build())) return;
         if(isNull(responseObserver, isRevoke, GrantSetResponse.newBuilder().setErrorCode(ErrorCode.INPUT_REQUIRE.getValue()).build())) return;

        // validate empty
        Record namespaceRecord = SessionBusiness.namespaces.get(namespace);
        if(isEmpty(responseObserver, namespaceRecord, GrantSetResponse.newBuilder().setErrorCode(ErrorCode.NOT_EXIST.getValue()).build())) return;
        String setId = namespaceRecord.getString(AppConstant.KEY) + "_" + set;
        Record setRecord = SessionBusiness.sets.get(setId);
        if(isEmpty(responseObserver, setRecord, GrantSetResponse.newBuilder().setErrorCode(ErrorCode.NOT_EXIST.getValue()).build())) return;

        // validate role
        if(!hasRole(responseObserver, AppConstant.ROLE_DCL, namespaceRecord, GrantSetResponse.newBuilder().setErrorCode(ErrorCode.ROLE_REQUIRE.getValue()).build())) return;

        List<Bin> lstRole = new ArrayList();

        List<String> users = Arrays.asList(setRecord.getString(getPermissionValue(permission)).split(","));
        String userId = String.valueOf(SessionBusiness.users.get(user).getString(AppConstant.KEY));
        if(users.contains(userId) && isRevoke) users.remove(userId);
        if(!users.contains(userId) && !isRevoke) users.add(userId);
        String strUser = String.join(",", users);
        Bin row = new Bin(getPermissionValue(permission), strUser);
        lstRole.add(row);

        lstRole.add(new Bin("update_at", new Date().getTime()));

        Key setKey = new Key(AEROSPIKE_NAMESPACE, AppConstant.TABLE_SET_ID,
                                   String.valueOf(setRecord.getString(AppConstant.KEY)));
        aeClient.put(eventLoops.next(), Connector.defaultWriteListener, null, setKey,
                     (Bin[])lstRole.toArray());

        SessionBusiness.sets.put(setId, aeClient.get(null, setKey));
        response(responseObserver, GrantSetResponse.newBuilder().setErrorCode(ErrorCode.SUCCESS.getValue()).build());
    }
}
