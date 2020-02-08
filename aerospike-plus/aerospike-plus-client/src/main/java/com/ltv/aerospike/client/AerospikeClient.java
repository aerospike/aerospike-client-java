/*
 * Copyright (c) 2019 LINE Corporation. All rights reserved.
 * LINE Corporation PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 */

package com.ltv.aerospike.client;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import com.google.protobuf.ByteString;
import com.ltv.aerospike.api.proto.CreateIndexServiceGrpc;
import com.ltv.aerospike.api.proto.CreateIndexServiceGrpc.CreateIndexServiceStub;
import com.ltv.aerospike.api.proto.CreateIndexServices.CreateIndexRequest;
import com.ltv.aerospike.api.proto.CreateIndexServices.CreateIndexRequest.IndexType;
import com.ltv.aerospike.api.proto.CreateIndexServices.CreateIndexResponse;
import com.ltv.aerospike.api.proto.CreateNamespaceServiceGrpc;
import com.ltv.aerospike.api.proto.CreateNamespaceServiceGrpc.CreateNamespaceServiceStub;
import com.ltv.aerospike.api.proto.CreateNamespaceServices.CreateNamespaceRequest;
import com.ltv.aerospike.api.proto.CreateNamespaceServices.CreateNamespaceResponse;
import com.ltv.aerospike.api.proto.CreateSequenceServiceGrpc;
import com.ltv.aerospike.api.proto.CreateSequenceServiceGrpc.CreateSequenceServiceStub;
import com.ltv.aerospike.api.proto.CreateSequenceServices.CreateSequenceRequest;
import com.ltv.aerospike.api.proto.CreateSequenceServices.CreateSequenceResponse;
import com.ltv.aerospike.api.proto.CreateSetServiceGrpc;
import com.ltv.aerospike.api.proto.CreateSetServiceGrpc.CreateSetServiceStub;
import com.ltv.aerospike.api.proto.CreateSetServices.CreateSetRequest;
import com.ltv.aerospike.api.proto.CreateSetServices.CreateSetResponse;
import com.ltv.aerospike.api.proto.CreateUserServiceGrpc;
import com.ltv.aerospike.api.proto.CreateUserServiceGrpc.CreateUserServiceStub;
import com.ltv.aerospike.api.proto.CreateUserServices.CreateUserRequest;
import com.ltv.aerospike.api.proto.CreateUserServices.CreateUserRequest.Role;
import com.ltv.aerospike.api.proto.CreateUserServices.CreateUserResponse;
import com.ltv.aerospike.api.proto.DeleteServiceGrpc;
import com.ltv.aerospike.api.proto.DeleteServiceGrpc.DeleteServiceStub;
import com.ltv.aerospike.api.proto.DeleteServices.DeleteRequest;
import com.ltv.aerospike.api.proto.DeleteServices.DeleteResponse;
import com.ltv.aerospike.api.proto.DeleteUserServiceGrpc;
import com.ltv.aerospike.api.proto.DeleteUserServiceGrpc.DeleteUserServiceStub;
import com.ltv.aerospike.api.proto.DeleteUserServices.DeleteUserRequest;
import com.ltv.aerospike.api.proto.DeleteUserServices.DeleteUserResponse;
import com.ltv.aerospike.api.proto.DropBinServiceGrpc;
import com.ltv.aerospike.api.proto.DropBinServiceGrpc.DropBinServiceStub;
import com.ltv.aerospike.api.proto.DropBinServices.DropBinRequest;
import com.ltv.aerospike.api.proto.DropBinServices.DropBinResponse;
import com.ltv.aerospike.api.proto.DropIndexServiceGrpc;
import com.ltv.aerospike.api.proto.DropIndexServiceGrpc.DropIndexServiceStub;
import com.ltv.aerospike.api.proto.DropIndexServices.DropIndexRequest;
import com.ltv.aerospike.api.proto.DropIndexServices.DropIndexResponse;
import com.ltv.aerospike.api.proto.DropNamespaceServiceGrpc;
import com.ltv.aerospike.api.proto.DropNamespaceServiceGrpc.DropNamespaceServiceStub;
import com.ltv.aerospike.api.proto.DropNamespaceServices.DropNamespaceRequest;
import com.ltv.aerospike.api.proto.DropNamespaceServices.DropNamespaceResponse;
import com.ltv.aerospike.api.proto.DropSequenceServiceGrpc;
import com.ltv.aerospike.api.proto.DropSequenceServiceGrpc.DropSequenceServiceStub;
import com.ltv.aerospike.api.proto.DropSequenceServices.DropSequenceRequest;
import com.ltv.aerospike.api.proto.DropSequenceServices.DropSequenceResponse;
import com.ltv.aerospike.api.proto.DropSetServiceGrpc;
import com.ltv.aerospike.api.proto.DropSetServiceGrpc.DropSetServiceStub;
import com.ltv.aerospike.api.proto.DropSetServices.DropSetRequest;
import com.ltv.aerospike.api.proto.DropSetServices.DropSetResponse;
import com.ltv.aerospike.api.proto.GetSequenceServiceGrpc;
import com.ltv.aerospike.api.proto.GetSequenceServiceGrpc.GetSequenceServiceStub;
import com.ltv.aerospike.api.proto.GetSequenceServices.GetSequenceRequest;
import com.ltv.aerospike.api.proto.GetSequenceServices.GetSequenceResponse;
import com.ltv.aerospike.api.proto.GetServiceGrpc;
import com.ltv.aerospike.api.proto.GetServices.GetRequest;
import com.ltv.aerospike.api.proto.GetServices.GetResponse;
import com.ltv.aerospike.api.proto.GrantNamespaceServiceGrpc;
import com.ltv.aerospike.api.proto.GrantNamespaceServiceGrpc.GrantNamespaceServiceStub;
import com.ltv.aerospike.api.proto.GrantNamespaceServices.GrantNamespaceRequest;
import com.ltv.aerospike.api.proto.GrantNamespaceServices.GrantNamespaceRequest.NamespaceRole;
import com.ltv.aerospike.api.proto.GrantNamespaceServices.GrantNamespaceResponse;
import com.ltv.aerospike.api.proto.GrantSetServiceGrpc;
import com.ltv.aerospike.api.proto.GrantSetServiceGrpc.GrantSetServiceStub;
import com.ltv.aerospike.api.proto.GrantSetServices.GrantSetRequest;
import com.ltv.aerospike.api.proto.GrantSetServices.GrantSetRequest.SetPermission;
import com.ltv.aerospike.api.proto.GrantSetServices.GrantSetResponse;
import com.ltv.aerospike.api.proto.LoginServiceGrpc;
import com.ltv.aerospike.api.proto.LoginServiceGrpc.LoginServiceBlockingStub;
import com.ltv.aerospike.api.proto.LoginServiceGrpc.LoginServiceStub;
import com.ltv.aerospike.api.proto.LoginServices.LoginRequest;
import com.ltv.aerospike.api.proto.LoginServices.LoginResponse;
import com.ltv.aerospike.api.proto.LogoutServiceGrpc;
import com.ltv.aerospike.api.proto.LogoutServiceGrpc.LogoutServiceStub;
import com.ltv.aerospike.api.proto.LogoutServices.LogoutRequest;
import com.ltv.aerospike.api.proto.LogoutServices.LogoutResponse;
import com.ltv.aerospike.api.proto.PutServiceGrpc;
import com.ltv.aerospike.api.proto.PutServiceGrpc.PutServiceStub;
import com.ltv.aerospike.api.proto.PutServices.PutRequest;
import com.ltv.aerospike.api.proto.PutServices.PutRequest.RecordExistsAction;
import com.ltv.aerospike.api.proto.PutServices.PutResponse;
import com.ltv.aerospike.api.proto.QueryServiceGrpc;
import com.ltv.aerospike.api.proto.QueryServiceGrpc.QueryServiceBlockingStub;
import com.ltv.aerospike.api.proto.QueryServiceGrpc.QueryServiceStub;
import com.ltv.aerospike.api.proto.QueryServices.QueryRequest;
import com.ltv.aerospike.api.proto.QueryServices.QueryRequest.Builder;
import com.ltv.aerospike.api.proto.QueryServices.QueryResponse;
import com.ltv.aerospike.api.proto.RenameBinServiceGrpc;
import com.ltv.aerospike.api.proto.RenameBinServiceGrpc.RenameBinServiceStub;
import com.ltv.aerospike.api.proto.RenameBinServices.RenameBinRequest;
import com.ltv.aerospike.api.proto.RenameBinServices.RenameBinResponse;
import com.ltv.aerospike.api.proto.RenameNamespaceServiceGrpc;
import com.ltv.aerospike.api.proto.RenameNamespaceServiceGrpc.RenameNamespaceServiceStub;
import com.ltv.aerospike.api.proto.RenameNamespaceServices.RenameNamespaceRequest;
import com.ltv.aerospike.api.proto.RenameNamespaceServices.RenameNamespaceResponse;
import com.ltv.aerospike.api.proto.RenameSetServiceGrpc;
import com.ltv.aerospike.api.proto.RenameSetServiceGrpc.RenameSetServiceStub;
import com.ltv.aerospike.api.proto.RenameSetServices.RenameSetRequest;
import com.ltv.aerospike.api.proto.RenameSetServices.RenameSetResponse;
import com.ltv.aerospike.api.proto.ShowBinServiceGrpc;
import com.ltv.aerospike.api.proto.ShowBinServiceGrpc.ShowBinServiceStub;
import com.ltv.aerospike.api.proto.ShowBinServices.ShowBinRequest;
import com.ltv.aerospike.api.proto.ShowBinServices.ShowBinResponse;
import com.ltv.aerospike.api.proto.ShowIndexServiceGrpc;
import com.ltv.aerospike.api.proto.ShowIndexServiceGrpc.ShowIndexServiceStub;
import com.ltv.aerospike.api.proto.ShowIndexServices.ShowIndexRequest;
import com.ltv.aerospike.api.proto.ShowIndexServices.ShowIndexResponse;
import com.ltv.aerospike.api.proto.ShowNamespaceServiceGrpc;
import com.ltv.aerospike.api.proto.ShowNamespaceServiceGrpc.ShowNamespaceServiceStub;
import com.ltv.aerospike.api.proto.ShowNamespaceServices.ShowNamespaceRequest;
import com.ltv.aerospike.api.proto.ShowNamespaceServices.ShowNamespaceResponse;
import com.ltv.aerospike.api.proto.ShowSequenceServiceGrpc;
import com.ltv.aerospike.api.proto.ShowSequenceServiceGrpc.ShowSequenceServiceStub;
import com.ltv.aerospike.api.proto.ShowSequenceServices.ShowSequenceRequest;
import com.ltv.aerospike.api.proto.ShowSequenceServices.ShowSequenceResponse;
import com.ltv.aerospike.api.proto.ShowSetServiceGrpc;
import com.ltv.aerospike.api.proto.ShowSetServiceGrpc.ShowSetServiceStub;
import com.ltv.aerospike.api.proto.ShowSetServices.ShowSetRequest;
import com.ltv.aerospike.api.proto.ShowSetServices.ShowSetResponse;
import com.ltv.aerospike.api.proto.TruncateNamespaceServiceGrpc;
import com.ltv.aerospike.api.proto.TruncateNamespaceServiceGrpc.TruncateNamespaceServiceStub;
import com.ltv.aerospike.api.proto.TruncateNamespaceServices.TruncateNamespaceRequest;
import com.ltv.aerospike.api.proto.TruncateNamespaceServices.TruncateNamespaceResponse;
import com.ltv.aerospike.api.proto.TruncateSetServiceGrpc;
import com.ltv.aerospike.api.proto.TruncateSetServiceGrpc.TruncateSetServiceStub;
import com.ltv.aerospike.api.proto.TruncateSetServices.TruncateSetRequest;
import com.ltv.aerospike.api.proto.TruncateSetServices.TruncateSetResponse;
import com.ltv.aerospike.api.proto.UpdateUserServiceGrpc;
import com.ltv.aerospike.api.proto.UpdateUserServiceGrpc.UpdateUserServiceStub;
import com.ltv.aerospike.api.proto.UpdateUserServices.UpdateUserRequest;
import com.ltv.aerospike.api.proto.UpdateUserServices.UpdateUserResponse;
import com.ltv.aerospike.api.util.ErrorCode;

import io.grpc.ManagedChannel;
import io.grpc.Metadata;
import io.grpc.netty.GrpcSslContexts;
import io.grpc.netty.NettyChannelBuilder;
import io.grpc.stub.MetadataUtils;
import io.grpc.stub.StreamObserver;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.util.InsecureTrustManagerFactory;

public class AerospikeClient {
    private static org.apache.log4j.Logger log = org.apache.log4j.Logger.getLogger(AerospikeClient.class.getSimpleName());
    public ManagedChannel channel;
    public static String authenToken;
    public String namespace;
    public String userName;
    public String password;
    public String host;
    public int port;

    public AerospikeClient(String host, int port, String userName, String password) {
        this.host = host;
        this.port = port;
        this.userName = userName;
        this.password = password;
    }

    public LoginResponse connect() {
        LoginResponse response = null;
        try {
            SslContext sslContext = GrpcSslContexts.forClient().trustManager(
                    InsecureTrustManagerFactory.INSTANCE).build();
            channel = NettyChannelBuilder.forAddress(host, port)
                                         .intercept(new AuthenticationInterceptor())
                                         .keepAliveTime(10, TimeUnit.SECONDS)
                                         .keepAliveTimeout(10, TimeUnit.SECONDS)
                                         .keepAliveWithoutCalls(true)
                                         .sslContext(sslContext)
                                         .build();
            response = login();
            if (response.getErrorCode() == ErrorCode.SUCCESS.getValue()) {
                authenToken = response.getAuthenToken();
                log.info("Aerospike client connect success");
            } else {
                log.error("Aerospike client connect failed");
            }
        } catch (Exception ex) {
            log.error("Cannot connect to Aerospike server: " + ex);
        }
        return response;
    }

    public void login(StreamObserver<LoginResponse> response) {
        LoginServiceStub stub = LoginServiceGrpc.newStub(channel);
        stub = MetadataUtils.attachHeaders(stub, getLoginHeader());
        stub.login(LoginRequest.newBuilder().build(), response);
    }

    private Metadata getLoginHeader() {
        Metadata header = new Metadata();
        Metadata.Key<String> userNameKey = Metadata.Key.of("userName", Metadata.ASCII_STRING_MARSHALLER);
        Metadata.Key<String> passwordKey = Metadata.Key.of("password", Metadata.ASCII_STRING_MARSHALLER);
        header.put(userNameKey, userName);
        header.put(passwordKey, password);
        return header;
    }

    public LoginResponse login() {
        LoginServiceBlockingStub loginStub = LoginServiceGrpc.newBlockingStub(channel);
        loginStub = MetadataUtils.attachHeaders(loginStub, getLoginHeader());
        LoginRequest request = LoginRequest.newBuilder().setUserName(userName).build();
        LoginResponse response = loginStub.login(request);
        return response;
    }

    public void logout(StreamObserver<LogoutResponse> response) {
        LogoutServiceStub stub = LogoutServiceGrpc.newStub(channel);
        stub.logout(LogoutRequest.newBuilder().build(), response);
    }

    public LogoutResponse logout() {
        return LogoutServiceGrpc.newBlockingStub(channel).logout(LogoutRequest.newBuilder().build());
    }

    public void createIndex(StreamObserver<CreateIndexResponse> response, String namespace, String set, String bin, String index, IndexType indexType) {
        CreateIndexServiceStub stub = CreateIndexServiceGrpc.newStub(channel);
        stub.createIndex(
                CreateIndexRequest.newBuilder()
                                  .setNamespace(namespace)
                                  .setSet(set)
                                  .setBin(bin)
                                  .setIndex(index)
                                  .setIndexType(indexType)
                                  .build(),
                response);
    }

    public CreateIndexResponse createIndex(String namespace, String set, String bin, String index, IndexType indexType) {
        return CreateIndexServiceGrpc.newBlockingStub(channel).createIndex(
                CreateIndexRequest
                        .newBuilder()
                        .setNamespace(namespace)
                        .setSet(set)
                        .setBin(bin)
                        .setIndex(index)
                        .setIndexType(indexType)
                        .build());
    }

    public void createNamespace(StreamObserver<CreateNamespaceResponse> response, String namespace) {
        CreateNamespaceServiceStub stub = CreateNamespaceServiceGrpc.newStub(channel);
        stub.createNamespace(CreateNamespaceRequest.newBuilder().setNamespace(namespace).build(), response);
    }

    public CreateNamespaceResponse createNamespace(String namespace) {
        return CreateNamespaceServiceGrpc.newBlockingStub(channel).createNamespace(
                CreateNamespaceRequest
                        .newBuilder()
                        .setNamespace(namespace)
                        .build());
    }

    public void createSequence(StreamObserver<CreateSequenceResponse> response, String namespace, String sequence, int start) {
        CreateSequenceServiceStub stub = CreateSequenceServiceGrpc.newStub(channel);
        stub.createSequence(CreateSequenceRequest.newBuilder()
                                                 .setNamespace(namespace)
                                                 .setSequence(sequence)
                                                 .setStart(start)
                                                 .build(),
                            response);
    }

    public CreateSequenceResponse createSequence(String namespace, String sequence, int start) {
        return CreateSequenceServiceGrpc.newBlockingStub(channel).createSequence(
                CreateSequenceRequest
                        .newBuilder()
                        .build());
    }

    public void createSet(StreamObserver<CreateSetResponse> response, String namespace, String set, HashMap<String, String> bins) {
        CreateSetServiceStub stub = CreateSetServiceGrpc.newStub(channel);
        stub.createSet(CreateSetRequest.newBuilder().setNamespace(namespace).setSet(set).putAllStringParams(bins).build(), response);
    }

    public CreateSetResponse createSet(String namespace, String set, HashMap<String, String> bins) {
        return CreateSetServiceGrpc.newBlockingStub(channel).createSet(CreateSetRequest.newBuilder().setNamespace(namespace).setSet(set).putAllStringParams(bins).build());
    }

    public void createUser(StreamObserver<CreateUserResponse> response, String name, String password, Role role) {
        CreateUserServiceStub stub = CreateUserServiceGrpc.newStub(channel);
        stub.createUser(CreateUserRequest.newBuilder().setName(name).setPassword(password).setRole(role).build(), response);
    }

    public CreateUserResponse createUser(String name, String password, Role role) {
        return CreateUserServiceGrpc.newBlockingStub(channel).createUser(
                CreateUserRequest
                        .newBuilder()
                        .setName(name)
                        .setPassword(password)
                        .setRole(role)
                        .build());
    }

    public void delete(StreamObserver<DeleteResponse> response, String namespace, String set, String key) {
        DeleteServiceStub stub = DeleteServiceGrpc.newStub(channel);
        stub.delete(DeleteRequest.newBuilder().setNamespace(namespace).setSet(set).setKey(key).build(), response);
    }

    public DeleteResponse delete(String namespace, String set, String key) {
        return DeleteServiceGrpc.newBlockingStub(channel).delete(
                DeleteRequest
                        .newBuilder()
                        .setNamespace(namespace)
                        .setSet(set)
                        .setKey(key)
                        .build());
    }

    public void deleteUser(StreamObserver<DeleteUserResponse> response, String userName) {
        DeleteUserServiceStub stub = DeleteUserServiceGrpc.newStub(channel);
        stub.deleteUser(DeleteUserRequest.newBuilder().setName(userName).build(), response);
    }

    public DeleteUserResponse deleteUser(String userName) {
        return DeleteUserServiceGrpc.newBlockingStub(channel).deleteUser(
                DeleteUserRequest
                        .newBuilder()
                        .setName(userName)
                        .build());
    }

    public void dropBin(StreamObserver<DropBinResponse> response, String namespace, String set, String bin) {
        DropBinServiceStub stub = DropBinServiceGrpc.newStub(channel);
        stub.dropBin(DropBinRequest.newBuilder().setNamespace(namespace).setSet(set).setBin(bin).build(), response);
    }

    public DropBinResponse dropBin(String namespace, String set, String bin) {
        return DropBinServiceGrpc.newBlockingStub(channel).dropBin(
                DropBinRequest
                        .newBuilder()
                        .setNamespace(namespace)
                        .setSet(set)
                        .setBin(bin)
                        .build());
    }

    public void dropIndex(StreamObserver<DropIndexResponse> response, String namespace, String index) {
        DropIndexServiceStub stub = DropIndexServiceGrpc.newStub(channel);
        stub.dropIndex(DropIndexRequest.newBuilder().setNamespace(namespace).setIndex(index).build(), response);
    }

    public DropIndexResponse dropIndex(String namespace, String index) {
        return DropIndexServiceGrpc.newBlockingStub(channel).dropIndex(
                DropIndexRequest
                        .newBuilder()
                        .setNamespace(namespace)
                        .setIndex(index)
                        .build());
    }

    public void dropNamespace(StreamObserver<DropNamespaceResponse> response, String namespace) {
        DropNamespaceServiceStub stub = DropNamespaceServiceGrpc.newStub(channel);
        stub.dropNamespace(DropNamespaceRequest.newBuilder().setNamespace(namespace).build(), response);
    }

    public DropNamespaceResponse dropNamespace(String namespace) {
        return DropNamespaceServiceGrpc.newBlockingStub(channel).dropNamespace(
                DropNamespaceRequest
                        .newBuilder()
                        .setNamespace(namespace)
                        .build());
    }

    public void dropSequence(StreamObserver<DropSequenceResponse> response, String namespace, String sequence) {
        DropSequenceServiceStub stub = DropSequenceServiceGrpc.newStub(channel);
        stub.dropSequence(DropSequenceRequest.newBuilder().setNamespace(namespace).setSequence(sequence).build(), response);
    }

    public DropSequenceResponse dropSequence(String namespace, String sequence) {
        return DropSequenceServiceGrpc.newBlockingStub(channel).dropSequence(
                DropSequenceRequest
                        .newBuilder()
                        .setNamespace(namespace)
                        .setSequence(sequence)
                        .build());
    }

    public void dropSet(StreamObserver<DropSetResponse> response, String namespace, String set) {
        DropSetServiceStub stub = DropSetServiceGrpc.newStub(channel);
        stub.dropSet(DropSetRequest.newBuilder().setNamespace(namespace).setSet(set).build(), response);
    }

    public DropSetResponse dropSet(String namespace, String set) {
        return DropSetServiceGrpc.newBlockingStub(channel).dropSet(
                DropSetRequest
                        .newBuilder()
                        .setNamespace(namespace)
                        .setSet(set)
                        .build());
    }

    public void getSequence(StreamObserver<GetSequenceResponse> response, String namespace, String sequence) {
        GetSequenceServiceStub stub = GetSequenceServiceGrpc.newStub(channel);
        stub.getSequence(GetSequenceRequest.newBuilder().setNamespace(namespace).setSequence(sequence).build(), response);
    }

    public GetSequenceResponse getSequence(String namespace, String sequence) {
        return GetSequenceServiceGrpc.newBlockingStub(channel).getSequence(
                GetSequenceRequest
                        .newBuilder()
                        .setNamespace(namespace)
                        .setSequence(sequence)
                        .build());
    }

    public void grantNamespace(StreamObserver<GrantNamespaceResponse> response, String userName, String namespace, NamespaceRole role) {
        GrantNamespaceServiceStub stub = GrantNamespaceServiceGrpc.newStub(channel);
        stub.grantNamespace(GrantNamespaceRequest.newBuilder().setUser(userName).setNamespace(namespace).setRole(role).setIsRevoke(false).build(), response);
    }

    public GrantNamespaceResponse grantNamespace(String userName, String namespace, NamespaceRole role) {
        return GrantNamespaceServiceGrpc.newBlockingStub(channel).grantNamespace(
                GrantNamespaceRequest
                        .newBuilder()
                        .setUser(userName)
                        .setNamespace(namespace)
                        .setRole(role)
                        .setIsRevoke(false)
                        .build());
    }

    public void revokeNamespace(StreamObserver<GrantNamespaceResponse> response, String userName, String namespace, NamespaceRole role) {
        GrantNamespaceServiceStub stub = GrantNamespaceServiceGrpc.newStub(channel);
        stub.grantNamespace(GrantNamespaceRequest.newBuilder().setUser(userName).setNamespace(namespace).setRole(role).setIsRevoke(true).build(), response);
    }

    public GrantNamespaceResponse revokeNamespace(String userName, String namespace, NamespaceRole role) {
        return GrantNamespaceServiceGrpc.newBlockingStub(channel).grantNamespace(
                GrantNamespaceRequest
                        .newBuilder()
                        .setUser(userName)
                        .setNamespace(namespace)
                        .setRole(role)
                        .setIsRevoke(true)
                        .build());
    }

    public void grantSet(StreamObserver<GrantSetResponse> response, String userName, String namespace, String set, SetPermission permission) {
        GrantSetServiceStub stub = GrantSetServiceGrpc.newStub(channel);
        stub.grantSet(GrantSetRequest.newBuilder().setUser(userName).setNamespace(namespace).setSet(set).setPermission(permission).setIsRevoke(false).build(), response);
    }

    public GrantSetResponse grantSet(String userName, String namespace, String set, SetPermission permission) {
        return GrantSetServiceGrpc.newBlockingStub(channel).grantSet(
                GrantSetRequest
                        .newBuilder()
                        .setUser(userName)
                        .setNamespace(namespace)
                        .setSet(set)
                        .setPermission(permission)
                        .setIsRevoke(false)
                        .build());
    }

    public void revokeSet(StreamObserver<GrantSetResponse> response, String userName, String namespace, String set, SetPermission permission) {
        GrantSetServiceStub stub = GrantSetServiceGrpc.newStub(channel);
        stub.grantSet(GrantSetRequest.newBuilder().setUser(userName).setNamespace(namespace).setSet(set).setPermission(permission).setIsRevoke(true).build(), response);
    }

    public GrantSetResponse revokeSet(String userName, String namespace, String set, SetPermission permission) {
        return GrantSetServiceGrpc.newBlockingStub(channel).grantSet(
                GrantSetRequest
                        .newBuilder()
                        .setUser(userName)
                        .setNamespace(namespace)
                        .setSet(set)
                        .setPermission(permission)
                        .setIsRevoke(true)
                        .build());
    }

    public void put(StreamObserver<PutResponse> response, RecordExistsAction policy, String namespace, String set, String key, HashMap<String, Object> bins) {
        put(response, policy, namespace, set, key, bins, 0);
    }

    public void put(StreamObserver<PutResponse> response, RecordExistsAction policy, String namespace, String set, String key, HashMap<String, Object> bins, int expiration) {
        if(policy == null) policy = RecordExistsAction.UPDATE;
        PutRequest.Builder builder = PutRequest.newBuilder().setWritePolicy(policy).setNamespace(namespace).setSet(set).setKey(key).setExpiration(expiration);
        PutServiceStub stub = PutServiceGrpc.newStub(channel);
        stub.put(setBinValue(builder, bins).build(), response);
    }

    public PutResponse put(RecordExistsAction policy, String namespace, String set, String key, HashMap<String, Object> bins) {
        return put(policy, namespace, set, key, bins, 0);
    }

    public PutResponse put(RecordExistsAction policy, String namespace, String set, String key, HashMap<String, Object> bins, int expiration) {
        PutRequest.Builder builder = PutRequest.newBuilder().setWritePolicy(policy).setNamespace(namespace).setSet(set).setKey(key).setExpiration(expiration);
        return PutServiceGrpc.newBlockingStub(channel).put(setBinValue(builder, bins).build());
    }

    public void get(StreamObserver<GetResponse> response, String namespace, String set, String key, Integer timeout) {
        if(timeout == null) timeout = 0;
        GetServiceGrpc.newStub(channel).get(GetRequest.newBuilder().setNamespace(namespace).setSet(set).setKey(key).setTimeout(timeout).build(), response);
    }

    public GetResponse get(String namespace, String set, String key, Integer timeout) {
        if(timeout == null) timeout = 0;
        return GetServiceGrpc.newBlockingStub(channel).get(GetRequest.newBuilder().setNamespace(namespace).setSet(set).setKey(key).setTimeout(timeout).build());
    }

    public void renameBin(StreamObserver<RenameBinResponse> response, String namespace, String set, String oldBin, String newBin) {
        RenameBinServiceStub stub = RenameBinServiceGrpc.newStub(channel);
        stub.renameBin(RenameBinRequest.newBuilder().setNamespace(namespace).setSet(set).setOldBin(oldBin).setNewBin(newBin).build(), response);
    }

    public RenameBinResponse renameBin(String namespace, String set, String oldBin, String newBin) {
        return RenameBinServiceGrpc.newBlockingStub(channel).renameBin(
                RenameBinRequest
                        .newBuilder()
                        .setNamespace(namespace)
                        .setSet(set)
                        .setOldBin(oldBin)
                        .setNewBin(newBin)
                        .build());
    }

    public void renameNamespace(StreamObserver<RenameNamespaceResponse> response, String oldName, String newName) {
        RenameNamespaceServiceStub stub = RenameNamespaceServiceGrpc.newStub(channel);
        stub.renameNamespace(RenameNamespaceRequest.newBuilder().setOldName(oldName).setNewName(newName).build(), response);
    }

    public RenameNamespaceResponse renameNamespace(String oldName, String newName) {
        return RenameNamespaceServiceGrpc.newBlockingStub(channel).renameNamespace(
                RenameNamespaceRequest
                        .newBuilder()
                        .setOldName(oldName)
                        .setNewName(newName)
                        .build());
    }

    public void renameSetService(StreamObserver<RenameSetResponse> response, String namespace, String oldSet, String newSet) {
        RenameSetServiceStub stub = RenameSetServiceGrpc.newStub(channel);
        stub.renameSet(RenameSetRequest.newBuilder().setNamespace(namespace).setOldSet(oldSet).setNewSet(newSet).build(), response);
    }

    public RenameSetResponse renameSet(String namespace, String oldSet, String newSet) {
        return RenameSetServiceGrpc.newBlockingStub(channel).renameSet(
                RenameSetRequest
                        .newBuilder()
                        .setNamespace(namespace)
                        .setOldSet(oldSet)
                        .setNewSet(newSet)
                        .build());
    }

    public void showBin(StreamObserver<ShowBinResponse> response, String namespace, String set) {
        ShowBinServiceStub stub = ShowBinServiceGrpc.newStub(channel);
        stub.showBin(ShowBinRequest.newBuilder().setNamespace(namespace).setSet(set).build(), response);
    }

    public ShowBinResponse showBin(String namespace, String set) {
        return ShowBinServiceGrpc.newBlockingStub(channel).showBin(
                ShowBinRequest
                        .newBuilder()
                        .setNamespace(namespace)
                        .setSet(set)
                        .build());
    }

    public void showIndex(StreamObserver<ShowIndexResponse> response, String namespace) {
        ShowIndexServiceStub stub = ShowIndexServiceGrpc.newStub(channel);
        stub.showIndex(ShowIndexRequest.newBuilder().setNamespace(namespace).build(), response);
    }

    public ShowIndexResponse showIndex(String namespace) {
        return ShowIndexServiceGrpc.newBlockingStub(channel).showIndex(
                ShowIndexRequest
                        .newBuilder()
                        .setNamespace(namespace)
                        .build());
    }

    public void showNamespace(StreamObserver<ShowNamespaceResponse> response) {
        ShowNamespaceServiceStub stub = ShowNamespaceServiceGrpc.newStub(channel);
        stub.showNamespace(ShowNamespaceRequest.newBuilder().build(), response);
    }

    public ShowNamespaceResponse showNamespace() {
        return ShowNamespaceServiceGrpc.newBlockingStub(channel).showNamespace(
                ShowNamespaceRequest.newBuilder().build());
    }

    public void showSequence(StreamObserver<ShowSequenceResponse> response, String namespace) {
        ShowSequenceServiceStub stub = ShowSequenceServiceGrpc.newStub(channel);
        stub.showSequence(ShowSequenceRequest.newBuilder().setNamespace(namespace).build(), response);
    }

    public ShowSequenceResponse showSequence(String namespace) {
        return ShowSequenceServiceGrpc.newBlockingStub(channel).showSequence(ShowSequenceRequest.newBuilder().setNamespace(namespace).build());
    }

    public void showSet(StreamObserver<ShowSetResponse> response, String namespace) {
        ShowSetServiceStub stub = ShowSetServiceGrpc.newStub(channel);
        stub.showSet(ShowSetRequest.newBuilder().setNamespace(namespace).build(), response);
    }

    public ShowSetResponse showSet(String namespace) {
        return ShowSetServiceGrpc.newBlockingStub(channel).showSet(ShowSetRequest.newBuilder().setNamespace(namespace).build());
    }

    public void truncateNamespace(StreamObserver<TruncateNamespaceResponse> response, String namespace) {
        TruncateNamespaceServiceStub stub = TruncateNamespaceServiceGrpc.newStub(channel);
        stub.truncateNamespace(TruncateNamespaceRequest.newBuilder().setNamespace(namespace).build(), response);
    }

    public TruncateNamespaceResponse truncateNamespace(String namespace) {
        return TruncateNamespaceServiceGrpc.newBlockingStub(channel).truncateNamespace(
                TruncateNamespaceRequest.newBuilder().setNamespace(namespace).build());
    }

    public void truncateSet(StreamObserver<TruncateSetResponse> response, String namespace, String set) {
        TruncateSetServiceStub stub = TruncateSetServiceGrpc.newStub(channel);
        stub.truncateSet(TruncateSetRequest.newBuilder().setNamespace(namespace).setSet(set).build(), response);
    }

    public TruncateSetResponse truncateSet(String namespace, String set) {
        return TruncateSetServiceGrpc.newBlockingStub(channel).truncateSet(
                TruncateSetRequest
                        .newBuilder()
                        .setNamespace(namespace)
                        .setSet(set)
                        .build());
    }

    public void updateUser(StreamObserver<UpdateUserResponse> response, String userName, String password, UpdateUserRequest.Role role) {
        UpdateUserServiceStub stub = UpdateUserServiceGrpc.newStub(channel);
        stub.updateUser(UpdateUserRequest.newBuilder().setName(userName).setPassword(password).setRole(role).build(), response);
    }

    public UpdateUserResponse updateUser(String userName, String password, UpdateUserRequest.Role role) {
        return UpdateUserServiceGrpc.newBlockingStub(channel).updateUser(
                UpdateUserRequest
                        .newBuilder()
                        .setName(userName)
                        .setPassword(password)
                        .setRole(role)
                        .build());
    }

    private PutRequest.Builder setBinValue(PutRequest.Builder builder, HashMap<String, Object> bins) {
        for(Map.Entry<String, Object> entry : bins.entrySet()) {
            Object value = entry.getValue();
            if(value instanceof String) {
                builder.putStringParams(entry.getKey(), (String) value);
            } else if(value instanceof Integer) {
                builder.putIntParams(entry.getKey(), (int) value);
            } else if(value instanceof Long) {
                builder.putLongParams(entry.getKey(), (long) value);
            } else if(value instanceof Float) {
                builder.putFloatParams(entry.getKey(), (float) value);
            } else if(value instanceof Double) {
                builder.putDoubleParams(entry.getKey(), (double) value);
            } else if(value instanceof Boolean) {
                builder.putBooleanParams(entry.getKey(), (boolean) value);
            } else if(value instanceof ByteString) {
                builder.putBytesParams(entry.getKey(), (ByteString) value);
            }
        }
        return builder;
    }

    private Builder buildQueryRequest(String namespace, String set, Qualifier... qualifiers) {
        Builder builder = QueryRequest.newBuilder().setNamespace(namespace).setSet(set);
        Integer count = 0;
        for(Qualifier qualifier : qualifiers) {
            count++;
            builder = builder.putFields("" + count, qualifier.getField());
            builder = builder.putFilter("" + count, qualifier.getFilter());
            builder = setQueryValue(builder, qualifier.getValue1(), "" + count, true);
            builder = setQueryValue(builder, qualifier.getValue2(), "" + count, false);
        }
        return builder;
    }

    public void query(StreamObserver<QueryResponse> response, String namespace, String set, Qualifier... qualifiers) {
        QueryServiceStub queryStub = QueryServiceGrpc.newStub(channel);
        queryStub.query(buildQueryRequest(namespace, set, qualifiers).build(), response);
    }

    public List<Map<String, Object>> query(String namespace, String set, Qualifier... qualifiers) throws InterruptedException {
        QueryServiceBlockingStub queryStub = QueryServiceGrpc.newBlockingStub(channel);
        Iterator<QueryResponse> queryIterator = queryStub.query(buildQueryRequest(namespace, set, qualifiers).build());
        List<Map<String, Object>> result = new ArrayList();
        while (queryIterator.hasNext()) {
            Map row = parseRecord(queryIterator.next());
            if(row != null && !row.isEmpty()) result.add(row);
        }
        return result;
    }

    public Map<String, Object> getFirstRecord(String namespace, String set, Qualifier... qualifiers) throws InterruptedException {
        List<Map<String, Object>> result = new ArrayList();
        query(new StreamObserver<QueryResponse>() {
            @Override
            public void onNext(QueryResponse response) {
                if(result.isEmpty()) {
                    result.add(parseRecord(response));
                    result.notify();
                }
            }

            @Override
            public void onError(Throwable throwable) {
            }

            @Override
            public void onCompleted() {
            }
        }, namespace, set, qualifiers);
        result.wait();
        if(result != null) return result.get(0);
        else return null;
    }
    
    public Map<String, Object> parseRecord(QueryResponse response){
        HashMap<String, Object> mapBin = new HashMap();
        mapBin.putAll(response.getStringBinMap());
        mapBin.putAll(response.getBooleanBinMap());
        mapBin.putAll(response.getLongBinMap());
        mapBin.putAll(response.getLongBinMap());
        mapBin.putAll(response.getFloatBinMap());
        mapBin.putAll(response.getDoubleBinMap());
        mapBin.putAll(response.getBytesBinMap());
        return mapBin;
    }

    public Map<String, Object> parseRecord(GetResponse response){
        HashMap<String, Object> mapBin = new HashMap();
        mapBin.putAll(response.getStringBinMap());
        mapBin.putAll(response.getBooleanBinMap());
        mapBin.putAll(response.getLongBinMap());
        mapBin.putAll(response.getLongBinMap());
        mapBin.putAll(response.getFloatBinMap());
        mapBin.putAll(response.getDoubleBinMap());
        mapBin.putAll(response.getBytesBinMap());
        return mapBin;
    }
    
    private Builder setQueryValue(Builder builder, Object value, String qualifierId, boolean isValue1) {
        if(value instanceof String) {
            if(isValue1) builder.putStringValue1(qualifierId, (String) value);
            builder.putStringValue2(qualifierId, (String) value);
        } else if(value instanceof Integer) {
            if(isValue1) builder.putIntValue1(qualifierId, (int) value);
            builder.putIntValue2(qualifierId, (int) value);
        } else if(value instanceof Long) {
            if(isValue1) builder.putLongValue1(qualifierId, (long) value);
            builder.putLongValue2(qualifierId, (long) value);
        } else if(value instanceof Float) {
            if(isValue1) builder.putFloatValue1(qualifierId, (float) value);
            builder.putFloatValue2(qualifierId, (float) value);
        } else if(value instanceof Double) {
            if(isValue1) builder.putDoubleValue1(qualifierId, (double) value);
            builder.putDoubleValue2(qualifierId, (double) value);
        } else if(value instanceof Boolean) {
            if(isValue1) builder.putBooleanValue1(qualifierId, (boolean) value);
            builder.putBooleanValue2(qualifierId, (boolean) value);
        } else if(value instanceof ByteString) {
            if(isValue1) builder.putBytesValue1(qualifierId, (ByteString) value);
            builder.putBytesValue2(qualifierId, (ByteString) value);
        }
        return builder;
    }
}
