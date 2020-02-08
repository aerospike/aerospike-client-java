package com.ltv.aerospike.server.service;

import java.util.HashMap;

import com.ltv.aerospike.api.proto.DropNamespaceServiceGrpc.DropNamespaceServiceImplBase;
import com.ltv.aerospike.api.proto.DropNamespaceServices.DropNamespaceRequest;
import com.ltv.aerospike.api.proto.DropNamespaceServices.DropNamespaceResponse;
import com.ltv.aerospike.server.business.NamespaceBusiness;
import com.ltv.aerospike.server.business.SessionBusiness;
import com.ltv.aerospike.server.util.AppConstant;

import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;

public class DropNamespaceService extends DropNamespaceServiceImplBase {

    @Override
    public void dropNamespace(DropNamespaceRequest dropNamespaceRequest, StreamObserver<DropNamespaceResponse> responseObserver) {
        final String token = AppConstant.AUTHEN_TOKEN_KEY.get();
        HashMap session = SessionBusiness.get(token);
        if (session == null) {
            responseObserver.onError(new StatusRuntimeException(Status.UNAUTHENTICATED));
        } else {
            (new NamespaceBusiness(responseObserver)).dropNamespace(dropNamespaceRequest);
        }
    }
}
