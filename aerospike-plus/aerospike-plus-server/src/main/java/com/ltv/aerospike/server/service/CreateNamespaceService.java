package com.ltv.aerospike.server.service;

import java.util.HashMap;

import com.ltv.aerospike.api.proto.CreateNamespaceServiceGrpc.CreateNamespaceServiceImplBase;
import com.ltv.aerospike.api.proto.CreateNamespaceServices.CreateNamespaceRequest;
import com.ltv.aerospike.api.proto.CreateNamespaceServices.CreateNamespaceResponse;
import com.ltv.aerospike.server.business.NamespaceBusiness;
import com.ltv.aerospike.server.business.SessionBusiness;
import com.ltv.aerospike.server.util.AppConstant;

import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;

public class CreateNamespaceService extends CreateNamespaceServiceImplBase {

    @Override
    public void createNamespace(CreateNamespaceRequest createNamespaceRequest, StreamObserver<CreateNamespaceResponse> responseObserver) {
        final String token = AppConstant.AUTHEN_TOKEN_KEY.get();
        HashMap session = SessionBusiness.get(token);
        if (session == null) {
            responseObserver.onError(new StatusRuntimeException(Status.UNAUTHENTICATED));
        } else {
            (new NamespaceBusiness(responseObserver)).createNamespace(createNamespaceRequest);
        }
    }
}
