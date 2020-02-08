package com.ltv.aerospike.server.service;

import java.util.HashMap;

import com.ltv.aerospike.api.proto.GrantNamespaceServiceGrpc.GrantNamespaceServiceImplBase;
import com.ltv.aerospike.api.proto.GrantNamespaceServices.GrantNamespaceRequest;
import com.ltv.aerospike.api.proto.GrantNamespaceServices.GrantNamespaceResponse;
import com.ltv.aerospike.server.business.SessionBusiness;
import com.ltv.aerospike.server.business.UserBusiness;
import com.ltv.aerospike.server.util.AppConstant;

import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;

public class GrantNamespaceService extends GrantNamespaceServiceImplBase {

    @Override
    public void grantNamespace(GrantNamespaceRequest grantNamespaceRequest, StreamObserver<GrantNamespaceResponse> responseObserver) {
        final String token = AppConstant.AUTHEN_TOKEN_KEY.get();
        HashMap session = SessionBusiness.get(token);
        if (session == null) {
            responseObserver.onError(new StatusRuntimeException(Status.UNAUTHENTICATED));
        } else {
            (new UserBusiness(responseObserver)).grantNamespace(grantNamespaceRequest);
        }
    }
}
