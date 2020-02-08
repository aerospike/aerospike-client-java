package com.ltv.aerospike.server.service;

import java.util.HashMap;

import com.ltv.aerospike.api.proto.ShowNamespaceServiceGrpc.ShowNamespaceServiceImplBase;
import com.ltv.aerospike.api.proto.ShowNamespaceServices.ShowNamespaceRequest;
import com.ltv.aerospike.api.proto.ShowNamespaceServices.ShowNamespaceResponse;
import com.ltv.aerospike.server.business.NamespaceBusiness;
import com.ltv.aerospike.server.business.SessionBusiness;
import com.ltv.aerospike.server.util.AppConstant;

import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;

public class ShowNamespaceService extends ShowNamespaceServiceImplBase {

    @Override
    public void showNamespace(ShowNamespaceRequest showNamespaceRequest, StreamObserver<ShowNamespaceResponse> responseObserver) {
        final String token = AppConstant.AUTHEN_TOKEN_KEY.get();
        HashMap session = SessionBusiness.get(token);
        if (session == null) {
            responseObserver.onError(new StatusRuntimeException(Status.UNAUTHENTICATED));
        } else {
            (new NamespaceBusiness(responseObserver)).showNamespace();
        }
    }
}
