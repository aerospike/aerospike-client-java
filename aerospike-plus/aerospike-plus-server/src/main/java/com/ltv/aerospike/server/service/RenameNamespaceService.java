package com.ltv.aerospike.server.service;

import java.util.HashMap;

import com.ltv.aerospike.api.proto.RenameNamespaceServiceGrpc.RenameNamespaceServiceImplBase;
import com.ltv.aerospike.api.proto.RenameNamespaceServices.RenameNamespaceRequest;
import com.ltv.aerospike.api.proto.RenameNamespaceServices.RenameNamespaceResponse;
import com.ltv.aerospike.server.business.NamespaceBusiness;
import com.ltv.aerospike.server.business.SessionBusiness;
import com.ltv.aerospike.server.util.AppConstant;

import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;

public class RenameNamespaceService extends RenameNamespaceServiceImplBase {

    @Override
    public void renameNamespace(RenameNamespaceRequest renameNamespaceRequest, StreamObserver<RenameNamespaceResponse> responseObserver) {
        final String token = AppConstant.AUTHEN_TOKEN_KEY.get();
        HashMap session = SessionBusiness.get(token);
        if (session == null) {
            responseObserver.onError(new StatusRuntimeException(Status.UNAUTHENTICATED));
        } else {
            (new NamespaceBusiness(responseObserver)).renameNamespace(renameNamespaceRequest);
        }
    }
}
