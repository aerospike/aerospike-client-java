package com.ltv.aerospike.server.service;

import java.util.HashMap;

import com.ltv.aerospike.api.proto.TruncateNamespaceServiceGrpc.TruncateNamespaceServiceImplBase;
import com.ltv.aerospike.api.proto.TruncateNamespaceServices.TruncateNamespaceRequest;
import com.ltv.aerospike.api.proto.TruncateNamespaceServices.TruncateNamespaceResponse;
import com.ltv.aerospike.server.business.NamespaceBusiness;
import com.ltv.aerospike.server.business.SessionBusiness;
import com.ltv.aerospike.server.util.AppConstant;

import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;

public class TruncateNamespaceService extends TruncateNamespaceServiceImplBase {

    @Override
    public void truncateNamespace(TruncateNamespaceRequest truncateNamespaceRequest, StreamObserver<TruncateNamespaceResponse> responseObserver) {
        final String token = AppConstant.AUTHEN_TOKEN_KEY.get();
        HashMap session = SessionBusiness.get(token);
        if (session == null) {
            responseObserver.onError(new StatusRuntimeException(Status.UNAUTHENTICATED));
        } else {
            (new NamespaceBusiness(responseObserver)).truncateNamespace(truncateNamespaceRequest);
        }
    }
}
