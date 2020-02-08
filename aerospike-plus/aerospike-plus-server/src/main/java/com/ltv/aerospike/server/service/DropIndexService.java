package com.ltv.aerospike.server.service;

import java.util.HashMap;

import com.ltv.aerospike.api.proto.DropIndexServiceGrpc.DropIndexServiceImplBase;
import com.ltv.aerospike.api.proto.DropIndexServices.DropIndexRequest;
import com.ltv.aerospike.api.proto.DropIndexServices.DropIndexResponse;
import com.ltv.aerospike.server.business.IndexBusiness;
import com.ltv.aerospike.server.business.SessionBusiness;
import com.ltv.aerospike.server.util.AppConstant;

import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;

public class DropIndexService extends DropIndexServiceImplBase {

    @Override
    public void dropIndex(DropIndexRequest dropIndexRequest, StreamObserver<DropIndexResponse> responseObserver) {
        final String token = AppConstant.AUTHEN_TOKEN_KEY.get();
        HashMap session = SessionBusiness.get(token);
        if (session == null) {
            responseObserver.onError(new StatusRuntimeException(Status.UNAUTHENTICATED));
        } else {
            (new IndexBusiness(responseObserver)).dropIndex(dropIndexRequest);
        }
    }
}
