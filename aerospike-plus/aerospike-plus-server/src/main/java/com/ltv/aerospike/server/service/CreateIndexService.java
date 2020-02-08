package com.ltv.aerospike.server.service;

import java.util.HashMap;

import com.ltv.aerospike.api.proto.CreateIndexServiceGrpc.CreateIndexServiceImplBase;
import com.ltv.aerospike.api.proto.CreateIndexServices.CreateIndexRequest;
import com.ltv.aerospike.api.proto.CreateIndexServices.CreateIndexResponse;
import com.ltv.aerospike.server.business.IndexBusiness;
import com.ltv.aerospike.server.business.SessionBusiness;
import com.ltv.aerospike.server.util.AppConstant;

import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;

public class CreateIndexService extends CreateIndexServiceImplBase {

    @Override
    public void createIndex(CreateIndexRequest createIndexRequest, StreamObserver<CreateIndexResponse> responseObserver) {
        final String token = AppConstant.AUTHEN_TOKEN_KEY.get();
        HashMap session = SessionBusiness.get(token);
        if (session == null) {
            responseObserver.onError(new StatusRuntimeException(Status.UNAUTHENTICATED));
        } else {
            (new IndexBusiness(responseObserver)).createIndex(createIndexRequest);
        }
    }
}
