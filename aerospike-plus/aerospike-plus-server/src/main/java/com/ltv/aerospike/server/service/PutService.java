package com.ltv.aerospike.server.service;

import java.util.HashMap;

import com.ltv.aerospike.api.proto.PutServiceGrpc.PutServiceImplBase;
import com.ltv.aerospike.api.proto.PutServices.PutRequest;
import com.ltv.aerospike.api.proto.PutServices.PutResponse;
import com.ltv.aerospike.server.business.SessionBusiness;
import com.ltv.aerospike.server.business.TransactionBusiness;
import com.ltv.aerospike.server.util.AppConstant;

import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;

public class PutService extends PutServiceImplBase {

    @Override
    public void put(PutRequest putRequest, StreamObserver<PutResponse> responseObserver) {
        final String token = AppConstant.AUTHEN_TOKEN_KEY.get();
        HashMap session = SessionBusiness.get(token);
        if (session == null) {
            responseObserver.onError(new StatusRuntimeException(Status.UNAUTHENTICATED));
        } else {
            (new TransactionBusiness(responseObserver)).put(putRequest);
        }
    }
}
