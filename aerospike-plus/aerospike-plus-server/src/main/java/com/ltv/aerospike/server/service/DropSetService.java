package com.ltv.aerospike.server.service;

import java.util.HashMap;

import com.ltv.aerospike.api.proto.DropSetServiceGrpc.DropSetServiceImplBase;
import com.ltv.aerospike.api.proto.DropSetServices.DropSetRequest;
import com.ltv.aerospike.api.proto.DropSetServices.DropSetResponse;
import com.ltv.aerospike.server.business.SessionBusiness;
import com.ltv.aerospike.server.business.SetBusiness;
import com.ltv.aerospike.server.util.AppConstant;

import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;

public class DropSetService extends DropSetServiceImplBase {

    @Override
    public void dropSet(DropSetRequest dropSetRequest, StreamObserver<DropSetResponse> responseObserver) {
        final String token = AppConstant.AUTHEN_TOKEN_KEY.get();
        HashMap session = SessionBusiness.get(token);
        if (session == null) {
            responseObserver.onError(new StatusRuntimeException(Status.UNAUTHENTICATED));
        } else {
            (new SetBusiness(responseObserver)).dropSet(dropSetRequest);
        }
    }
}
