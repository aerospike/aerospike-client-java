package com.ltv.aerospike.server.service;

import java.util.HashMap;

import com.ltv.aerospike.api.proto.DropSequenceServiceGrpc.DropSequenceServiceImplBase;
import com.ltv.aerospike.api.proto.DropSequenceServices.DropSequenceRequest;
import com.ltv.aerospike.api.proto.DropSequenceServices.DropSequenceResponse;
import com.ltv.aerospike.server.business.SequenceBusiness;
import com.ltv.aerospike.server.business.SessionBusiness;
import com.ltv.aerospike.server.util.AppConstant;

import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;

public class DropSequenceService extends DropSequenceServiceImplBase {

    @Override
    public void dropSequence(DropSequenceRequest dropSequenceRequest, StreamObserver<DropSequenceResponse> responseObserver) {
        final String token = AppConstant.AUTHEN_TOKEN_KEY.get();
        HashMap session = SessionBusiness.get(token);
        if (session == null) {
            responseObserver.onError(new StatusRuntimeException(Status.UNAUTHENTICATED));
        } else {
            (new SequenceBusiness(responseObserver)).dropSequence(dropSequenceRequest);
        }
    }
}
