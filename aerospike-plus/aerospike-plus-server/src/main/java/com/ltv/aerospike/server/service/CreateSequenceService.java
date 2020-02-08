package com.ltv.aerospike.server.service;

import java.util.HashMap;

import com.ltv.aerospike.api.proto.CreateSequenceServiceGrpc.CreateSequenceServiceImplBase;
import com.ltv.aerospike.api.proto.CreateSequenceServices.CreateSequenceRequest;
import com.ltv.aerospike.api.proto.CreateSequenceServices.CreateSequenceResponse;
import com.ltv.aerospike.server.business.SequenceBusiness;
import com.ltv.aerospike.server.business.SessionBusiness;
import com.ltv.aerospike.server.util.AppConstant;

import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;

public class CreateSequenceService extends CreateSequenceServiceImplBase {

    @Override
    public void createSequence(CreateSequenceRequest createSequenceRequest, StreamObserver<CreateSequenceResponse> responseObserver) {
        final String token = AppConstant.AUTHEN_TOKEN_KEY.get();
        HashMap session = SessionBusiness.get(token);
        if (session == null) {
            responseObserver.onError(new StatusRuntimeException(Status.UNAUTHENTICATED));
        } else {
            (new SequenceBusiness(responseObserver)).createSequence(createSequenceRequest);
        }
    }
}
