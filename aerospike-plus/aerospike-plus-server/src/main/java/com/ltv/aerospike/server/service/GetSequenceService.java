package com.ltv.aerospike.server.service;

import java.util.HashMap;

import com.ltv.aerospike.api.proto.GetSequenceServiceGrpc.GetSequenceServiceImplBase;
import com.ltv.aerospike.api.proto.GetSequenceServices.GetSequenceRequest;
import com.ltv.aerospike.api.proto.GetSequenceServices.GetSequenceResponse;
import com.ltv.aerospike.server.business.SequenceBusiness;
import com.ltv.aerospike.server.business.SessionBusiness;
import com.ltv.aerospike.server.util.AppConstant;

import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;

public class GetSequenceService extends GetSequenceServiceImplBase {

    @Override
    public void getSequence(GetSequenceRequest getSequenceRequest, StreamObserver<GetSequenceResponse> responseObserver) {
        final String token = AppConstant.AUTHEN_TOKEN_KEY.get();
        HashMap session = SessionBusiness.get(token);
        if (session == null) {
            responseObserver.onError(new StatusRuntimeException(Status.UNAUTHENTICATED));
        } else {
            (new SequenceBusiness(responseObserver)).getSequence(getSequenceRequest);
        }
    }
}
