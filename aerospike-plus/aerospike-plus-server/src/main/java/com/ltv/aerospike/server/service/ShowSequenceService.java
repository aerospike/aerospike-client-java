package com.ltv.aerospike.server.service;

import java.util.HashMap;

import com.ltv.aerospike.api.proto.ShowSequenceServiceGrpc.ShowSequenceServiceImplBase;
import com.ltv.aerospike.api.proto.ShowSequenceServices.ShowSequenceRequest;
import com.ltv.aerospike.api.proto.ShowSequenceServices.ShowSequenceResponse;
import com.ltv.aerospike.server.business.SequenceBusiness;
import com.ltv.aerospike.server.business.SessionBusiness;
import com.ltv.aerospike.server.util.AppConstant;

import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;

public class ShowSequenceService extends ShowSequenceServiceImplBase {

    @Override
    public void showSequence(ShowSequenceRequest showSequenceRequest, StreamObserver<ShowSequenceResponse> responseObserver) {
        final String token = AppConstant.AUTHEN_TOKEN_KEY.get();
        HashMap session = SessionBusiness.get(token);
        if (session == null) {
            responseObserver.onError(new StatusRuntimeException(Status.UNAUTHENTICATED));
        } else {
            (new SequenceBusiness(responseObserver)).showSequence(showSequenceRequest);
        }
    }
}
