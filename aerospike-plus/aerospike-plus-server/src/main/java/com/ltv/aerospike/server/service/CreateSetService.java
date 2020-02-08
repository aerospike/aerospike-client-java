package com.ltv.aerospike.server.service;

import java.util.HashMap;

import com.ltv.aerospike.api.proto.CreateSetServiceGrpc.CreateSetServiceImplBase;
import com.ltv.aerospike.api.proto.CreateSetServices.CreateSetRequest;
import com.ltv.aerospike.api.proto.CreateSetServices.CreateSetResponse;
import com.ltv.aerospike.server.business.SessionBusiness;
import com.ltv.aerospike.server.business.SetBusiness;
import com.ltv.aerospike.server.util.AppConstant;

import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;

public class CreateSetService extends CreateSetServiceImplBase {

    @Override
    public void createSet(CreateSetRequest createSetRequest, StreamObserver<CreateSetResponse> responseObserver) {
        final String token = AppConstant.AUTHEN_TOKEN_KEY.get();
        HashMap session = SessionBusiness.get(token);
        if (session == null) {
            responseObserver.onError(new StatusRuntimeException(Status.UNAUTHENTICATED));
        } else {
            (new SetBusiness(responseObserver)).createSet(createSetRequest);
        }
    }
}
