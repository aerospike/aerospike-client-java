package com.ltv.aerospike.server.service;

import java.util.HashMap;

import com.ltv.aerospike.api.proto.GrantSetServiceGrpc.GrantSetServiceImplBase;
import com.ltv.aerospike.api.proto.GrantSetServices.GrantSetRequest;
import com.ltv.aerospike.api.proto.GrantSetServices.GrantSetResponse;
import com.ltv.aerospike.server.business.SessionBusiness;
import com.ltv.aerospike.server.business.UserBusiness;
import com.ltv.aerospike.server.util.AppConstant;

import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;

public class GrantSetService extends GrantSetServiceImplBase {

    @Override
    public void grantSet(GrantSetRequest grantSetRequest, StreamObserver<GrantSetResponse> responseObserver) {
        final String token = AppConstant.AUTHEN_TOKEN_KEY.get();
        HashMap session = SessionBusiness.get(token);
        if (session == null) {
            responseObserver.onError(new StatusRuntimeException(Status.UNAUTHENTICATED));
        } else {
            (new UserBusiness(responseObserver)).grantSet(grantSetRequest);
        }
    }
}
