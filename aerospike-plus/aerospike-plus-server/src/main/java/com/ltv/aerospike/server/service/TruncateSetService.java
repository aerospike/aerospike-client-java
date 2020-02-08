package com.ltv.aerospike.server.service;

import java.util.HashMap;

import com.ltv.aerospike.api.proto.TruncateSetServiceGrpc.TruncateSetServiceImplBase;
import com.ltv.aerospike.api.proto.TruncateSetServices.TruncateSetRequest;
import com.ltv.aerospike.api.proto.TruncateSetServices.TruncateSetResponse;
import com.ltv.aerospike.server.business.SessionBusiness;
import com.ltv.aerospike.server.business.SetBusiness;
import com.ltv.aerospike.server.util.AppConstant;

import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;

public class TruncateSetService extends TruncateSetServiceImplBase {

    @Override
    public void truncateSet(TruncateSetRequest truncateSetRequest, StreamObserver<TruncateSetResponse> responseObserver) {
        final String token = AppConstant.AUTHEN_TOKEN_KEY.get();
        HashMap session = SessionBusiness.get(token);
        if (session == null) {
            responseObserver.onError(new StatusRuntimeException(Status.UNAUTHENTICATED));
        } else {
            (new SetBusiness(responseObserver)).truncateSet(truncateSetRequest);
        }
    }
}
