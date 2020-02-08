package com.ltv.aerospike.server.service;

import java.util.HashMap;

import com.ltv.aerospike.api.proto.GetServiceGrpc.GetServiceImplBase;
import com.ltv.aerospike.api.proto.GetServices.GetRequest;
import com.ltv.aerospike.api.proto.GetServices.GetResponse;
import com.ltv.aerospike.server.business.QueryBusiness;
import com.ltv.aerospike.server.business.SessionBusiness;
import com.ltv.aerospike.server.util.AppConstant;

import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;

public class GetService extends GetServiceImplBase {

    @Override
    public void get(GetRequest getRequest, StreamObserver<GetResponse> responseObserver) {
        final String token = AppConstant.AUTHEN_TOKEN_KEY.get();
        HashMap session = SessionBusiness.get(token);
        if (session == null) {
            responseObserver.onError(new StatusRuntimeException(Status.UNAUTHENTICATED));
        } else {
            (new QueryBusiness(responseObserver)).get(getRequest);
        }
    }
}
