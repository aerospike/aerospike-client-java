package com.ltv.aerospike.server.service;

import java.util.HashMap;

import com.ltv.aerospike.api.proto.ShowSetServiceGrpc.ShowSetServiceImplBase;
import com.ltv.aerospike.api.proto.ShowSetServices.ShowSetRequest;
import com.ltv.aerospike.api.proto.ShowSetServices.ShowSetResponse;
import com.ltv.aerospike.server.business.SessionBusiness;
import com.ltv.aerospike.server.business.SetBusiness;
import com.ltv.aerospike.server.util.AppConstant;

import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;

public class ShowSetService extends ShowSetServiceImplBase {

    @Override
    public void showSet(ShowSetRequest showSetRequest, StreamObserver<ShowSetResponse> responseObserver) {
        final String token = AppConstant.AUTHEN_TOKEN_KEY.get();
        HashMap session = SessionBusiness.get(token);
        if (session == null) {
            responseObserver.onError(new StatusRuntimeException(Status.UNAUTHENTICATED));
        } else {
            (new SetBusiness(responseObserver)).showSet(showSetRequest);
        }
    }
}
