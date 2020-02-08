package com.ltv.aerospike.server.service;

import java.util.HashMap;

import com.ltv.aerospike.api.proto.ShowBinServiceGrpc.ShowBinServiceImplBase;
import com.ltv.aerospike.api.proto.ShowBinServices.ShowBinRequest;
import com.ltv.aerospike.api.proto.ShowBinServices.ShowBinResponse;
import com.ltv.aerospike.server.business.BinBusiness;
import com.ltv.aerospike.server.business.SessionBusiness;
import com.ltv.aerospike.server.util.AppConstant;

import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;

public class ShowBinService extends ShowBinServiceImplBase {

    @Override
    public void showBin(ShowBinRequest showBinRequest, StreamObserver<ShowBinResponse> responseObserver) {
        final String token = AppConstant.AUTHEN_TOKEN_KEY.get();
        HashMap session = SessionBusiness.get(token);
        if (session == null) {
            responseObserver.onError(new StatusRuntimeException(Status.UNAUTHENTICATED));
        } else {
            (new BinBusiness(responseObserver)).showBin(showBinRequest);
        }
    }
}
