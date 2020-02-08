package com.ltv.aerospike.server.service;

import java.util.HashMap;

import com.ltv.aerospike.api.proto.DropBinServiceGrpc.DropBinServiceImplBase;
import com.ltv.aerospike.api.proto.DropBinServices.DropBinRequest;
import com.ltv.aerospike.api.proto.DropBinServices.DropBinResponse;
import com.ltv.aerospike.server.business.BinBusiness;
import com.ltv.aerospike.server.business.SessionBusiness;
import com.ltv.aerospike.server.util.AppConstant;

import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;

public class DropBinService extends DropBinServiceImplBase {

    @Override
    public void dropBin(DropBinRequest dropBinRequest, StreamObserver<DropBinResponse> responseObserver) {
        final String token = AppConstant.AUTHEN_TOKEN_KEY.get();
        HashMap session = SessionBusiness.get(token);
        if (session == null) {
            responseObserver.onError(new StatusRuntimeException(Status.UNAUTHENTICATED));
        } else {
            (new BinBusiness(responseObserver)).dropBin(dropBinRequest);
        }
    }
}
