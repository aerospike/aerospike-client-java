package com.ltv.aerospike.server.service;

import java.util.HashMap;

import com.ltv.aerospike.api.proto.RenameBinServiceGrpc.RenameBinServiceImplBase;
import com.ltv.aerospike.api.proto.RenameBinServices.RenameBinRequest;
import com.ltv.aerospike.api.proto.RenameBinServices.RenameBinResponse;
import com.ltv.aerospike.server.business.BinBusiness;
import com.ltv.aerospike.server.business.SessionBusiness;
import com.ltv.aerospike.server.util.AppConstant;

import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;

public class RenameBinService extends RenameBinServiceImplBase {

    @Override
    public void renameBin(RenameBinRequest renameBinRequest, StreamObserver<RenameBinResponse> responseObserver) {
        final String token = AppConstant.AUTHEN_TOKEN_KEY.get();
        HashMap session = SessionBusiness.get(token);
        if (session == null) {
            responseObserver.onError(new StatusRuntimeException(Status.UNAUTHENTICATED));
        } else {
            (new BinBusiness(responseObserver)).renameBin(renameBinRequest);
        }
    }
}
