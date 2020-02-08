package com.ltv.aerospike.server.service;

import java.util.HashMap;

import com.ltv.aerospike.api.proto.RenameSetServiceGrpc.RenameSetServiceImplBase;
import com.ltv.aerospike.api.proto.RenameSetServices.RenameSetRequest;
import com.ltv.aerospike.api.proto.RenameSetServices.RenameSetResponse;
import com.ltv.aerospike.server.business.SessionBusiness;
import com.ltv.aerospike.server.business.SetBusiness;
import com.ltv.aerospike.server.util.AppConstant;

import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;

public class RenameSetService extends RenameSetServiceImplBase {

    @Override
    public void renameSet(RenameSetRequest renameSetRequest, StreamObserver<RenameSetResponse> responseObserver) {
        final String token = AppConstant.AUTHEN_TOKEN_KEY.get();
        HashMap session = SessionBusiness.get(token);
        if (session == null) {
            responseObserver.onError(new StatusRuntimeException(Status.UNAUTHENTICATED));
        } else {
            (new SetBusiness(responseObserver)).renameSet(renameSetRequest);
        }
    }
}
