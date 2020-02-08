package com.ltv.aerospike.server.service;

import java.util.HashMap;

import com.ltv.aerospike.api.proto.DeleteServiceGrpc.DeleteServiceImplBase;
import com.ltv.aerospike.api.proto.DeleteServices.DeleteRequest;
import com.ltv.aerospike.api.proto.DeleteServices.DeleteResponse;
import com.ltv.aerospike.server.business.SessionBusiness;
import com.ltv.aerospike.server.business.TransactionBusiness;
import com.ltv.aerospike.server.util.AppConstant;

import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;

public class DeleteService extends DeleteServiceImplBase {

    @Override
    public void delete(DeleteRequest deleteRequest, StreamObserver<DeleteResponse> responseObserver) {
        final String token = AppConstant.AUTHEN_TOKEN_KEY.get();
        HashMap session = SessionBusiness.get(token);
        if (session == null) {
            responseObserver.onError(new StatusRuntimeException(Status.UNAUTHENTICATED));
        } else {
            (new TransactionBusiness(responseObserver)).delete(deleteRequest);
        }
    }
}
