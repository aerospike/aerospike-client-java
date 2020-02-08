package com.ltv.aerospike.server.service;

import java.util.HashMap;

import com.ltv.aerospike.api.proto.ShowIndexServiceGrpc.ShowIndexServiceImplBase;
import com.ltv.aerospike.api.proto.ShowIndexServices.ShowIndexRequest;
import com.ltv.aerospike.api.proto.ShowIndexServices.ShowIndexResponse;
import com.ltv.aerospike.server.business.IndexBusiness;
import com.ltv.aerospike.server.business.SessionBusiness;
import com.ltv.aerospike.server.util.AppConstant;

import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;

public class ShowIndexService extends ShowIndexServiceImplBase {

    @Override
    public void showIndex(ShowIndexRequest showIndexRequest, StreamObserver<ShowIndexResponse> responseObserver) {
        final String token = AppConstant.AUTHEN_TOKEN_KEY.get();
        HashMap session = SessionBusiness.get(token);
        if (session == null) {
            responseObserver.onError(new StatusRuntimeException(Status.UNAUTHENTICATED));
        } else {
            (new IndexBusiness(responseObserver)).showIndex(showIndexRequest);
        }
    }
}
