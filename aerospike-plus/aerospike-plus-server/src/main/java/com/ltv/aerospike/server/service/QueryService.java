package com.ltv.aerospike.server.service;

import java.util.HashMap;

import com.ltv.aerospike.api.proto.QueryServiceGrpc.QueryServiceImplBase;
import com.ltv.aerospike.api.proto.QueryServices.QueryRequest;
import com.ltv.aerospike.api.proto.QueryServices.QueryResponse;
import com.ltv.aerospike.server.business.QueryBusiness;
import com.ltv.aerospike.server.business.SessionBusiness;
import com.ltv.aerospike.server.util.AppConstant;

import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;

public class QueryService extends QueryServiceImplBase {

    @Override
    public void query(QueryRequest queryRequest, StreamObserver<QueryResponse> responseObserver) {
        final String token = AppConstant.AUTHEN_TOKEN_KEY.get();
        HashMap session = SessionBusiness.get(token);
        if (session == null) {
            responseObserver.onError(new StatusRuntimeException(Status.UNAUTHENTICATED));
        } else {
            (new QueryBusiness(responseObserver)).query(queryRequest);
        }
    }
}
