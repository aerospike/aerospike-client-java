package com.ltv.aerospike.server.service;

import java.util.HashMap;

import com.ltv.aerospike.api.proto.CreateUserServiceGrpc.CreateUserServiceImplBase;
import com.ltv.aerospike.api.proto.CreateUserServices.CreateUserRequest;
import com.ltv.aerospike.api.proto.CreateUserServices.CreateUserResponse;
import com.ltv.aerospike.server.business.SessionBusiness;
import com.ltv.aerospike.server.business.UserBusiness;
import com.ltv.aerospike.server.util.AppConstant;

import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;

public class CreateUserService extends CreateUserServiceImplBase {

    @Override
    public void createUser(CreateUserRequest createUserRequest, StreamObserver<CreateUserResponse> responseObserver) {
        final String token = AppConstant.AUTHEN_TOKEN_KEY.get();
        HashMap session = SessionBusiness.get(token);
        if (session == null) {
            responseObserver.onError(new StatusRuntimeException(Status.UNAUTHENTICATED));
        } else {
            (new UserBusiness(responseObserver)).create(createUserRequest);
        }
    }
}
