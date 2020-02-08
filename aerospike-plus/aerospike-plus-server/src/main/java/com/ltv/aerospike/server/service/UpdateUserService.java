package com.ltv.aerospike.server.service;

import java.util.HashMap;

import com.ltv.aerospike.api.proto.UpdateUserServiceGrpc.UpdateUserServiceImplBase;
import com.ltv.aerospike.api.proto.UpdateUserServices.UpdateUserRequest;
import com.ltv.aerospike.api.proto.UpdateUserServices.UpdateUserResponse;
import com.ltv.aerospike.server.business.SessionBusiness;
import com.ltv.aerospike.server.business.UserBusiness;
import com.ltv.aerospike.server.util.AppConstant;

import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;

public class UpdateUserService extends UpdateUserServiceImplBase {

    @Override
    public void updateUser(UpdateUserRequest updateUserRequest, StreamObserver<UpdateUserResponse> responseObserver) {
        final String token = AppConstant.AUTHEN_TOKEN_KEY.get();
        HashMap session = SessionBusiness.get(token);
        if (session == null) {
            responseObserver.onError(new StatusRuntimeException(Status.UNAUTHENTICATED));
        } else {
            (new UserBusiness(responseObserver)).update(updateUserRequest);
        }
    }
}
