package com.ltv.aerospike.server.service;

import java.util.HashMap;

import com.ltv.aerospike.api.proto.DeleteUserServiceGrpc.DeleteUserServiceImplBase;
import com.ltv.aerospike.api.proto.DeleteUserServices.DeleteUserRequest;
import com.ltv.aerospike.api.proto.DeleteUserServices.DeleteUserResponse;
import com.ltv.aerospike.server.business.SessionBusiness;
import com.ltv.aerospike.server.business.UserBusiness;
import com.ltv.aerospike.server.util.AppConstant;

import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;

public class DeleteUserService extends DeleteUserServiceImplBase {

    @Override
    public void deleteUser(DeleteUserRequest deleteUserRequest, StreamObserver<DeleteUserResponse> responseObserver) {
        final String token = AppConstant.AUTHEN_TOKEN_KEY.get();
        HashMap session = SessionBusiness.get(token);
        if (session == null) {
            responseObserver.onError(new StatusRuntimeException(Status.UNAUTHENTICATED));
        } else {
            (new UserBusiness(responseObserver)).delete(deleteUserRequest);
        }
    }
}
