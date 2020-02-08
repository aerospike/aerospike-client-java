package com.ltv.aerospike.server.service;

import java.util.HashMap;

import com.ltv.aerospike.api.proto.LoginServiceGrpc.LoginServiceImplBase;
import com.ltv.aerospike.api.proto.LoginServices.LoginRequest;
import com.ltv.aerospike.api.proto.LoginServices.LoginResponse;
import com.ltv.aerospike.api.util.ErrorCode;
import com.ltv.aerospike.server.business.SessionBusiness;
import com.ltv.aerospike.server.util.AppConstant;

import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;

public class LoginService extends LoginServiceImplBase {

    @Override
    public void login(LoginRequest loginRequest, StreamObserver<LoginResponse> responseObserver) {
        final String token = AppConstant.AUTHEN_TOKEN_KEY.get();
        HashMap session = SessionBusiness.get(token);
        if (session == null) {
            responseObserver.onError(new StatusRuntimeException(Status.UNAUTHENTICATED));
        } else {
            responseObserver.onNext(
                    LoginResponse
                            .newBuilder()
                            .setAuthenToken(AppConstant.AUTHEN_TOKEN_KEY.get())
                            .setErrorCode(ErrorCode.SUCCESS.getValue()).build());
            responseObserver.onCompleted();
        }
    }
}
