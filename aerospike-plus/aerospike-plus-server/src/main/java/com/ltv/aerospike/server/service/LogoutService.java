package com.ltv.aerospike.server.service;

import java.util.HashMap;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.Key;
import com.aerospike.client.listener.DeleteListener;
import com.ltv.aerospike.api.proto.LogoutServiceGrpc.LogoutServiceImplBase;
import com.ltv.aerospike.api.proto.LogoutServices.LogoutRequest;
import com.ltv.aerospike.api.proto.LogoutServices.LogoutResponse;
import com.ltv.aerospike.api.util.ErrorCode;
import com.ltv.aerospike.server.business.SessionBusiness;
import com.ltv.aerospike.server.run.StartApp;
import com.ltv.aerospike.server.util.AppConstant;

import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;

public class LogoutService extends LogoutServiceImplBase {

    @Override
    public void logout(LogoutRequest logoutRequest, StreamObserver<LogoutResponse> responseObserver) {
        final String token = AppConstant.AUTHEN_TOKEN_KEY.get();
        HashMap session = SessionBusiness.get(token);
        if (session == null) {
            responseObserver.onError(new StatusRuntimeException(Status.UNAUTHENTICATED));
        } else {
            Key key = new Key(StartApp.aeConnector.AEROSPIKE_NAMESPACE, "" + AppConstant.TABLE_SESSION_ID, token);
            StartApp.aeConnector.aeClient.delete(StartApp.aeConnector.eventLoops.next(),
                 new DeleteListener() {
                     @Override
                     public void onSuccess(Key key, boolean existed) {
                         SessionBusiness.invalidate(token);
                         responseObserver.onNext(
                                 LogoutResponse
                                         .newBuilder()
                                         .setErrorCode(ErrorCode.SUCCESS.getValue()).build());
                         responseObserver.onCompleted();
                     }

                     @Override
                     public void onFailure(AerospikeException exception) {
                         responseObserver.onError(exception);
                     }
                 },
                 null,
                 key);
        }
    }
}
