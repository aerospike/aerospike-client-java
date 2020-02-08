/*
 * Copyright (c) 2019 LINE Corporation. All rights reserved.
 * LINE Corporation PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 */

package com.ltv.aerospike.server.business;

import org.apache.log4j.Logger;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.Record;
import com.aerospike.client.async.EventLoops;
import com.aerospike.helper.query.QueryEngine;
import com.ltv.aerospike.server.run.StartApp;
import com.ltv.aerospike.server.util.AppConstant;

import io.grpc.stub.StreamObserver;

public class BaseBusiness {
    private static Logger log = Logger.getLogger(BaseBusiness.class.getSimpleName());

    public StreamObserver responseObserver;
    public AerospikeClient aeClient;
    public String AEROSPIKE_NAMESPACE;
    public EventLoops eventLoops;
    public QueryEngine queryEngine;

    public BaseBusiness(StreamObserver responseObserver) {
        this.responseObserver = responseObserver;
        aeClient = StartApp.aeConnector.aeClient;
        AEROSPIKE_NAMESPACE = StartApp.aeConnector.AEROSPIKE_NAMESPACE;
        eventLoops = StartApp.aeConnector.eventLoops;
        queryEngine = StartApp.aeConnector.queryEngine;
    }

    public void response(StreamObserver responseObserver, Object response) {
        try {
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        } catch (Exception ex) {
            log.error("Response failed: " + ex);
        }
    }

    public boolean isNull(StreamObserver responseObserver, Object param, Object failedResponse) {
        if(param == null) {
            response(responseObserver, failedResponse);
            return true;
        }
        return false;
    }

    public boolean isEmpty(StreamObserver responseObserver, Record param, Object failedResponse) {
        return isNull(responseObserver, param, failedResponse);
    }

    public boolean isDuplicate(StreamObserver responseObserver, Record param, Object failedResponse) {
        if(param != null) {
            response(responseObserver, failedResponse);
            return true;
        }
        return false;
    }

    public boolean isEqual(StreamObserver responseObserver, String oldName, String newName, Object failedResponse) {
        if(oldName == newName) {
            response(responseObserver, failedResponse);
            return true;
        }
        return false;
    }

    public boolean hasRole(StreamObserver responseObserver, String role, Object failedResponse) {
        return hasRole(responseObserver, role, null, failedResponse);
    }

    public boolean hasRole(StreamObserver responseObserver, String role, Record namespaceRecord, Object failedResponse) {
        if(SessionBusiness.hasRole(AppConstant.AUTHEN_TOKEN_KEY.get(), role, namespaceRecord)) return true;
        response(responseObserver, failedResponse);
        return false;
    }

    public boolean hasPermission(StreamObserver responseObserver, Record namespaceRecord, Record setRecord, String permission, Object failedResponse) {
        if(SessionBusiness.hasPermission(AppConstant.AUTHEN_TOKEN_KEY.get(), namespaceRecord, setRecord, permission)) return true;
        response(responseObserver, failedResponse);
        return false;
    }
}
