/*
 * Copyright (c) 2019 LINE Corporation. All rights reserved.
 * LINE Corporation PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 */

package com.ltv.aerospike.server.filter;

import java.util.HashMap;

import org.apache.commons.codec.digest.DigestUtils;

import com.aerospike.client.Bin;
import com.aerospike.client.Record;
import com.aerospike.client.query.Filter;
import com.aerospike.client.query.RecordSet;
import com.aerospike.client.query.Statement;
import com.ltv.aerospike.server.business.SessionBusiness;
import com.ltv.aerospike.server.repository.Connector;
import com.ltv.aerospike.server.run.StartApp;
import com.ltv.aerospike.server.util.AppConstant;

import io.grpc.Context;
import io.grpc.Contexts;
import io.grpc.Metadata;
import io.grpc.Metadata.Key;
import io.grpc.ServerCall;
import io.grpc.ServerCall.Listener;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;

public class AuthorizationInterceptor implements ServerInterceptor {
    private static org.apache.log4j.Logger log = org.apache.log4j.Logger.getLogger(AuthorizationInterceptor.class.getSimpleName());

    public <ReqT, RespT> Listener<ReqT> interceptCall(final ServerCall<ReqT, RespT> serverCall, final Metadata metadata, final ServerCallHandler<ReqT, RespT> serverCallHandler) {
        final String token = metadata.get(Key.of("authen_token", Metadata.ASCII_STRING_MARSHALLER));
        HashMap session = SessionBusiness.get(token);

        if (session == null || session.get("userName") == null) {
            final String userName = metadata.get(Key.of("userName", Metadata.ASCII_STRING_MARSHALLER));
            final String password = metadata.get(Key.of("password", Metadata.ASCII_STRING_MARSHALLER));

            RecordSet rs = null;
            try {
                if (userName != null || password != null) {
                    Statement stmt = new Statement();
                    stmt.setNamespace(StartApp.aeConnector.AEROSPIKE_NAMESPACE);
                    stmt.setSetName(AppConstant.TABLE_USER_ID);
                    stmt.setFilter(Filter.equal("name", userName));

                    rs = StartApp.aeConnector.aeClient.query(null, stmt);
                    if (rs.next()) {
                        Record record = rs.getRecord();
                        if (DigestUtils.sha256Hex(StartApp.config.getConfig("secret-key") + password)
                                       .equals(record.getString("password"))) {
                            String authenToken = DigestUtils.sha256Hex(
                                    userName + StartApp.config.getConfig("secret-key") + password);
                            SessionBusiness.put(
                                    authenToken,
                                    record.getString(AppConstant.KEY),
                                    record.getString("name"),
                                    record.getString("role"));
                            com.aerospike.client.Key key = new com.aerospike.client.Key(
                                    StartApp.aeConnector.AEROSPIKE_NAMESPACE,
                                    "" + AppConstant.TABLE_SESSION_ID, authenToken);
                            StartApp.aeConnector.aeClient.put(StartApp.aeConnector.eventLoops.next(),
                                                              Connector.defaultWriteListener,
                                                              null,
                                                              key,
                                                              new Bin("token", authenToken),
                                                              new Bin(AppConstant.KEY, record.getString(AppConstant.KEY)),
                                                              new Bin("name", record.getString("name")),
                                                              new Bin("role", record.getString("role")));
                            Context ctx = Context.current().withValue(AppConstant.AUTHEN_TOKEN_KEY,
                                                                      authenToken);
                            return Contexts.interceptCall(ctx, serverCall, metadata, serverCallHandler);
                        }
                    }
                }
            } catch (Exception ex) {
                log.error(ex);
            } finally {
                if(rs != null) rs.close();
            }
        }
        Context ctx = Context.current().withValue(AppConstant.AUTHEN_TOKEN_KEY, token);
        return Contexts.interceptCall(ctx, serverCall, metadata, serverCallHandler);
    }
}