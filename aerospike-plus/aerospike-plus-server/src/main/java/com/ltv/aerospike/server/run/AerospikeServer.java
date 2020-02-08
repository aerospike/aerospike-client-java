/*
 * Copyright (c) 2019 LINE Corporation. All rights reserved.
 * LINE Corporation PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 */

package com.ltv.aerospike.server.run;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.TimeUnit;

import com.ltv.aerospike.server.filter.AuthorizationInterceptor;
import com.ltv.aerospike.server.service.CreateIndexService;
import com.ltv.aerospike.server.service.CreateNamespaceService;
import com.ltv.aerospike.server.service.CreateSequenceService;
import com.ltv.aerospike.server.service.CreateSetService;
import com.ltv.aerospike.server.service.CreateUserService;
import com.ltv.aerospike.server.service.DeleteService;
import com.ltv.aerospike.server.service.DeleteUserService;
import com.ltv.aerospike.server.service.DropBinService;
import com.ltv.aerospike.server.service.DropIndexService;
import com.ltv.aerospike.server.service.DropNamespaceService;
import com.ltv.aerospike.server.service.DropSequenceService;
import com.ltv.aerospike.server.service.DropSetService;
import com.ltv.aerospike.server.service.GetSequenceService;
import com.ltv.aerospike.server.service.GetService;
import com.ltv.aerospike.server.service.GrantNamespaceService;
import com.ltv.aerospike.server.service.GrantSetService;
import com.ltv.aerospike.server.service.LoginService;
import com.ltv.aerospike.server.service.LogoutService;
import com.ltv.aerospike.server.service.PutService;
import com.ltv.aerospike.server.service.QueryService;
import com.ltv.aerospike.server.service.RenameBinService;
import com.ltv.aerospike.server.service.RenameNamespaceService;
import com.ltv.aerospike.server.service.RenameSetService;
import com.ltv.aerospike.server.service.ShowBinService;
import com.ltv.aerospike.server.service.ShowIndexService;
import com.ltv.aerospike.server.service.ShowNamespaceService;
import com.ltv.aerospike.server.service.ShowSequenceService;
import com.ltv.aerospike.server.service.ShowSetService;
import com.ltv.aerospike.server.service.TruncateNamespaceService;
import com.ltv.aerospike.server.service.TruncateSetService;
import com.ltv.aerospike.server.service.UpdateUserService;

import io.grpc.Server;
import io.grpc.netty.GrpcSslContexts;
import io.grpc.netty.NettyServerBuilder;
import io.netty.handler.ssl.ClientAuth;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.handler.ssl.util.SelfSignedCertificate;

/**
 * Server that manages startup/shutdown of a {@code Greeter} server with TLS enabled.
 */
public class AerospikeServer {
    private static org.apache.log4j.Logger log = org.apache.log4j.Logger.getLogger(AerospikeServer.class.getSimpleName());

    public Server server;

    public int port;
    public String certChainFilePath;
    public String publicKeyFilePath;
    public String trustCertCollectionFilePath;

    public AerospikeServer(int port) {
        this.port = port;
        new AerospikeServer(port, null, null, null);
    }

    public AerospikeServer(int port,
                               String certChainFilePath,
                               String publicKeyFilePath,
                               String trustCertCollectionFilePath) {
        this.port = port;
        this.certChainFilePath = certChainFilePath;
        this.publicKeyFilePath = publicKeyFilePath;
        this.trustCertCollectionFilePath = trustCertCollectionFilePath;
    }

    public SslContextBuilder getSslContextBuilder() {
        SslContextBuilder sslClientContextBuilder = null;
        try {
            if (certChainFilePath == null) {
                SelfSignedCertificate ssc = new SelfSignedCertificate();
                sslClientContextBuilder = SslContextBuilder.forServer(ssc.certificate(),
                                                                      ssc.privateKey());
            } else {
                sslClientContextBuilder = SslContextBuilder.forServer(new File(certChainFilePath),
                                                                      new File(publicKeyFilePath));
            }

            if (trustCertCollectionFilePath != null) {
                sslClientContextBuilder.trustManager(new File(trustCertCollectionFilePath));
                sslClientContextBuilder.clientAuth(ClientAuth.REQUIRE);
            }
        } catch (Exception ex) {
            log.error("Create SSL certificate failed: ", ex);
        }
        return GrpcSslContexts.configure(sslClientContextBuilder);
    }

    public void start() throws IOException {
        server = NettyServerBuilder.forPort(port)
                                   .addService(new CreateIndexService())
                                   .addService(new CreateNamespaceService())
                                   .addService(new CreateSequenceService())
                                   .addService(new CreateSetService())
                                   .addService(new CreateUserService())
                                   .addService(new DeleteService())
                                   .addService(new DeleteUserService())
                                   .addService(new DropBinService())
                                   .addService(new DropIndexService())
                                   .addService(new DropNamespaceService())
                                   .addService(new DropSequenceService())
                                   .addService(new DropSetService())
                                   .addService(new GetSequenceService())
                                   .addService(new GrantNamespaceService())
                                   .addService(new GrantSetService())
                                   .addService(new LoginService())
                                   .addService(new LogoutService())
                                   .addService(new PutService())
                                   .addService(new RenameBinService())
                                   .addService(new RenameNamespaceService())
                                   .addService(new RenameSetService())
                                   .addService(new ShowBinService())
                                   .addService(new ShowIndexService())
                                   .addService(new ShowNamespaceService())
                                   .addService(new ShowSequenceService())
                                   .addService(new ShowSetService())
                                   .addService(new TruncateNamespaceService())
                                   .addService(new TruncateSetService())
                                   .addService(new UpdateUserService())
                                   .addService(new QueryService())
                                   .addService(new GetService())
                                   .intercept(new AuthorizationInterceptor())
                                   .sslContext(getSslContextBuilder().build())
                                   .keepAliveTime(10, TimeUnit.SECONDS)
                                   .keepAliveTimeout(20, TimeUnit.SECONDS)
                                   .permitKeepAliveWithoutCalls(true)
                                   .build()
                                   .start();
        log.info("Server started, listening on " + port);
        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                log.info("*** shutting down gRPC server since JVM is shutting down");
                AerospikeServer.this.stop();
                log.info("*** server shut down");
            }
        });
    }

    public void stop() {
        if (server != null) {
            server.shutdown();
        }
    }

    /**
     * Await termination on the main thread since the grpc library uses daemon threads.
     */
    public void blockUntilShutdown() throws InterruptedException {
        if (server != null) {
            server.awaitTermination();
        }
    }
}
