/*
 * Copyright (c) 2019 LINE Corporation. All rights reserved.
 * LINE Corporation PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 */

package com.ltv.aerospike.server.repository;

import org.apache.log4j.Logger;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.AerospikeException;
import com.aerospike.client.Host;
import com.aerospike.client.Key;
import com.aerospike.client.async.EventLoops;
import com.aerospike.client.async.EventPolicy;
import com.aerospike.client.async.NettyEventLoops;
import com.aerospike.client.listener.DeleteListener;
import com.aerospike.client.listener.WriteListener;
import com.aerospike.client.policy.ClientPolicy;
import com.aerospike.helper.query.QueryEngine;

import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;

public class Connector {
    private static Logger log = Logger.getLogger(Connector.class.getSimpleName());
    public AerospikeClient aeClient;
    public String AEROSPIKE_NAMESPACE;
    public EventLoops eventLoops;
    public QueryEngine queryEngine;
    public static WriteListener defaultWriteListener;
    public static DeleteListener defaultDeleteListener;

    public Connector() {};
    public Connector(String host, Integer port, String namespace, Integer maxCommandsInProcess, Integer maxCommandsInQueue) {

        AEROSPIKE_NAMESPACE = namespace;

        EventPolicy eventPolicy = new EventPolicy();
        eventPolicy.maxCommandsInProcess = maxCommandsInProcess;
        eventPolicy.maxCommandsInQueue = maxCommandsInQueue;
        EventLoopGroup group = new NioEventLoopGroup();
        eventLoops = new NettyEventLoops(eventPolicy, group);

        ClientPolicy policy = new ClientPolicy();
        policy.maxConnsPerNode = 20000;
        policy.eventLoops = eventLoops;
        Host[] hosts = Host.parseHosts(host, port);
        aeClient = new AerospikeClient(policy, hosts);

        queryEngine = new QueryEngine(aeClient);

        defaultDeleteListener = new DeleteListener() {
            @Override
            public void onSuccess(Key key, boolean existed) {
            }

            @Override
            public void onFailure(AerospikeException exception) {
                log.info("Failed to delete ", exception);
            }
        };

        defaultWriteListener = new WriteListener() {
            @Override
            public void onSuccess(Key key) {
            }

            @Override
            public void onFailure(AerospikeException exception) {
                log.info("Failed to update ", exception);
            }
        };
    };

}

