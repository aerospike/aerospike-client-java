/*
 * Copyright (c) 2019 LINE Corporation. All rights reserved.
 * LINE Corporation PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 */

/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.ltv.aerospike.server.run;

import org.apache.log4j.Level;

import com.ltv.aerospike.server.repository.Connector;
import com.ltv.core.common.util.ConfigUtils;

/**
 *
 * @author HienDM
 */
public class StartApp {
    private static org.apache.log4j.Logger log = org.apache.log4j.Logger.getLogger(StartApp.class.getSimpleName());
    public static ConfigUtils config;
    public static Connector aeConnector;
    
    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) throws Exception {
        start("etc");
    }
    
    public static void start(String configPath) throws Exception {
        config = new ConfigUtils(configPath);
        
        String logLevel = config.getConfig("log-level");
        if (logLevel.isEmpty()) {
            logLevel = "ERROR";
        }
        org.apache.log4j.Logger.getRootLogger().setLevel(Level.toLevel(logLevel));

        log.info("Start setup ...");
        (new StartSetup()).onStart();

        log.info("Start Aerospike server ...");
        Integer port = Integer.parseInt(config.getConfig("port"));
        AerospikeServer server = new AerospikeServer(port);
        server.start();
        log.info("Aerospike server is listening on port: " + port);
        server.blockUntilShutdown();
    }
}
