package com.aerospike.submillipost.config;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.IAerospikeClient;
import com.aerospike.client.policy.ClientPolicy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class AerospikeConfig {

    private static final Logger log = LoggerFactory.getLogger(AerospikeConfig.class);

    @Bean(destroyMethod = "close")
    public IAerospikeClient aerospikeClient(AppConfig appConfig) {
        var policy = new ClientPolicy();
        policy.timeout = 10_000;
        policy.loginTimeout = 10_000;
        policy.writePolicyDefault.sendKey = true;
        policy.readPolicyDefault.sendKey = true;

        var db = appConfig.getAerospike();
        policy.useServicesAlternate = db.isUseServicesAlternate();

        var client = new AerospikeClient(policy, db.getHost(), db.getPort());
        log.info("Connected to Aerospike at {}:{} (useServicesAlternate={})",
                db.getHost(), db.getPort(), policy.useServicesAlternate);
        return client;
    }
}
