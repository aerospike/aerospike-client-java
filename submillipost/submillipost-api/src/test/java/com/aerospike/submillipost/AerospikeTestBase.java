package com.aerospike.submillipost;

import com.aerospike.client.IAerospikeClient;
import com.aerospike.submillipost.config.AppConfig;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.TestInstance;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;


@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@ActiveProfiles("test")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public abstract class AerospikeTestBase {

    private static final String USER_SET = "users";

    @Autowired
    private IAerospikeClient aerospikeClient;

    @Autowired
    private AppConfig appConfig;

    @BeforeAll
    void truncateUsersSet() {
        var ns = appConfig.getAerospike().getNamespace();
        aerospikeClient.truncate(null, ns, USER_SET, null);
    }
}
