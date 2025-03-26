package com.aerospike.client.configuration;

import com.aerospike.client.configuration.serializers.Configuration;

public interface ConfigurationProvider {
    void loadConfiguration();

    Configuration fetchConfiguration();
    Configuration fetchDynamicConfiguration();
}
