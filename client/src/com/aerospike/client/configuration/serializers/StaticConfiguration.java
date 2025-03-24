package com.aerospike.client.configuration.serializers;

import com.aerospike.client.configuration.serializers.staticconfig.*;

public class StaticConfiguration {
    public StaticClientConfig staticClientConfig;

    public StaticConfiguration() {}

    public StaticClientConfig getStaticClientConfig() {
        return this.staticClientConfig;
    }

    public void setStaticClientConfig(StaticClientConfig staticClientConfig) {
        this.staticClientConfig = staticClientConfig;
    }

    @Override
    public String toString() {
        return "{" +
            " \n\t\t"+ getStaticClientConfig() +
            "\n\t}";
    }

}
