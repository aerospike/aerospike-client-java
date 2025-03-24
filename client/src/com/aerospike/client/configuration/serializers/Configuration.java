package com.aerospike.client.configuration.serializers;

public class Configuration {
    public Metadata metadata;
    public StaticConfiguration staticConfiguration;
    public DynamicConfiguration dynamicConfiguration;

    public Configuration() {}

    public Metadata getMetadata() {
        return this.metadata;
    }

    public void setMetadata(Metadata metadata) {
        this.metadata = metadata;
    }

    public StaticConfiguration getStaticConfiguration() {
        return this.staticConfiguration;
    }

    public void setStaticConfiguration(StaticConfiguration staticConfiguration) {
        this.staticConfiguration = staticConfiguration;
    }

    public DynamicConfiguration getDynamicConfiguration() {
        return this.dynamicConfiguration;
    }

    public void setDynamicConfiguration(DynamicConfiguration dynamicConfiguration) {
        this.dynamicConfiguration = dynamicConfiguration;
    }


    @Override
    public String toString() {
        return "\n{" +
            "\n\tmetadata= " + getMetadata() +
            "\n\tstatic= " + getStaticConfiguration() +
            "\n\tdynamic= " + getDynamicConfiguration() +
            "\n}\n";
    }
}
