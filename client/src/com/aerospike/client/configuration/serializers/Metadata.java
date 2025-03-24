package com.aerospike.client.configuration.serializers;

public class Metadata {
    public String appName;
    public String schemaVersion;
    public int generation;


    public String getAppName() {
        return this.appName;
    }

    public void setAppName(String appName) {
        this.appName = appName;
    }

    public int getGeneration() {
        return this.generation;
    }

    public void setGeneration(int generation) {
        this.generation = generation;
    }

    public String getSchemaVersion() {
        return this.schemaVersion;
    }

    public void setSchemaVersion(String schemaVersion) {
        this.schemaVersion = schemaVersion;
    }


    @Override
    public String toString() {
        return "{" +
            " version='" + getSchemaVersion() + "'" +
            ", appName='" + getAppName() + "'" +
            ", generation='" + getGeneration() + "'" +
            "}";
    }

}
