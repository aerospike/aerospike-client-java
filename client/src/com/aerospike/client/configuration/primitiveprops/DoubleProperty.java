package com.aerospike.client.configuration.primitiveprops;

public class DoubleProperty {
    public double value;

    public DoubleProperty() {}

    public DoubleProperty(double value) {
        this.value = value;
    }

    public void setValue(double value) {
        this.value = value;
    }
}
