package com.ltv.aerospike.client;

import com.ltv.aerospike.api.proto.QueryServices.QueryRequest.FilterOperation;

public class Qualifier {
    private String field;
    private FilterOperation filter;
    private Object value1;
    private Object value2;

    public Qualifier(String field, FilterOperation filter, Object value1) {
        this.field = field;
        this.filter = filter;
        this.value1 = value1;
    }

    public Qualifier(String field, FilterOperation filter, Object value1, Object value2) {
        this(field, filter, value1);
        this.value2 = value2;
    }

    public String getField() {
        return field;
    }

    public void setField(String field) {
        this.field = field;
    }

    public FilterOperation getFilter() {
        return filter;
    }

    public void setFilter(FilterOperation filter) {
        this.filter = filter;
    }

    public Object getValue1() {
        return value1;
    }

    public void setValue1(Object value1) {
        this.value1 = value1;
    }

    public Object getValue2() {
        return value2;
    }

    public void setValue2(Object value2) {
        this.value2 = value2;
    }
}
