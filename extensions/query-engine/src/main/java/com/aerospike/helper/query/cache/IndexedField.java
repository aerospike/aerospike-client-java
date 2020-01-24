package com.aerospike.helper.query.cache;

import java.util.Objects;

public class IndexedField {

    private final String namespace;
    private final String set;
    private final String field;

    public IndexedField(String namespace, String set, String field) {
        this.namespace = namespace;
        this.set = set;
        this.field = field;
    }

    public String getNamespace() {
        return namespace;
    }

    public String getSet() {
        return set;
    }

    public String getField() {
        return field;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        IndexedField that = (IndexedField) o;
        return Objects.equals(namespace, that.namespace) &&
                Objects.equals(set, that.set) &&
                Objects.equals(field, that.field);
    }

    @Override
    public int hashCode() {
        return Objects.hash(namespace, set, field);
    }
}
