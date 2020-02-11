/*
 * Copyright 2012-2020 Aerospike, Inc.
 *
 * Portions may be licensed to Aerospike, Inc. under one or more contributor
 * license agreements WHICH ARE COMPATIBLE WITH THE APACHE LICENSE, VERSION 2.0.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package com.aerospike.helper.model;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import com.aerospike.client.query.IndexType;

/**
 * This class represents a Secondary Index
 * created in the cluster.
 *
 * @author Peter Milne
 */
public class Index {

    private final Map<String, String> values;
    private final String name;
    private final String namespace;
    private final String set;
    private final String bin;
    private final IndexType indexType;

    public Index(Map<String, String> values, String name,
                 String namespace, String set, String bin, IndexType indexType) {
        this.values = values;
        this.name = name;
        this.namespace = namespace;
        this.set = set;
        this.bin = bin;
        this.indexType = indexType;
    }

    public List<NameValuePair> getValues() {
        return this.values.keySet().stream()
                .map(key -> new NameValuePair(key, this.values.get(key)))
                .collect(Collectors.collectingAndThen(
                        Collectors.toList(),
                        Collections::unmodifiableList));
    }

    @Override
    public String toString() {
        return this.getName();
    }

    public String getName() {
        return this.name;
    }

    public String getBin() {
        return this.bin;
    }

    public String getSet() {
        return this.set;
    }

    public String getNamespace() {
        return this.namespace;
    }

    public IndexType getType() {
        return this.indexType;
    }
}
