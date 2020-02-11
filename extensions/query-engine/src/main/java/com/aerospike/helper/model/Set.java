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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * this class represents a Set defined in the cluster
 *
 * @author peter
 */
public class Set {
    private Namespace parent;
    private String name;
    protected Map<String, NameValuePair> values;

    public Set(Namespace parent, String info) {
        this.parent = parent;
        setInfo(info);
    }

    public Object getParent() {
        return this.parent;
    }

    @Override
    public String toString() {
        return this.name;
    }

    @Override
    public boolean equals(Object obj) {
        return ((obj instanceof Set) &&
                (obj.toString().equals(toString())));
    }

    public String getName() {
        return toString();
    }

    public void setInfo(String info) {
        //ns_name=test:set_name=demo:n_objects=1:set-stop-write-count=0:set-evict-hwm-count=0:set-enable-xdr=use-default:set-delete=false
        if (!info.isEmpty()) {
            String[] parts = info.split(":");
            if (values == null) {
                values = new HashMap<String, NameValuePair>();
            }

            for (String part : parts) {
                String[] kv = part.split("=");
                String key = kv[0];
                String value = kv[1];
                NameValuePair storedValue = values.get(key);
                if (storedValue == null) {
                    storedValue = new NameValuePair(key, value);
                    values.put(key, storedValue);
                } else {
                    storedValue.value = value;
                }
            }
            applySetName();
        }
    }

    public void mergeSetInfo(String info) {
	//ns=test:set=selector:objects=1000:memory_data_bytes=0:deleting=false:stop-writes-count=0:set-enable-xdr=use-default:disable-eviction=false
        if (!info.isEmpty()) {
            String[] parts = info.split(":");
            if (values == null) {
                values = new HashMap<String, NameValuePair>();
            }

            for (String part : parts) {
                String[] kv = part.split("=");
                String key = kv[0];
                String value = kv[1];
                NameValuePair storedValue = values.get(key);
                if (storedValue == null) {
                    storedValue = new NameValuePair(key, value);
                    values.put(key, storedValue);
                } else {
                    try {
                        Long newValue = Long.parseLong(value);
                        Long oldValue = Long.parseLong(storedValue.value.toString());
                        storedValue.value = Long.toString(oldValue + newValue);
                    } catch (NumberFormatException e) {
                        storedValue.value = value;
                    }
                }
            }
            applySetName();
        }
    }

    public void setValues(Map<String, NameValuePair> newValues) {
        this.values = newValues;
    }

    public List<NameValuePair> getValues() {
        List<NameValuePair> result = new ArrayList<NameValuePair>();
        java.util.Set<String> keys = this.values.keySet();
        for (String key : keys) {
            NameValuePair nvp = this.values.get(key);
            result.add(nvp);
        }
        return result;
    }

    public void clear() {
        java.util.Set<String> keys = this.values.keySet();
        for (String key : keys) {
            NameValuePair nvp = this.values.get(key);
            nvp.clear();
        }
    }

    private void applySetName(){
        this.name = values.get("set") == null ? (String)values.get("set_name").value : (String)values.get("set").value;
    }
}
