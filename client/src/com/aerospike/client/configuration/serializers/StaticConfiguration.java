/*
 * Copyright (c) 2012-2025 Aerospike, Inc.
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

package com.aerospike.client.configuration.serializers;

import com.aerospike.client.Log;
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
        StringBuffer propsString = new StringBuffer();
        try {
            propsString.append(getStaticClientConfig());
        } catch (Exception e) {
            Log.error(e.toString());
        }
        return propsString.toString();
    }
}
