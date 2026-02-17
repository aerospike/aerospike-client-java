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

package com.aerospike.client.configuration.serializers.dynamicconfig;

import com.aerospike.client.configuration.primitiveprops.BooleanProperty;
import com.aerospike.client.Log;

public class DynamicBatchUDFconfig {
    public BooleanProperty sendKey;
    public BooleanProperty durableDelete;

    public DynamicBatchUDFconfig() {}

    public void setDurableDelete(BooleanProperty durableDelete) { this.durableDelete = durableDelete; }

    public void setSendKey(BooleanProperty sendKey) { this.sendKey = sendKey; }

    public BooleanProperty getSendKey() { return sendKey; }

    public BooleanProperty getDurableDelete() { return durableDelete; }

    @Override
    public String toString() {
        StringBuffer propsString = new StringBuffer("{");
        try {
            propsString.append(" send_key=").append(sendKey.value).append(", ");
            propsString.append(" durable_delete=").append(durableDelete.value).append(", ");
        } catch (Exception e) {
            Log.error(e.toString());
        }
        return propsString.append("}").toString();
    }
}
