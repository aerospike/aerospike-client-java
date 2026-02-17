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

import java.util.List;

import com.aerospike.client.configuration.primitiveprops.BooleanProperty;
import com.aerospike.client.configuration.primitiveprops.IntProperty;
import com.aerospike.client.configuration.primitiveprops.StringProperty;
import com.aerospike.client.Log;

public class DynamicClientConfig {
    public StringProperty appId;
    public IntProperty timeout;
    public IntProperty errorRateWindow;
    public IntProperty maxErrorRate;
    public BooleanProperty failIfNotConnected;
    public IntProperty loginTimeout;
    public IntProperty maxSocketIdle;
    public BooleanProperty rackAware;
    public List<Integer> rackIds;
    public IntProperty tendInterval;
    public BooleanProperty useServiceAlternative;

    public DynamicClientConfig() {}

    public void setAppId(StringProperty appId) { this.appId = appId; }

    public void setTimeout(IntProperty timeout) { this.timeout = timeout; }

    public void setErrorRateWindow(IntProperty errorRateWindow) { this.errorRateWindow = errorRateWindow; }

    public void setMaxErrorRate(IntProperty maxErrorRate) { this.maxErrorRate = maxErrorRate; }

    public void setFailIfNotConnected(BooleanProperty failIfNotConnected) { this.failIfNotConnected = failIfNotConnected; }

    public void setLoginTimeout(IntProperty loginTimeout) { this.loginTimeout = loginTimeout; }

    public void setMaxSocketIdle(IntProperty maxSocketIdle) { this.maxSocketIdle = maxSocketIdle; }

    public void setRackAware(BooleanProperty rackAware) { this.rackAware = rackAware; }

    public void setRackIds(List<Integer> rackIds) { this.rackIds = rackIds; }

    public void setTendInterval(IntProperty tendInterval) { this.tendInterval = tendInterval; }

    public void setUseServiceAlternative(BooleanProperty useServiceAlternative) { this.useServiceAlternative = useServiceAlternative; }

    public StringProperty getAppId() { return appId; }

    public List<Integer> getRackIds() { return rackIds; }

    public IntProperty getTimeout() { return timeout; }

    public IntProperty getErrorRateWindow() { return errorRateWindow; }

    public IntProperty getMaxErrorRate() { return maxErrorRate; }

    public BooleanProperty getFailIfNotConnected() { return failIfNotConnected; }

    public IntProperty getLoginTimeout() { return loginTimeout; }

    public IntProperty getMaxSocketIdle() { return maxSocketIdle; }

    public BooleanProperty getRackAware() { return rackAware; }

    public IntProperty getTendInterval() { return tendInterval; }

    public BooleanProperty getUseServiceAlternative() { return useServiceAlternative; }

    @Override
    public String toString() {
        StringBuffer propsString = new StringBuffer("{");
        try {
            propsString.append(" app_id=").append(appId.value).append(", ");
            propsString.append(" timeout=").append(timeout.value).append(", ");
            propsString.append(" error_rate_window=").append(errorRateWindow.value).append(", ");
            propsString.append(" max_error_rate=").append(maxErrorRate.value).append(", ");
            propsString.append(" fail_if_not_connected=").append(failIfNotConnected.value).append(", ");
            propsString.append(" login_timeout=").append(loginTimeout.value).append(", ");
            propsString.append(" max_socket_idle=").append(maxSocketIdle.value).append(", ");
            propsString.append(" rack_aware=").append(rackAware.value).append(", ");
            propsString.append(" rack_ids=").append(getRackIds().toString()).append(", ");
            propsString.append(" tend_interval=").append(tendInterval.value).append(", ");
            propsString.append(" use_service_alternative=").append(useServiceAlternative.value).append(", ");
        } catch (Exception e) {
            Log.error(e.toString());
        }
        return propsString.append("}").toString();
    }
}
