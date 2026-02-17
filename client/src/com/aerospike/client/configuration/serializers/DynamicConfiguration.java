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

import com.aerospike.client.configuration.serializers.dynamicconfig.*;

public class DynamicConfiguration {
    public DynamicClientConfig dynamicClientConfig;
    public DynamicReadConfig dynamicReadConfig;
    public DynamicWriteConfig dynamicWriteConfig;
    public DynamicQueryConfig dynamicQueryConfig;
    public DynamicScanConfig dynamicScanConfig;
    public DynamicBatchReadConfig dynamicBatchReadConfig;
    public DynamicBatchWriteConfig dynamicBatchWriteConfig;
    public DynamicBatchUDFconfig dynamicBatchUDFconfig;
    public DynamicBatchDeleteConfig dynamicBatchDeleteConfig;
    public DynamicTxnRollConfig dynamicTxnRollConfig;
    public DynamicTxnVerifyConfig dynamicTxnVerifyConfig;
    public DynamicMetricsConfig dynamicMetricsConfig;

    public DynamicConfiguration() {}

    public DynamicClientConfig getDynamicClientConfig() { return this.dynamicClientConfig; }

    public DynamicReadConfig getDynamicReadConfig() { return dynamicReadConfig; }

    public DynamicWriteConfig getDynamicWriteConfig() { return dynamicWriteConfig; }

    public DynamicQueryConfig getDynamicQueryConfig() { return dynamicQueryConfig; }

    public DynamicScanConfig getDynamicScanConfig() { return dynamicScanConfig; }

    public DynamicBatchReadConfig getDynamicBatchReadConfig() { return dynamicBatchReadConfig; }

    public DynamicBatchWriteConfig getDynamicBatchWriteConfig() { return dynamicBatchWriteConfig; }

    public DynamicBatchUDFconfig getDynamicBatchUDFconfig() { return dynamicBatchUDFconfig; }

    public DynamicBatchDeleteConfig getDynamicBatchDeleteConfig() { return dynamicBatchDeleteConfig; }

    public DynamicTxnRollConfig getDynamicTxnRollConfig() { return dynamicTxnRollConfig; }

    public DynamicTxnVerifyConfig getDynamicTxnVerifyConfig() { return dynamicTxnVerifyConfig; }

    public DynamicMetricsConfig getDynamicMetricsConfig() { return dynamicMetricsConfig; }

    public void setDynamicReadConfig(DynamicReadConfig dynamicReadConfig) { this.dynamicReadConfig = dynamicReadConfig; }

    public void setDynamicWriteConfig(DynamicWriteConfig dynamicWriteConfig) { this.dynamicWriteConfig = dynamicWriteConfig; }

    public void setDynamicClientConfig(DynamicClientConfig dynamicClientConfig) { this.dynamicClientConfig = dynamicClientConfig; }

    public void setDynamicQueryConfig(DynamicQueryConfig dynamicQueryConfig) { this.dynamicQueryConfig = dynamicQueryConfig; }

    public void setDynamicScanConfig(DynamicScanConfig dynamicScanConfig) { this.dynamicScanConfig = dynamicScanConfig; }

    public void setDynamicBatchReadConfig(DynamicBatchReadConfig dynamicBatchReadConfig) { this.dynamicBatchReadConfig = dynamicBatchReadConfig; }

    public void setDynamicBatchWriteConfig(DynamicBatchWriteConfig dynamicBatchWriteConfig) { this.dynamicBatchWriteConfig = dynamicBatchWriteConfig; }

    public void setDynamicBatchUDFconfig(DynamicBatchUDFconfig dynamicBatchUDFconfig) { this.dynamicBatchUDFconfig = dynamicBatchUDFconfig; }

    public void setDynamicBatchDeleteConfig(DynamicBatchDeleteConfig dynamicBatchDeleteConfig) { this.dynamicBatchDeleteConfig = dynamicBatchDeleteConfig; }

    public void setDynamicTxnRollConfig(DynamicTxnRollConfig dynamicTxnRollConfig) { this.dynamicTxnRollConfig = dynamicTxnRollConfig; }

    public void setDynamicTxnVerifyConfig(DynamicTxnVerifyConfig dynamicTxnVerifyConfig) { this.dynamicTxnVerifyConfig = dynamicTxnVerifyConfig; }

    public void setDynamicMetricsConfig(DynamicMetricsConfig dynamicMetricsConfig) { this.dynamicMetricsConfig = dynamicMetricsConfig; }

    @Override
    public String toString() {
        return "{" +
            "\n\t\tclient= " + getDynamicClientConfig() + "," +
            "\n\t\tread= " + getDynamicReadConfig() + "," +
            "\n\t\twrite= " + getDynamicWriteConfig() + "," +
            "\n\t\tquery= " + getDynamicQueryConfig() + "," +
            "\n\t\tscan= " + getDynamicScanConfig() + "," +
            "\n\t\tbatch_read= " + getDynamicBatchReadConfig() + "," +
            "\n\t\tbatch_write= " + getDynamicBatchWriteConfig() + "," +
            "\n\t\tbatch_udf= " + getDynamicBatchUDFconfig() + "," +
            "\n\t\tbatch_delete= " + getDynamicBatchDeleteConfig() + "," +
            "\n\t\ttxn_roll= " + getDynamicTxnRollConfig() + "," +
            "\n\t\ttxn_verify= " + getDynamicTxnVerifyConfig() + "," +
            "\n\t\tmetrics= " + getDynamicMetricsConfig() + "," +
            "\n\t}";
    }
}
