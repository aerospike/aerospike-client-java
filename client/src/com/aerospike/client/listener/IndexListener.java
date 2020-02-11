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
package com.aerospike.client.listener;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.async.AsyncIndexTask;

/**
 * Asynchronous result notifications for create/drop index commands.
 */
public interface IndexListener {
    /**
     * This method is called when an asynchronous command completes successfully.
     *
     * @param indexTask			task monitor that can be used to query for index command completion.
     */
    void onSuccess(AsyncIndexTask indexTask);

    /**
     * This method is called when an asynchronous command fails.
     *
     * @param exception			error that occurred
     */
    public void onFailure(AerospikeException exception);
}
