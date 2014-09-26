/* 
 * Copyright 2012-2014 Aerospike, Inc.
 *
 * Portions may be licensed to Aerospike, Inc. under one or more contributor
 * license agreements.
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
package com.aerospike.benchmarks;

import com.aerospike.client.policy.Policy;
import com.aerospike.client.policy.WritePolicy;

public class Arguments {
	public String namespace;
	public String setName;
	public Workload workload;
	public DBObjectSpec[] objectSpec;
	public Policy readPolicy = new Policy();
	public WritePolicy writePolicy = new WritePolicy();
	public int nBins;
	public int readPct;
	public int readMultiBinPct;
	public int writeMultiBinPct;
	public int throughput;
	public boolean reportNotFound;
	public boolean validate;
	public boolean debug;
	public KeyType keyType;
}
