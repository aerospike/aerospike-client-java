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

import java.util.Random;

import com.aerospike.client.Bin;
import com.aerospike.client.Value;
import com.aerospike.client.policy.BatchPolicy;
import com.aerospike.client.policy.Policy;
import com.aerospike.client.policy.WritePolicy;

public class Arguments {
	public String namespace;
	public String setName;
	public Workload workload;
	public StorageType storeType;
	public DBObjectSpec[] objectSpec;
	public Policy readPolicy;
	public WritePolicy writePolicy;
	public BatchPolicy batchPolicy;
	public int batchSize;
	public int nBins;
	public int readPct;
	public int readMultiBinPct;
	public int writeMultiBinPct;
	public int throughput;
	public boolean reportNotFound;
	public boolean validate;
	public boolean debug;
	public KeyType keyType;
	public Bin[] fixedBins;
	public Bin[] fixedBin;

	public void setFixedBins() {
		// Fixed values are used when the extra random call overhead is not wanted
		// in the benchmark measurement.
		Random random = new Random();
		fixedBins = getBins(random, true);
		fixedBin = new Bin[] {fixedBins[0]};
	}

	public Bin[] getBins(Random random, boolean multiBin) {
		if (fixedBins != null) {
		    return (multiBin)? fixedBins : fixedBin;
		}
		
		int binCount = (multiBin)? nBins : 1;	
		Bin[] bins = new Bin[binCount];
		int specLength = objectSpec.length;
		
		for (int i = 0; i < binCount; i++) {
			String name = Integer.toString(i);
			Value value = genValue(random, objectSpec[i % specLength]);
			bins[i] = new Bin(name, value);
		}
		return bins;
	}
    
	private static Value genValue(Random random, DBObjectSpec spec) {
		switch (spec.type) {
		case 'I':
			return Value.get(random.nextInt());
			
		case 'B':
			byte[] ba = new byte[spec.size];
			random.nextBytes(ba);
			return Value.get(ba);
				
		case 'S':
			StringBuilder sb = new StringBuilder(spec.size);
            for (int i = 0; i < spec.size; i++) {
            	// Append ascii value between ordinal 33 and 127.
                sb.append((char)(random.nextInt(94) + 33));
            }
			return Value.get(sb.toString());
			
		case 'D':
			return Value.get(System.currentTimeMillis());
			
		default:
			return Value.getAsNull();
		}
	}	
}
