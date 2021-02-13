/*
 * Copyright 2012-2021 Aerospike, Inc.
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
package com.aerospike.benchmarks;

import com.aerospike.client.Bin;
import com.aerospike.client.Value;
import com.aerospike.client.policy.BatchPolicy;
import com.aerospike.client.policy.Policy;
import com.aerospike.client.policy.WritePolicy;
import com.aerospike.client.util.RandomShift;

public class Arguments {
	public String namespace;
	public String[] batchNamespaces;
	public String setName;
	public Workload workload;
	public DBObjectSpec[] objectSpec;
	public Policy readPolicy;
	public WritePolicy writePolicy;
	public WritePolicy updatePolicy;
	public WritePolicy replacePolicy;
	public BatchPolicy batchPolicy;
	public int batchSize;
	public int nBins;
	public int readPct;
	public int readMultiBinPct;
	public int writeMultiBinPct;
	public int throughput;
	public long transactionLimit;
	public boolean reportNotFound;
	public boolean debug;
	public TransactionalWorkload transactionalWorkload;
	public KeyType keyType;
	public Bin[] fixedBins;
	public Bin[] fixedBin;
	public String udfPackageName;
	public String udfFunctionName;
	public Value[] udfValues;

	public void setFixedBins() {
		// Fixed values are used when the extra random call overhead is not wanted
		// in the benchmark measurement.
		RandomShift random = new RandomShift();
		fixedBins = getBins(random, true, -1);
		fixedBin = new Bin[] {fixedBins[0]};
	}

	public Bin[] getBins(RandomShift random, boolean multiBin, long keySeed) {
		if (fixedBins != null) {
			return (multiBin)? fixedBins : fixedBin;
		}

		int binCount = (multiBin)? nBins : 1;
		Bin[] bins = new Bin[binCount];
		int specLength = objectSpec.length;

		for (int i = 0; i < binCount; i++) {
			String name = Integer.toString(i);
			// Use passed in value for 0th bin. Random for others.
			Value value = genValue(random, objectSpec[i % specLength],
							i == 0 ? keySeed : -1);
			bins[i] = new Bin(name, value);
		}
		return bins;
	}

	private static Value genValue(RandomShift random, DBObjectSpec spec, long keySeed) {
		switch (spec.type) {
		case 'I':
			if (keySeed == -1) {
				return Value.get(random.nextInt());
			} else {
				return Value.get(keySeed);
			}

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

		case 'R':
			spec.size = Math.abs(spec.size / 8);
			// ... relies on size being a multiple of 8, which it will be.
			spec.rand_pct = Math.abs(spec.rand_pct);
			long[] data = new long[spec.size];
			int idx = 0;
			int rand_pct = spec.rand_pct;
			if (rand_pct < 100) {
				int n_zeros = (spec.size * (100 - rand_pct)) / 100;
				int n_rands = spec.size - n_zeros;
				for (int z = n_zeros; z != 0; z--) {
					data[idx++] = 0;
				}
				for (int r = n_rands; r != 0; r--) {
					data[idx++] = random.nextLong();
				}
			}
			while (idx < spec.size) {
				data[idx++] = random.nextLong();
			}
			return Value.get(data);

		case 'D':
			return Value.get(System.currentTimeMillis());

		default:
			return Value.getAsNull();
		}
	}
}
