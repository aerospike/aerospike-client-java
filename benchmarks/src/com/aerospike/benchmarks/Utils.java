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

import java.util.Date;
import java.util.Random;

import com.aerospike.client.Bin;
import com.aerospike.client.Value;

public class Utils {
	protected static Bin[] genBins(Random r, int binSize, DBObjectSpec[] spec, int generation) {
		Bin[] bins = new Bin[binSize];
		for(int i=0; i<binSize; i++) {
			String name = Integer.toString(i);
			Value value = genValue(r, spec[i%spec.length].type, spec[i%spec.length].size, generation);
			bins[i] = new Bin(name, value);
		}
		return bins;
	}

   protected static Value genValue(Random r, char type, int size, int generation) {
		if(type == 'B') {
			byte[] ba = new byte[size];
			r.nextBytes(ba);
			return Value.get(ba);
		} else if(type == 'D') {
			return Value.get(Integer.toString((int) (new Date().getTime()%86400000))+","+Integer.toString(generation));
		} else {
			int v = r.nextInt();
			v = v < 0 ? (-v) : v;
			if(type == 'I') {
				return Value.get(v);
			} else if(type == 'S') {
				StringBuilder builder = new StringBuilder();
				while(builder.length() < size) {
					builder.append(Integer.toString(v));
				}
				return Value.get(builder.toString());
			}
		}
		return Value.getAsNull();
	}
	
	protected static String genKey(int i, int keyLen) {
		String key = "";
		for(int j=keyLen-1; j>=0; j--) {
			key = (i % 10) + key;
			i /= 10;
		}
		return key;
	}	
}
