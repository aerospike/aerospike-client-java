/*
 * Aerospike Client - Java Library
 *
 * Copyright 2013 by Aerospike, Inc. All rights reserved.
 *
 * Availability of this source code to partners and customers includes
 * redistribution rights covered by individual contract. Please check your
 * contract for exact rights and responsibilities.
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
				String vs_sm = Integer.toString(v);
				String vs = "";
				while(vs.length() < size) {
					vs += vs_sm;
				}
				return Value.get(vs.substring(vs.length()-size));
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
