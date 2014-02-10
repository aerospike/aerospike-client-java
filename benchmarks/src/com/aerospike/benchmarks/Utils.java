/*******************************************************************************
 * Copyright 2012-2014 by Aerospike.
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to
 * deal in the Software without restriction, including without limitation the
 * rights to use, copy, modify, merge, publish, distribute, sublicense, and/or
 * sell copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
 * FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS
 * IN THE SOFTWARE.
 ******************************************************************************/
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
