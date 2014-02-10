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

import java.util.Map;

import com.aerospike.client.Bin;
import com.aerospike.client.Record;

public final class ExpectedValue {
	Bin[] bins;
	int generation;
	
	public ExpectedValue(Bin[] bins, int generation) {
		this.bins = bins;
		this.generation = generation;
	}
	
	public void write(Bin[] bins) {
		this.bins = bins;
		this.generation++;
	}
	
	public void add(Bin[] addBins, int incrValue) {
		int orig = 0;
		
		if (this.bins != null) {
			Bin bin = this.bins[0];
			
			if (bin != null) {
				Object object = bin.value.getObject();
				
				if (object instanceof Integer) {
					orig = (Integer)object;
				}
			}
		}
		
		if (orig != 0) {
			this.bins[0] = new Bin(addBins[0].name, orig + incrValue);
		}
		else {
			this.bins = addBins;
		}
		this.generation++;
	}
	
	public boolean validate(Record record) {
		if (bins == null) {
			if (record == null) {
				return true;
			}
			System.out.println("Mismatch: Expected null. Received not null.");			
			return false;
		}
		
		if (record == null || record.bins == null) {			
			System.out.println("Mismatch: Expected not null. Received null.");			
			return false;
		}
		Map<String,Object> map = record.bins;
		int max = bins.length;
		
		for (int i = 0; i < max; i++) {
			Object expected = bins[i].value.getObject();
			Object received = map.get(Integer.toString(i));
			
			if (! expected.equals(received)) {
				System.out.println("Mismatch: Expected '" + expected + "' Received '" + received + "'");
				return false;
			}
		}
		return true;
	}
}
