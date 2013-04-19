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
