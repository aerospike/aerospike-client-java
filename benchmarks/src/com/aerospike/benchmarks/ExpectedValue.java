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

import java.util.Map;

import com.aerospike.client.Bin;
import com.aerospike.client.Record;

public final class ExpectedValue {
	Bin[] bins;
	int generation;

	public ExpectedValue(int generation) {
		this.generation = generation;
	}

	public void add(Bin[] addBins, int incrValue) {
		int orig = 0;

		Bin[] bins = read();

		if (bins != null) {
			Bin bin = bins[0];

			if (bin != null) {
				Object object = bin.value.getObject();

				if (object instanceof Integer) {
					orig = (Integer)object;
				}
			}
		}

		if (orig != 0) {
			bins[0] = new Bin(addBins[0].name, orig + incrValue);
			write(bins);
		}
		else {
			write(addBins);
		}
	}

	public boolean validate(Record record, Bin[] bins) {
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
	
	private Bin[] read() {
		// TODO: read from Aerospike
		return bins;
	}
	
	public void write(Bin[] bins) {
		this.bins = bins;
		this.generation++;
	}
}
