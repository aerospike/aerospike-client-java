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
package com.aerospike.benchmarks;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import com.aerospike.client.util.RandomShift;

/**
 * Create a transactional workload with a fixed set of transactions. This could be for example
 * 3 reads, 2 updates and 1 replace.
 * @author Tim
 *
 */
public class TransactionalWorkload implements Iterable<TransactionalItem>{
	private enum VariationType {
		PLUS,
		MINUS
	};

	// These options are derived and should not be set
	private int minReads;
	private int maxReads;
	private int minWrites;
	private int maxWrites;
	private TransactionalItem[] items;
	private static final TransactionalItem MULTI_BIN_WRITE = new TransactionalItem(TransactionalType.MULTI_BIN_WRITE);
	private static final TransactionalItem MULTI_BIN_READ = new TransactionalItem(TransactionalType.MULTI_BIN_READ);
	private static final TransactionalItem SINGLE_BIN_UPDATE = new TransactionalItem(TransactionalType.SINGLE_BIN_UPDATE);
	private static final TransactionalItem MULTI_BIN_UPDATE = new TransactionalItem(TransactionalType.MULTI_BIN_UPDATE);

	public TransactionalWorkload(String[] formatStrings) throws Exception {
		if (formatStrings == null || formatStrings.length == 0) {
			throw new Exception("invalid empty transactional workload string");
		}
		parseFormatStrings(formatStrings);
	}


	private void parseFormatStrings(String[] formatStrings) throws Exception {
		int reads = 0;
		int writes = 0;
		String variance = "";

		for (int i = 1; i < formatStrings.length; i++) {
			String thisOption = formatStrings[i];
			if (thisOption.length() < 3 || thisOption.charAt(1) != ':') {
				throw new Exception("Invalid transaction workload argument: " + thisOption);
			}

			String thisOptionValue = thisOption.substring(2);
			switch(thisOption.charAt(0)) {
				case 'r':
					reads = Integer.parseInt(thisOptionValue);
					break;
				case 'w':
					writes = Integer.parseInt(thisOptionValue);
					break;
				case 'v':
					variance = thisOptionValue;
					break;
				case 't':
					this.items = parseFixedTransaction(thisOptionValue);
			}
		}
		if (reads < 0) {
			throw new Exception("reads cannot be negative for transactional workload");
		}
		if (writes < 0) {
			throw new Exception("writes cannot be negative for transactional workload");
		}
		if (reads == 0 && writes == 0 && (items == null || items.length == 0)) {
			throw new Exception("no reads or writes defined for transactional workload");
		}

		this.minReads = applyVariance(reads, variance, VariationType.MINUS);
		this.maxReads = applyVariance(reads, variance, VariationType.PLUS);
		this.minWrites = applyVariance(writes, variance, VariationType.MINUS);
		this.maxWrites = applyVariance(writes, variance, VariationType.PLUS);

	}

	private TransactionalItem[] parseFixedTransaction(String thisOptionValue) throws Exception{
		// A fixed transaction string consists of a code for a transactional item, possibly preceeded by
		// an count. Eg "rwrrriu20r" is a read, a write (either update or replace), 3 reads, an insert,
		// an update then 20 reads, in that order.
		List<TransactionalItem> itemList = new ArrayList<TransactionalItem>();
		String options = "^((\\d*[";
		for (TransactionalType item : TransactionalType.values()) {
			options += item.getCode();
		}
		options += "]))+$";
		if (!thisOptionValue.matches(options)) {
			throw new Exception("transaction pattern '" + thisOptionValue + "' is not valid (should match " + options + ").");
		}
		int index = 0;
		while (index < thisOptionValue.length()) {
			int startIndex = index;
			int count = 1;
			while (Character.isDigit(thisOptionValue.charAt(index))) {
				index++;
			}
			if (index != startIndex) {
				count = Integer.parseInt(thisOptionValue.substring(startIndex, index));
			}
			TransactionalType type = TransactionalType.lookupCode(thisOptionValue.charAt(index));
			if (type.isBatch()) {
				itemList.add(new TransactionalItem(type, count));
			}
			else {
				for (int c = 0; c < count; c++) {
					itemList.add(new TransactionalItem(type));
				}
			}
			index++;
		}
		return itemList.toArray(new TransactionalItem[0]);
	}

	private int applyVariance(int base, String varianceStr, VariationType type) throws Exception{
		if (varianceStr == null || varianceStr.isEmpty() || base == 0) {
			return base;
		}

		// Parse the variance
		double variance;
		if (varianceStr.matches("^\\d+(\\.\\d+)?%$")) {
			// Percentage variance, like 23.4%
			double variancePct = Double.parseDouble(varianceStr.substring(0, varianceStr.length() - 1));
			variance = base * variancePct/100;
		}
		else if (varianceStr.matches("^\\d+$")) {
			// Absolute variance
			variance = Double.parseDouble(varianceStr);
		}
		else {
			throw new Exception("Cannot parse variance string '" + varianceStr + "'");
		}
		double result;
		if (type == VariationType.PLUS) {
			result = base + Math.floor(variance);
		}
		else {
			result = base - Math.floor(variance);
		}
		return Math.min(0, (int)result);
	}


	private class WorkloadIterator implements Iterator<TransactionalItem> {
		private int reads = 0;
		private int writes = 0;
		private RandomShift random;
		private int fixedSequenceIndex = 0;

		public WorkloadIterator(RandomShift random) {
			this.random = random;

			if (this.random == null) {
				this.reads = (minReads + maxReads)/2;
				this.writes = (minWrites + maxWrites)/2;
			}
			else {
				if (minReads == maxReads) {
					reads = minReads;
				}
				else {
					reads = minReads + random.nextInt(maxReads-minReads+1);
				}
				if (minWrites == maxWrites) {
					writes = minWrites;
				}
				else {
					writes = minWrites + random.nextInt(maxWrites-minWrites+1);
				}
			}
 		}

		@Override
		public boolean hasNext() {
			if (items != null && fixedSequenceIndex < items.length) {
				return true;
			}
			return reads > 0 || writes > 0;
		}

		@Override
		public TransactionalItem next() {
			TransactionalItem result = null;
			if (items != null && fixedSequenceIndex < items.length) {
				result = items[fixedSequenceIndex++];
			}
			else {
				// determine the result based on what's pending
				if (writes > 0 && reads > 0) {
					if (random == null) {
						result = (writes > reads) ? MULTI_BIN_WRITE : MULTI_BIN_READ;
					}
					else {
						int index = random.nextInt(writes + reads);
						result = (index > writes)? MULTI_BIN_READ : MULTI_BIN_WRITE;
					}
				}
				else if (reads > 0) {
					result = MULTI_BIN_READ;
				}
				else if (writes > 0) {
					result = MULTI_BIN_WRITE;
				}
			}
			if (result.getType().isRead()) {
				reads--;
			}
			else {
				writes--;
			}
			// We never want to return back a WRITE, this really is either an update or a replace
			if (result.getType() == TransactionalType.MULTI_BIN_WRITE) {
				result = MULTI_BIN_UPDATE;
			}
			else if (result.getType() == TransactionalType.SINGLE_BIN_WRITE) {
				result = SINGLE_BIN_UPDATE;
			}
			return result;
		}

		@Override
		public void remove() {
			throw new UnsupportedOperationException("Remove is not supported");
		}
	}
	@Override
	public Iterator<TransactionalItem> iterator() {
		return new WorkloadIterator(null);
	}
	public Iterator<TransactionalItem> iterator(RandomShift random) {
		return new WorkloadIterator(random);
	}
}
