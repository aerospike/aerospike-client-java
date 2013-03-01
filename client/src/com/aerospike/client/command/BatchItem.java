/*
 * Aerospike Client - Java Library
 *
 * Copyright 2012 by Aerospike, Inc. All rights reserved.
 *
 * Availability of this source code to partners and customers includes
 * redistribution rights covered by individual contract. Please check your
 * contract for exact rights and responsibilities.
 */
package com.aerospike.client.command;

import java.util.ArrayList;

public final class BatchItem {
	private int index;
	private ArrayList<Integer> duplicates;
	
	public BatchItem(int idx) {
		this.index = idx;
	}
	
	public void addDuplicate(int idx) {
		if (duplicates == null) {
			duplicates = new ArrayList<Integer>();
			duplicates.add(index);
			index = 0;
		}
		duplicates.add(idx);
	}
	
	public int getIndex() {
		return (duplicates == null)? index : duplicates.get(index++);
	}
}
