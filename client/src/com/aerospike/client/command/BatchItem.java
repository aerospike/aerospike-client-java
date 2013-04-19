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
import java.util.HashMap;

import com.aerospike.client.Key;

public final class BatchItem {
	
	public static HashMap<Key,BatchItem> generateMap(Key[] keys) {
		
		HashMap<Key,BatchItem> keyMap = new HashMap<Key,BatchItem>(keys.length);
		
		for (int i = 0; i < keys.length; i++) {
			Key key = keys[i];
			BatchItem item = keyMap.get(key);
			
			if (item == null) {
				item = new BatchItem(i);
				keyMap.put(key, item);
			}
			else {
				item.addDuplicate(i);
			}
		}
		return keyMap;
	}
	
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
