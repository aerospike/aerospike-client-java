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
