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
