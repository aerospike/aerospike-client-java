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
package com.aerospike.test.async;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.junit.Test;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.Key;
import com.aerospike.client.Record;
import com.aerospike.client.Value;
import com.aerospike.client.cdt.ListOperation;
import com.aerospike.client.cdt.MapOperation;
import com.aerospike.client.cdt.MapPolicy;
import com.aerospike.client.cdt.MapReturnType;
import com.aerospike.client.listener.DeleteListener;
import com.aerospike.client.listener.RecordListener;

public class TestAsyncOperate extends TestAsync {
	private static final String binName = args.getBinName("putgetbin");

	@Test
	public void asyncOperateList() {
		final Key key = new Key(args.namespace, args.set, "aoplkey1");

		client.delete(eventLoop, new DeleteListener() {
			public void onSuccess(Key key, boolean existed) {
				List<Value> itemList = new ArrayList<Value>();
				itemList.add(Value.get(55));
				itemList.add(Value.get(77));

				client.operate(eventLoop, new ReadHandler(), null, key,
						ListOperation.appendItems(binName, itemList),
						ListOperation.pop(binName, -1),
						ListOperation.size(binName)
						);
			}

			public void onFailure(AerospikeException e) {
				setError(e);
				notifyComplete();
			}
		}, null, key);

		waitTillComplete();
	}

	private class ReadHandler implements RecordListener {

		public void onSuccess(Key key, Record record) {
			assertRecordFound(key, record);

			List<?> list = record.getList(binName);

			long size = (Long)list.get(0);
			assertEquals(2, size);

			long val = (Long)list.get(1);
			assertEquals(77, val);

			size = (Long)list.get(2);
			assertEquals(1, size);

			notifyComplete();
		}

		public void onFailure(AerospikeException e) {
			setError(e);
			notifyComplete();
		}
	}

	@Test
	public void asyncOperateMap() {
		final Key key = new Key(args.namespace, args.set, "aopmkey1");

		client.delete(eventLoop, new DeleteListener() {
			public void onSuccess(Key key, boolean existed) {
				Map<Value,Value> map = new HashMap<Value,Value>();
				map.put(Value.get("a"), Value.get(1));
				map.put(Value.get("b"), Value.get(2));
				map.put(Value.get("c"), Value.get(3));

				client.operate(eventLoop, new MapHandler(), null, key,
						MapOperation.putItems(MapPolicy.Default, binName, map),
						MapOperation.getByRankRange(binName, -1, 1, MapReturnType.KEY_VALUE)
						);
			}

			public void onFailure(AerospikeException e) {
				setError(e);
				notifyComplete();
			}
		}, null, key);

		waitTillComplete();
	}

	private class MapHandler implements RecordListener {

		public void onSuccess(Key key, Record record) {
			assertRecordFound(key, record);

			List<?> results = record.getList(binName);

			long size = (Long)results.get(0);
			assertEquals(3, size);

			List<?> list = (List<?>)results.get(1);
			Entry<?,?> entry = (Entry<?,?>)list.get(0);
			assertEquals("c", entry.getKey());
			assertEquals(3L, entry.getValue());

			notifyComplete();
		}

		public void onFailure(AerospikeException e) {
			setError(e);
			notifyComplete();
		}
	}
}
