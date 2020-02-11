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
package com.aerospike.helper.model;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

/**
 * This class represents a namespace
 * defined in the cluster.
 *
 * @author peter
 */
public class Namespace {
	protected String name;
	protected Map<String, Set> sets;
	protected Map<String, NameValuePair> values;
	protected java.util.Set<String> dontMerge = new HashSet<String>(Arrays.asList("available-bin-names",
			"cold-start-evict-ttl",
			"current-time",
			"default-ttl",
			"evict-tenths-pct",
			"free-pct-memory",
			"high-water-disk-pct",
			"high-water-memory-pct",
			"max-ttl",
			"max-void-time",
			"nsup-cycle-duration",
			"nsup-cycle-sleep-pct",
			"repl-factor",
			"stop-writes-pct"));

	public Namespace(String name) {
		this.name = name;
		values = new HashMap<String, NameValuePair>();
	}

	@Override
	public String toString() {
		return this.name;
	}

	@Override
	public boolean equals(Object obj) {
		return ((obj instanceof Namespace) &&
				(obj.toString().equals(toString())));
	}

	public void addSet(String setData) {
		if (sets == null)
			sets = new HashMap<String, Set>();
		Set newSet = new Set(this, setData);
		Set existingSet = sets.get(newSet.getName());
		if (existingSet == null) {
			sets.put(newSet.getName(), newSet);
		} else {
			existingSet.setInfo(setData);
		}
	}

	public void mergeSet(String setData) {
		if (sets == null)
			sets = new HashMap<String, Set>();
		Set newSet = new Set(this, setData);
		Set existingSet = sets.get(newSet.getName());
		if (existingSet == null) {
			sets.put(newSet.getName(), newSet);
		} else {
			existingSet.mergeSetInfo(setData);
		}
	}

	public Collection<Set> getSets() {
		if (sets == null)
			sets = new HashMap<String, Set>();
		return sets.values();
	}

	public String getName() {
		return toString();
	}

	public void clear() {
		if (this.sets != null) {
			for (Set set : this.sets.values()) {
				set.clear();
			}
		}
	}

	public List<NameValuePair> getValues() {
		List<NameValuePair> result = new ArrayList<NameValuePair>();
		if (this.values != null) {
			java.util.Set<String> keys = this.values.keySet();
			for (String key : keys) {
				NameValuePair nvp = this.values.get(key);
				result.add(nvp);
			}
		}
		return result;
	}

	public void setInfo(String info, Map<String, NameValuePair> map, boolean merge) {
		/*
		type=device;objects=0;master-objects=0;prole-objects=0;expired-objects=0;evicted-objects=0; \
		set-deleted-objects=0;set-evicted-objects=0;used-bytes-memory=18688;data-used-bytes-memory=0; \
		index-used-bytes-memory=0;sindex-used-bytes-memory=18688;free-pct-memory=99;max-void-time=0; \
		min-evicted-ttl=0;max-evicted-ttl=0;non-expirable-objects=0;current-time=137899728; \
		stop-writes=false;hwm-breached=false;available-bin-names=32767;ldt_reads=0;ldt_read_success=0; \
		ldt_deletes=0;ldt_delete_success=0;ldt_writes=0;ldt_write_success=0;ldt_updates=0;ldt_errors=0; \
		used-bytes-disk=0;free-pct-disk=100;available_pct=99;sets-enable-xdr=true;memory-size=4294967296; \
		low-water-pct=0;high-water-disk-pct=50;high-water-memory-pct=60;evict-tenths-pct=5; \
		stop-writes-pct=90;cold-start-evict-ttl=4294967295;repl-factor=1;default-ttl=2592000;max-ttl=0; \
		conflict-resolution-policy=generation;allow_versions=false;single-bin=false;enable-xdr=false; \
		disallow-null-setname=false;total-bytes-memory=4294967296;total-bytes-disk=4294967296; \
		defrag-period=10;defrag-max-blocks=4000;defrag-lwm-pct=45;write-smoothing-period=0; \
		defrag-startup-minimum=10;max-write-cache=67108864;min-avail-pct=5;post-write-queue=0; \
		data-in-memory=true;load-at-startup=true;file=/opt/aerospike/test.data;filesize=4294967296; \
		writethreads=1;writecache=67108864;obj-size-hist-max=100
		 */
		if (map == null)
			return;

		if (info.isEmpty())
			return;
		String[] parts = info.split(";");

		for (String part : parts) {
			String[] kv = part.split("=");
			String key = kv[0];
			String value = kv[1];
			NameValuePair storedValue = map.get(key);
			if (storedValue == null) {
				storedValue = new NameValuePair(key, value);
				map.put(key, storedValue);
			} else {
				if (merge && !dontMerge.contains(key)) {
					try {
						Long newValue = Long.parseLong(value);
						Long oldValue = Long.parseLong(storedValue.value.toString());
						storedValue.value = Long.toString(oldValue + newValue);
					} catch (NumberFormatException e) {
						storedValue.value = value;
					}
				} else {
					storedValue.value = value;
				}
			}
		}
	}

	public void setNamespaceInfo(String info) {
		setInfo(info, values, false);
	}

	public void mergeNamespaceInfo(String info) {
		setInfo(info, values, true);
	}

	public Set findSet(String tableName) {
		return this.sets.get(tableName);
	}

}
