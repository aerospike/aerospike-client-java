/*
 * Copyright 2012-2026 Aerospike, Inc.
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
package com.aerospike.client.discovery;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.fail;

import java.io.DataInputStream;
import java.io.InputStream;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.Test;

import com.aerospike.client.Host;
import com.aerospike.client.discovery.Endpoint.SourceList;

public class StaticMapAddressTranslatorNoIoTest {
	@Test
	public void guardDetectsLookup() throws Exception {
		GuardedResolverProvider.arm();

		try {
			InetAddress.getByName("translator-guard.invalid");
			fail("Lookup was not intercepted");
		}
		catch (AssertionError e) {
			assertEquals("Unexpected name lookup: translator-guard.invalid", e.getMessage());
		}
		finally {
			GuardedResolverProvider.disarm();
		}
	}

	@Test
	public void translateDoesNoNameLookup() {
		Map<String,String> ipMap = new HashMap<String,String>();
		ipMap.put("node1.internal", "node1.external.invalid");
		StaticMapAddressTranslator translator = new StaticMapAddressTranslator(ipMap);

		GuardedResolverProvider.arm();

		try {
			Host hit = translator.translate(new Endpoint("BB9", "tls1", "node1.internal", 3000, SourceList.STANDARD));
			Host miss = translator.translate(new Endpoint("BB9", "tls1", "node2.internal", 3000, SourceList.ALTERNATE));

			assertEquals("node1.external.invalid", hit.name);
			assertEquals("node2.internal", miss.name);
		}
		finally {
			GuardedResolverProvider.disarm();
		}
	}

	@Test
	public void translatorReferencesNoIoTypes() throws Exception {
		for (String name : classReferences(StaticMapAddressTranslator.class)) {
			assertFalse(name, name.startsWith("java/net/"));
			assertFalse(name, name.startsWith("java/nio/"));
			assertFalse(name, name.startsWith("java/io/"));
		}
	}

	private static List<String> classReferences(Class<?> cls) throws Exception {
		try (InputStream is = cls.getResourceAsStream(cls.getSimpleName() + ".class")) {
			DataInputStream in = new DataInputStream(is);
			in.readInt();
			in.readUnsignedShort();
			in.readUnsignedShort();

			int count = in.readUnsignedShort();
			String[] utf8 = new String[count];
			List<Integer> classIndexes = new ArrayList<Integer>();

			for (int i = 1; i < count; i++) {
				int tag = in.readUnsignedByte();

				switch (tag) {
				case 1:
					utf8[i] = in.readUTF();
					break;
				case 7:
					classIndexes.add(in.readUnsignedShort());
					break;
				case 8: case 16: case 19: case 20:
					in.readUnsignedShort();
					break;
				case 15:
					in.readUnsignedByte();
					in.readUnsignedShort();
					break;
				case 3: case 4: case 9: case 10: case 11: case 12: case 17: case 18:
					in.readInt();
					break;
				case 5: case 6:
					in.readLong();
					i++;
					break;
				default:
					throw new IllegalStateException("Unknown constant pool tag " + tag);
				}
			}

			List<String> names = new ArrayList<String>();

			for (int index : classIndexes) {
				names.add(utf8[index]);
			}

			for (String s : utf8) {
				if (s != null && s.startsWith("(")) {
					names.addAll(descriptorTypes(s));
				}
			}
			return names;
		}
	}

	private static List<String> descriptorTypes(String descriptor) {
		List<String> types = new ArrayList<String>();
		int start = descriptor.indexOf('L');

		while (start >= 0) {
			int end = descriptor.indexOf(';', start);
			types.add(descriptor.substring(start + 1, end));
			start = descriptor.indexOf('L', end);
		}
		return types;
	}
}
