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

import java.io.DataInputStream;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

/**
 * Class and string constants read from a class file's constant pool.
 */
final class ClassConstants {
	final List<String> classReferences = new ArrayList<String>();
	final List<String> strings = new ArrayList<String>();

	ClassConstants(Class<?> cls) throws Exception {
		String file = cls.getName().substring(cls.getName().lastIndexOf('.') + 1) + ".class";

		try (InputStream is = cls.getResourceAsStream(file)) {
			DataInputStream in = new DataInputStream(is);
			in.readInt();
			in.readUnsignedShort();
			in.readUnsignedShort();

			int count = in.readUnsignedShort();
			String[] utf8 = new String[count];
			List<Integer> classIndexes = new ArrayList<Integer>();
			List<Integer> stringIndexes = new ArrayList<Integer>();

			for (int i = 1; i < count; i++) {
				int tag = in.readUnsignedByte();

				switch (tag) {
				case 1:
					utf8[i] = in.readUTF();
					break;
				case 7:
					classIndexes.add(in.readUnsignedShort());
					break;
				case 8:
					stringIndexes.add(in.readUnsignedShort());
					break;
				case 16: case 19: case 20:
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

			for (int index : classIndexes) {
				classReferences.add(utf8[index]);
			}

			for (String s : utf8) {
				if (s != null && s.startsWith("(")) {
					classReferences.addAll(descriptorTypes(s));
				}
			}

			for (int index : stringIndexes) {
				strings.add(utf8[index]);
			}
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
