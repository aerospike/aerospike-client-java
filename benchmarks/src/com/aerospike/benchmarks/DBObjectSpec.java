/*
 * Copyright 2012-2024 Aerospike, Inc.
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

class DBObjectSpec {
	enum Type {
		INTEGER,
		BYTES,
		STRING,
		RANDOM,
		TIMESTAMP
	}

	final Type type;
	final int size;
	final int randPct;

	DBObjectSpec(String s) {
		String[] args = s.split(":");
		this.type = parseType(args[0].charAt(0));

		switch (this.type) {
			default:
			case TIMESTAMP:
			case INTEGER:
				this.size = 8;
				this.randPct = 0;
				break;

			case STRING:
			case BYTES:
				this.size = Integer.parseInt(args[1]);
				this.randPct = 0;
				break;

			case RANDOM:
				// Convert size to multiples of 8.
				this.size = Math.abs(Integer.parseInt(args[1]) / 8);
				this.randPct = Math.abs(Integer.parseInt(args[2]));
				break;
		}
	}

	DBObjectSpec() {
		this.type = Type.INTEGER;
		this.size = 8;
		this.randPct = 0;
	}

	static Type parseType(char t) {
		switch (t) {
			case 'I':
				return Type.INTEGER;

			case 'B':
				return Type.BYTES;

			case 'S':
				return Type.STRING;

			case 'R':
				return Type.RANDOM;

			case 'D':
				return Type.TIMESTAMP;

			default:
				throw new RuntimeException("Invalid type: " + t);
		}
	}
}
