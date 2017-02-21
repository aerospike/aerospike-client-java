/*
 * Copyright 2012-2017 Aerospike, Inc.
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
package com.aerospike.client.cdt;

import java.util.List;

import com.aerospike.client.Operation;
import com.aerospike.client.Value;
import com.aerospike.client.util.Packer;

public abstract class MapBase {
	protected static final int SET_TYPE = 64;
	protected static final int ADD = 65;
	protected static final int ADD_ITEMS = 66;
	protected static final int PUT = 67;
	protected static final int PUT_ITEMS = 68;
	protected static final int REPLACE = 69;
	protected static final int REPLACE_ITEMS = 70;
	protected static final int INCREMENT = 73;
	protected static final int DECREMENT = 74;
	protected static final int CLEAR = 75;
	protected static final int REMOVE_BY_KEY = 76;
	protected static final int REMOVE_BY_INDEX = 77;
	protected static final int REMOVE_BY_RANK = 79;
	protected static final int REMOVE_BY_KEY_LIST = 81;
	protected static final int REMOVE_BY_VALUE = 82;
	protected static final int REMOVE_BY_VALUE_LIST = 83;
	protected static final int REMOVE_BY_KEY_INTERVAL = 84;
	protected static final int REMOVE_BY_INDEX_RANGE = 85;
	protected static final int REMOVE_BY_VALUE_INTERVAL = 86;
	protected static final int REMOVE_BY_RANK_RANGE = 87;
	protected static final int SIZE = 96;
	protected static final int GET_BY_KEY = 97;
	protected static final int GET_BY_INDEX = 98;
	protected static final int GET_BY_RANK = 100;
	protected static final int GET_BY_VALUE = 102;
	protected static final int GET_BY_KEY_INTERVAL = 103;
	protected static final int GET_BY_INDEX_RANGE = 104;
	protected static final int GET_BY_VALUE_INTERVAL = 105;
	protected static final int GET_BY_RANK_RANGE = 106;
	
	protected static Operation setMapPolicy(String binName, int attributes) {
		Packer packer = new Packer();
		packer.packRawShort(SET_TYPE);
		packer.packArrayBegin(1);
		packer.packInt(attributes);
		return new Operation(Operation.Type.MAP_MODIFY, binName, Value.get(packer.toByteArray()));
	}
	
	protected static Operation createPut(int command, int attributes, String binName, Value value1, Value value2) {
		Packer packer = new Packer();
		packer.packRawShort(command);
		
		if (command == MapBase.REPLACE) {
			// Replace doesn't allow map attributes because it does not create on non-existing key.
			packer.packArrayBegin(2);
			value1.pack(packer);
			value2.pack(packer);
		}
		else {
			packer.packArrayBegin(3);
			value1.pack(packer);
			value2.pack(packer);
			packer.packInt(attributes);
		}		
		return new Operation(Operation.Type.MAP_MODIFY, binName, Value.get(packer.toByteArray()));
	}

	protected static Operation createOperation(int command, int attributes, String binName, Value value1, Value value2) {
		Packer packer = new Packer();
		packer.packRawShort(command);		
		packer.packArrayBegin(3);
		value1.pack(packer);
		value2.pack(packer);
		packer.packInt(attributes);
		return new Operation(Operation.Type.MAP_MODIFY, binName, Value.get(packer.toByteArray()));
	}
	
	protected static Operation createOperation(int command, Operation.Type type, String binName) {
		Packer packer = new Packer();
		packer.packRawShort(command);
		return new Operation(type, binName, Value.get(packer.toByteArray()));
	}

	protected static Operation createOperation(int command, Operation.Type type, String binName, List<Value> value, MapReturnType returnType) {
		Packer packer = new Packer();
		packer.packRawShort(command);
		packer.packArrayBegin(2);
		packer.packInt(returnType.type);
		packer.packValueList(value);
		return new Operation(type, binName, Value.get(packer.toByteArray()));
	}

	protected static Operation createOperation(int command, Operation.Type type, String binName, Value value, MapReturnType returnType) {
		Packer packer = new Packer();
		packer.packRawShort(command);
		packer.packArrayBegin(2);
		packer.packInt(returnType.type);
		value.pack(packer);		
		return new Operation(type, binName, Value.get(packer.toByteArray()));
	}
	
	protected static Operation createOperation(int command, Operation.Type type, String binName, int index, MapReturnType returnType) {
		Packer packer = new Packer();
		packer.packRawShort(command);
		packer.packArrayBegin(2);
		packer.packInt(returnType.type);
		packer.packInt(index);
		return new Operation(type, binName, Value.get(packer.toByteArray()));
	}
	
	protected static Operation createOperation(int command, Operation.Type type, String binName, int index, int count, MapReturnType returnType) {
		Packer packer = new Packer();
		packer.packRawShort(command);
		packer.packArrayBegin(3);
		packer.packInt(returnType.type);
		packer.packInt(index);
		packer.packInt(count);
		return new Operation(type, binName, Value.get(packer.toByteArray()));
	}

	protected static Operation createRangeOperation(int command, Operation.Type type, String binName, Value begin, Value end, MapReturnType returnType) {
		Packer packer = new Packer();
		packer.packRawShort(command);
		
		if (begin == null) {
			begin = Value.getAsNull();
		}
		
		if (end == null) {
			packer.packArrayBegin(2);
			packer.packInt(returnType.type);
			begin.pack(packer);		
		}
		else {
			packer.packArrayBegin(3);
			packer.packInt(returnType.type);
			begin.pack(packer);		
			end.pack(packer);		
		}
		return new Operation(type, binName, Value.get(packer.toByteArray()));
	}
}
