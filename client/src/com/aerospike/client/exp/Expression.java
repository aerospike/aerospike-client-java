/*
 * Copyright 2012-2021 Aerospike, Inc.
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
package com.aerospike.client.exp;

import java.io.Serializable;
import java.util.Arrays;

import com.aerospike.client.command.Command;
import com.aerospike.client.util.Crypto;
import com.aerospike.client.util.Packer;

/**
 * Packed expression byte instructions.
 */
public final class Expression implements CommandExp, Serializable {
	private static final long serialVersionUID = 1L;

	private final byte[] bytes;

	/**
	 * Expression constructor used by {@link Exp#build(Exp)}
	 */
	Expression(Exp exp) {
		Packer packer = new Packer();
		exp.pack(packer);
		bytes = packer.toByteArray();
		/*
		for (int i = 0; i < bytes.length; i++) {
			int b = bytes[i] & 0xff;
			System.out.println("" + b);
		}
		*/
	}

	/**
	 * Return packed byte instructions.
	 */
	public byte[] getBytes() {
		return bytes;
	}

	/**
	 * Return byte instructions in base64 encoding.
	 */
	public String getBase64() {
		return Crypto.encodeBase64(bytes);
	}

	/**
	 * Estimate expression size in wire protocol.
	 * For internal use only.
	 */
	public int size() {
		return bytes.length + Command.FIELD_HEADER_SIZE;
	}

	/**
	 * Write expression in wire protocol.
	 * For internal use only.
	 */
	public int write(Command cmd) {
		cmd.writeExpHeader(bytes.length);
		System.arraycopy(bytes, 0, cmd.dataBuffer, cmd.dataOffset, bytes.length);
		return cmd.dataOffset + bytes.length;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + Arrays.hashCode(bytes);
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		Expression other = (Expression) obj;
		if (!Arrays.equals(bytes, other.bytes))
			return false;
		return true;
	}
}
