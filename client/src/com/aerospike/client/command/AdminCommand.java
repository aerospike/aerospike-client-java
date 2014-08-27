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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.mindrot.jbcrypt.BCrypt;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.UserRoles;
import com.aerospike.client.cluster.Cluster;
import com.aerospike.client.cluster.Connection;
import com.aerospike.client.cluster.Node;
import com.aerospike.client.policy.AdminPolicy;
import com.aerospike.client.util.ThreadLocalData;

public final class AdminCommand {
	// Commands
	private static final byte AUTHENTICATE = 0;
	private static final byte CREATE_USER = 1;
	private static final byte DROP_USER = 2;
	private static final byte SET_PASSWORD = 3;
	private static final byte CHANGE_PASSWORD = 4;
	private static final byte GRANT_ROLES = 5;
	private static final byte REVOKE_ROLES = 6;
	private static final byte REPLACE_ROLES = 7;
	//private static final byte CREATE_ROLE = 8;
	private static final byte QUERY_USERS = 9;
	//private static final byte QUERY_ROLES = 10;

	// Field IDs
	private static final byte USER = 0;
	private static final byte PASSWORD = 1;
	private static final byte OLD_PASSWORD = 2;
	private static final byte CREDENTIAL = 3;
	private static final byte ROLES = 10;
	//private static final byte PRIVILEGES = 11;

	// Misc
	private static final long MSG_VERSION = 0L;
	private static final long MSG_TYPE = 2L;
	private static final int FIELD_HEADER_SIZE = 5;
	private static final int HEADER_SIZE = 24;
	private static final int HEADER_REMAINING = 16;
	private static final int RESULT_CODE = 9;
	private static final int QUERY_END = 50;

	private byte[] dataBuffer;
	private int dataOffset;

	public AdminCommand() {
		dataBuffer = ThreadLocalData.getBuffer();
		dataOffset = 8;
	}

	public AdminCommand(byte[] dataBuffer) {
		this.dataBuffer = dataBuffer;
		dataOffset = 8;
	}

	public void authenticate(Connection conn, byte[] user, byte[] password) 
		throws AerospikeException, IOException {
		
		setAuthenticate(user, password);
		conn.write(dataBuffer, dataOffset);
		conn.readFully(dataBuffer, HEADER_SIZE);

		int result = dataBuffer[RESULT_CODE];
		if (result != 0)
		{
			throw new AerospikeException(result, "Authentication failed");
		}
	}

	public int setAuthenticate(byte[] user, byte[] password) {
		writeHeader(AUTHENTICATE, 2);
		writeField(USER, user);
		writeField(CREDENTIAL, password);
		writeSize();
		return dataOffset;
	}

	public void createUser(Cluster cluster, AdminPolicy policy, String user, String password, List<String> roles) throws AerospikeException {
		writeHeader(CREATE_USER, 3);
		writeField(USER, user);
		writeField(PASSWORD, password);
		writeRoles(roles);
		executeCommand(cluster, policy);
	}

	public void dropUser(Cluster cluster, AdminPolicy policy, String user) throws AerospikeException {
		writeHeader(DROP_USER, 1);
		writeField(USER, user);
		executeCommand(cluster, policy);
	}

	public void setPassword(Cluster cluster, AdminPolicy policy, byte[] user, String password) throws AerospikeException {
		writeHeader(SET_PASSWORD, 2);
		writeField(USER, user);
		writeField(PASSWORD, password);
		executeCommand(cluster, policy);
	}

	public void changePassword(Cluster cluster, AdminPolicy policy, byte[] user, String password) throws AerospikeException {
		writeHeader(CHANGE_PASSWORD, 3);
		writeField(USER, user);
		writeField(OLD_PASSWORD, cluster.getPassword());
		writeField(PASSWORD, password);
		executeCommand(cluster, policy);
	}

	public void grantRoles(Cluster cluster, AdminPolicy policy, String user, List<String> roles) throws AerospikeException {
		writeHeader(GRANT_ROLES, 2);
		writeField(USER, user);
		writeRoles(roles);
		executeCommand(cluster, policy);
	}

	public void revokeRoles(Cluster cluster, AdminPolicy policy, String user, List<String> roles) throws AerospikeException {
		writeHeader(REVOKE_ROLES, 2);
		writeField(USER, user);
		writeRoles(roles);
		executeCommand(cluster, policy);
	}

	public void replaceRoles(Cluster cluster, AdminPolicy policy, String user, List<String> roles) throws AerospikeException {
		writeHeader(REPLACE_ROLES, 2);
		writeField(USER, user);
		writeRoles(roles);
		executeCommand(cluster, policy);
	}

	public UserRoles queryUser(Cluster cluster, AdminPolicy policy, String user) throws AerospikeException {
		List<UserRoles> list = new ArrayList<UserRoles>(1);
		writeHeader(QUERY_USERS, 1);
		writeField(USER, user);
		readUsers(cluster, policy, list);
		return (list.size() > 0) ? list.get(0) : null;
	}

	public List<UserRoles> queryUsers(Cluster cluster, AdminPolicy policy) throws AerospikeException {
		List<UserRoles> list = new ArrayList<UserRoles>(100);
		writeHeader(QUERY_USERS, 0);
		readUsers(cluster, policy, list);
		return list;
	}

	private void writeRoles(List<String> roles) {
		int offset = dataOffset + FIELD_HEADER_SIZE;
		dataBuffer[offset++] = (byte)roles.size();

		for (String role : roles) {
			int len = Buffer.stringToUtf8(role, dataBuffer, offset + 1);
			dataBuffer[offset] = (byte)len;
			offset += len + 1;
		}

		int size = offset - dataOffset - FIELD_HEADER_SIZE;
		writeFieldHeader(ROLES, size);
		dataOffset = offset;
	}

	private void writeSize() {
		// Write total size of message which is the current offset.
		long size = ((long)dataOffset - 8) | (MSG_VERSION << 56) | (MSG_TYPE << 48);
		Buffer.longToBytes(size, dataBuffer, 0);
	}

	private void writeHeader(byte command, int fieldCount) {
		// Authenticate header is almost all zeros
		Arrays.fill(dataBuffer, dataOffset, dataOffset + 16, (byte)0);
		dataBuffer[dataOffset + 2] = command;
		dataBuffer[dataOffset + 3] = (byte)fieldCount;
		dataOffset += 16;
	}

	private void writeField(byte id, String str) {
		int len = Buffer.stringToUtf8(str, dataBuffer, dataOffset + FIELD_HEADER_SIZE);
		writeFieldHeader(id, len);
		dataOffset += len;
	}

	private void writeField(byte id, byte[] bytes) {
		System.arraycopy(bytes, 0, dataBuffer, dataOffset + FIELD_HEADER_SIZE, bytes.length);
		writeFieldHeader(id, bytes.length);
		dataOffset += bytes.length;
	}

	private void writeFieldHeader(byte id, int size) {
		Buffer.intToBytes(size + 1, dataBuffer, dataOffset);
		dataOffset += 4;
		dataBuffer[dataOffset++] = id;
	}

	private void executeCommand(Cluster cluster, AdminPolicy policy) throws AerospikeException {
		writeSize();
		Node node = cluster.getRandomNode();
		int timeout = (policy == null) ? 1000 : policy.timeout;
		Connection conn = node.getConnection(timeout);

		try {
			conn.write(dataBuffer, dataOffset);
			conn.readFully(dataBuffer, HEADER_SIZE);
			node.putConnection(conn);
		}
		catch (Exception e) {
			// All IO exceptions are considered fatal.  Do not retry.
			// Close socket to flush out possible garbage.  Do not put back in pool.
			conn.close();
			throw new AerospikeException(e);
		}

		int result = dataBuffer[RESULT_CODE];

		if (result != 0)
		{
			throw new AerospikeException(result);
		}
	}

	public void readUsers(Cluster cluster, AdminPolicy policy, List<UserRoles> list) throws AerospikeException {
		writeSize();
		Node node = cluster.getRandomNode();
		int timeout = (policy == null) ? 1000 : policy.timeout;
		int status = 0;
		Connection conn = node.getConnection(timeout);

		try {
			conn.write(dataBuffer, dataOffset);
			status = readUserBlocks(conn, list);
			node.putConnection(conn);
		}
		catch (Exception e) {
			// Garbage may be in socket.  Do not put back into pool.
			conn.close();
			throw new AerospikeException(e);
		}

		if (status > 0) {
			throw new AerospikeException(status, "Query users failed.");
		}
	}

	private int readUserBlocks(Connection conn, List<UserRoles> list) throws IOException {
		int status = 0;

		while (status == 0)	{
			conn.readFully(dataBuffer, 8);
			long size = Buffer.bytesToLong(dataBuffer, 0);
			int receiveSize = ((int)(size & 0xFFFFFFFFFFFFL));

			if (receiveSize > 0) {
				if (receiveSize > dataBuffer.length) {
					dataBuffer = ThreadLocalData.resizeBuffer(receiveSize);
				}
				conn.readFully(dataBuffer, receiveSize);
				status = parseUsers(list, receiveSize);
			}
			else {
				break;
			}
		}
		return status;
	}

	private int parseUsers(List<UserRoles> list, int receiveSize)
	{
		dataOffset = 0;

		while (dataOffset < receiveSize) {
			int resultCode = dataBuffer[dataOffset + 1];

			if (resultCode != 0) {
				if (resultCode == QUERY_END) {
					return -1;
				}
				return resultCode;
			}

			UserRoles userRoles = new UserRoles();
			int fieldCount = dataBuffer[dataOffset + 3];
			dataOffset += HEADER_REMAINING;

			for (int i = 0; i < fieldCount; i++) {
				int len = Buffer.bytesToInt(dataBuffer, dataOffset);
				dataOffset += 4;
				int id = dataBuffer[dataOffset++];
				len--;

				if (id == USER) {
					userRoles.user = Buffer.utf8ToString(dataBuffer, dataOffset, len);
					dataOffset += len;
				}
				else if (id == ROLES) {
					parseRoles(userRoles);
				}
				else {
					dataOffset += len;
				}
			}

			if (userRoles.user == null && userRoles.roles == null) {
				continue;
			}

			if (userRoles.roles == null) {
				userRoles.roles = new ArrayList<String>(0);
			}
			list.add(userRoles);
		}
		return 0;
	}

	private void parseRoles(UserRoles userRoles) {
		int size = dataBuffer[dataOffset++];
		userRoles.roles = new ArrayList<String>(size);

		for (int i = 0; i < size; i++) {
			int len = dataBuffer[dataOffset++];
			String role = Buffer.utf8ToString(dataBuffer, dataOffset, len);
			dataOffset += len;
			userRoles.roles.add(role);
		}
	}

	public static String hashPassword(String password) {
		return BCrypt.hashpw(password, "$2a$10$7EqJtq98hPqEX7fNZaFWoO");
	}
}
