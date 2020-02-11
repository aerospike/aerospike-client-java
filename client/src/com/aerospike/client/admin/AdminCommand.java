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
package com.aerospike.client.admin;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.mindrot.jbcrypt.BCrypt;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.Log;
import com.aerospike.client.ResultCode;
import com.aerospike.client.cluster.Cluster;
import com.aerospike.client.cluster.Connection;
import com.aerospike.client.cluster.Node;
import com.aerospike.client.command.Buffer;
import com.aerospike.client.command.Command;
import com.aerospike.client.policy.AdminPolicy;
import com.aerospike.client.policy.AuthMode;
import com.aerospike.client.util.ThreadLocalData;

public class AdminCommand {
	// Commands
	private static final byte AUTHENTICATE = 0;
	private static final byte CREATE_USER = 1;
	private static final byte DROP_USER = 2;
	private static final byte SET_PASSWORD = 3;
	private static final byte CHANGE_PASSWORD = 4;
	private static final byte GRANT_ROLES = 5;
	private static final byte REVOKE_ROLES = 6;
	private static final byte QUERY_USERS = 9;
	private static final byte CREATE_ROLE = 10;
	private static final byte DROP_ROLE = 11;
	private static final byte GRANT_PRIVILEGES = 12;
	private static final byte REVOKE_PRIVILEGES = 13;
	private static final byte SET_WHITELIST = 14;
	private static final byte QUERY_ROLES = 16;
	private static final byte LOGIN = 20;

	// Field IDs
	private static final byte USER = 0;
	private static final byte PASSWORD = 1;
	private static final byte OLD_PASSWORD = 2;
	private static final byte CREDENTIAL = 3;
	private static final byte CLEAR_PASSWORD = 4;
	private static final byte SESSION_TOKEN = 5;
	private static final byte SESSION_TTL = 6;
	private static final byte ROLES = 10;
	private static final byte ROLE = 11;
	private static final byte PRIVILEGES = 12;
	private static final byte WHITELIST = 13;

	// Misc
	private static final long MSG_VERSION = 2L;
	private static final long MSG_TYPE = 2L;
	private static final int FIELD_HEADER_SIZE = 5;
	private static final int HEADER_SIZE = 24;
	private static final int HEADER_REMAINING = 16;
	private static final int RESULT_CODE = 9;
	private static final int QUERY_END = 50;

	byte[] dataBuffer;
	int dataOffset;

	public AdminCommand() {
		this.dataBuffer = new byte[8192];
		dataOffset = 8;
	}

	public AdminCommand(byte[] dataBuffer) {
		this.dataBuffer = dataBuffer;
		dataOffset = 8;
	}

	public static class LoginCommand extends AdminCommand {
		public byte[] sessionToken = null;
		public long sessionExpiration = 0;

		public LoginCommand(Cluster cluster, Connection conn) throws IOException {
			super(ThreadLocalData.getBuffer());

			conn.setTimeout(cluster.loginTimeout);

			try {
				login(cluster, conn);
			}
			finally {
				conn.setTimeout(cluster.connectionTimeout);
			}
		}

		private void login(Cluster cluster, Connection conn) throws IOException {
			if (cluster.authMode == AuthMode.INTERNAL) {
				writeHeader(LOGIN, 2);
				writeField(USER, cluster.getUser());
				writeField(CREDENTIAL, cluster.getPasswordHash());
			}
			else {
				writeHeader(LOGIN, 3);
				writeField(USER, cluster.getUser());
				writeField(CREDENTIAL, cluster.getPasswordHash());
				writeField(CLEAR_PASSWORD, cluster.getPassword());
			}
			writeSize();
			conn.write(dataBuffer, dataOffset);
			conn.readFully(dataBuffer, HEADER_SIZE);

			int result = dataBuffer[RESULT_CODE] & 0xFF;

			if (result != 0) {
				if (result == ResultCode.INVALID_COMMAND) {
					// New login not supported.  Try old authentication.
					authenticateOld(cluster, conn);
					return;
				}

				if (result == ResultCode.SECURITY_NOT_ENABLED) {
					// Server does not require login.
					return;
				}

				// login failed.
				throw new AerospikeException(result, "Login failed");
			}

			// Read session token.
			long size = Buffer.bytesToLong(dataBuffer, 0);
			int receiveSize = ((int)(size & 0xFFFFFFFFFFFFL)) - HEADER_REMAINING;
			int fieldCount = dataBuffer[11] & 0xFF;

			if (receiveSize <= 0 || receiveSize > dataBuffer.length || fieldCount <= 0) {
				throw new AerospikeException(result, "Failed to retrieve session token");
			}

			conn.readFully(dataBuffer, receiveSize);
			dataOffset = 0;

			for (int i = 0; i < fieldCount; i++) {
				int len = Buffer.bytesToInt(dataBuffer, dataOffset);
				dataOffset += 4;
				int id = dataBuffer[dataOffset++] & 0xFF;
				len--;

				if (id == SESSION_TOKEN) {
					sessionToken = Arrays.copyOfRange(dataBuffer, dataOffset, dataOffset + len);
				}
				else if (id == SESSION_TTL) {
					// Subtract 60 seconds from ttl so client session expires before server session.
					long seconds = Buffer.bigUnsigned32ToLong(dataBuffer, dataOffset) - 60;

					if (seconds > 0) {
						sessionExpiration = System.nanoTime() + TimeUnit.SECONDS.toNanos(seconds);
					}
					else {
						Log.warn("Invalid session TTL: " + seconds);
					}
				}
				dataOffset += len;
			}

			if (sessionToken == null) {
				throw new AerospikeException(result, "Failed to retrieve session token");
			}
		}
	}

	public boolean authenticate(Cluster cluster, Connection conn, byte[] sessionToken)
		throws AerospikeException, IOException {

		dataOffset = 8;
		setAuthenticate(cluster, sessionToken);
		conn.write(dataBuffer, dataOffset);
		conn.readFully(dataBuffer, HEADER_SIZE, Command.STATE_READ_AUTH_HEADER);

		int result = dataBuffer[RESULT_CODE] & 0xFF;
		return result == 0 || result == ResultCode.SECURITY_NOT_ENABLED;
	}

	public int setAuthenticate(Cluster cluster, byte[] sessionToken) {
		writeHeader(AUTHENTICATE, 2);
		writeField(USER, cluster.getUser());

		if (sessionToken != null) {
			// New authentication.
			writeField(SESSION_TOKEN, sessionToken);
		}
		else {
			// Old authentication.
			writeField(CREDENTIAL, cluster.getPasswordHash());
		}
		writeSize();
		return dataOffset;
	}

	public void authenticateOld(Cluster cluster, Connection conn)
		throws AerospikeException, IOException {

		dataOffset = 8;
		writeHeader(AUTHENTICATE, 2);
		writeField(USER, cluster.getUser());
		writeField(CREDENTIAL, cluster.getPasswordHash());
		writeSize();

		conn.write(dataBuffer, dataOffset);
		conn.readFully(dataBuffer, HEADER_SIZE);

		int result = dataBuffer[RESULT_CODE] & 0xFF;
		if (result != 0) {
			throw new AerospikeException(result, "Authentication failed");
		}
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
		writeField(OLD_PASSWORD, cluster.getPasswordHash());
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

	public void createRole(Cluster cluster, AdminPolicy policy, String roleName, List<Privilege> privileges) throws AerospikeException {
		writeHeader(CREATE_ROLE, 2);
		writeField(ROLE, roleName);
		writePrivileges(privileges);
		executeCommand(cluster, policy);
	}

	public void createRole(Cluster cluster, AdminPolicy policy, String roleName, List<Privilege> privileges, List<String> whitelist) throws AerospikeException {
		int fieldCount = 1;

		if (privileges != null && privileges.size() > 0) {
			fieldCount++;
		}

		if (whitelist != null && whitelist.size() > 0) {
			fieldCount++;
		}

		writeHeader(CREATE_ROLE, fieldCount);
		writeField(ROLE, roleName);

		if (privileges != null && privileges.size() > 0) {
			writePrivileges(privileges);
		}

		if (whitelist != null && whitelist.size() > 0) {
			writeWhitelist(whitelist);
		}
		executeCommand(cluster, policy);
	}

	public void dropRole(Cluster cluster, AdminPolicy policy, String roleName) throws AerospikeException {
		writeHeader(DROP_ROLE, 1);
		writeField(ROLE, roleName);
		executeCommand(cluster, policy);
	}

	public void grantPrivileges(Cluster cluster, AdminPolicy policy, String roleName, List<Privilege> privileges) throws AerospikeException {
		writeHeader(GRANT_PRIVILEGES, 2);
		writeField(ROLE, roleName);
		writePrivileges(privileges);
		executeCommand(cluster, policy);
	}

	public void revokePrivileges(Cluster cluster, AdminPolicy policy, String roleName, List<Privilege> privileges) throws AerospikeException {
		writeHeader(REVOKE_PRIVILEGES, 2);
		writeField(ROLE, roleName);
		writePrivileges(privileges);
		executeCommand(cluster, policy);
	}

	public void setWhitelist(Cluster cluster, AdminPolicy policy, String roleName, List<String> whitelist) throws AerospikeException {
		int fieldCount = (whitelist != null && whitelist.size() > 0) ? 2 : 1;

		writeHeader(SET_WHITELIST, fieldCount);
		writeField(ROLE, roleName);

		if (whitelist != null && whitelist.size() > 0) {
			writeWhitelist(whitelist);
		}

		executeCommand(cluster, policy);
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

	private void writePrivileges(List<Privilege> privileges) {
		int offset = dataOffset + FIELD_HEADER_SIZE;
		dataBuffer[offset++] = (byte)privileges.size();

		for (Privilege privilege : privileges) {
			dataBuffer[offset++] = (byte)privilege.code.id;

			if (privilege.code.canScope()) {

				if (! (privilege.setName == null || privilege.setName.length() == 0) &&
					(privilege.namespace == null || privilege.namespace.length() == 0)) {
					throw new AerospikeException(ResultCode.INVALID_PRIVILEGE, "Admin privilege '" +
						privilege.code + "' has a set scope with an empty namespace.");
				}

				int len = Buffer.stringToUtf8(privilege.namespace, dataBuffer, offset + 1);
				dataBuffer[offset] = (byte)len;
				offset += len + 1;

				len = Buffer.stringToUtf8(privilege.setName, dataBuffer, offset + 1);
				dataBuffer[offset] = (byte)len;
				offset += len + 1;
			}
			else {
				if (! (privilege.namespace == null || privilege.namespace.length() == 0) ||
					! (privilege.setName == null || privilege.setName.length() == 0)) {
					throw new AerospikeException(ResultCode.INVALID_PRIVILEGE, "Admin global privilege '" +
						privilege.code + "' has namespace/set scope which is invalid.");
				}
			}
		}

		int size = offset - dataOffset - FIELD_HEADER_SIZE;
		writeFieldHeader(PRIVILEGES, size);
		dataOffset = offset;
	}

	private void writeWhitelist(List<String> whitelist) {
		int offset = dataOffset + FIELD_HEADER_SIZE;
		boolean comma = false;

		for (String address : whitelist) {
			if (comma) {
				dataBuffer[offset++] = ',';
			}
			else {
				comma = true;
			}
			offset += Buffer.stringToUtf8(address, dataBuffer, offset);
		}

		int size = offset - dataOffset - FIELD_HEADER_SIZE;
		writeFieldHeader(WHITELIST, size);
		dataOffset = offset;
	}

	final void writeSize() {
		// Write total size of message which is the current offset.
		long size = ((long)dataOffset - 8) | (MSG_VERSION << 56) | (MSG_TYPE << 48);
		Buffer.longToBytes(size, dataBuffer, 0);
	}

	final void writeHeader(byte command, int fieldCount) {
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

	final void writeField(byte id, byte[] bytes) {
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
			conn.updateLastUsed();
			node.putConnection(conn);
		}
		catch (Exception e) {
			// All IO exceptions are considered fatal.  Do not retry.
			// Close socket to flush out possible garbage.  Do not put back in pool.
			node.closeConnection(conn);
			throw new AerospikeException(e);
		}

		int result = dataBuffer[RESULT_CODE] & 0xFF;

		if (result != 0)
		{
			throw new AerospikeException(result);
		}
	}

	private void executeQuery(Cluster cluster, AdminPolicy policy) throws AerospikeException {
		writeSize();
		Node node = cluster.getRandomNode();
		int timeout = (policy == null) ? 1000 : policy.timeout;
		int status = 0;
		Connection conn = node.getConnection(timeout);

		try {
			conn.write(dataBuffer, dataOffset);
			status = readBlocks(conn);
			node.putConnection(conn);
		}
		catch (Exception e) {
			// Garbage may be in socket.  Do not put back into pool.
			node.closeConnection(conn);
			throw new AerospikeException(e);
		}

		if (status != QUERY_END && status > 0) {
			throw new AerospikeException(status, "Query failed.");
		}
	}

	private int readBlocks(Connection conn) throws IOException {
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
				conn.updateLastUsed();
				status = parseBlock(receiveSize);
			}
		}
		return status;
	}

	public static String hashPassword(String password) {
		return BCrypt.hashpw(password, "$2a$10$7EqJtq98hPqEX7fNZaFWoO");
	}

	int parseBlock(int receiveSize) {
		return QUERY_END;
	}

	public static final class UserCommand extends AdminCommand {
		private final List<User> list;

		public UserCommand(int capacity) {
			list = new ArrayList<User>(capacity);
		}

		public User queryUser(Cluster cluster, AdminPolicy policy, String user) throws AerospikeException {
			super.writeHeader(QUERY_USERS, 1);
			super.writeField(USER, user);
			super.executeQuery(cluster, policy);
			return (list.size() > 0) ? list.get(0) : null;
		}

		public List<User> queryUsers(Cluster cluster, AdminPolicy policy) throws AerospikeException {
			super.writeHeader(QUERY_USERS, 0);
			super.executeQuery(cluster, policy);
			return list;
		}

		@Override
		int parseBlock(int receiveSize)
		{
			super.dataOffset = 0;

			while (super.dataOffset < receiveSize) {
				int resultCode = super.dataBuffer[super.dataOffset + 1] & 0xFF;

				if (resultCode != 0) {
					return resultCode;
				}

				User user = new User();
				int fieldCount = super.dataBuffer[super.dataOffset + 3] & 0xFF;
				super.dataOffset += HEADER_REMAINING;

				for (int i = 0; i < fieldCount; i++) {
					int len = Buffer.bytesToInt(super.dataBuffer, super.dataOffset);
					super.dataOffset += 4;
					int id = super.dataBuffer[super.dataOffset++] & 0xFF;
					len--;

					if (id == USER) {
						user.name = Buffer.utf8ToString(super.dataBuffer, super.dataOffset, len);
						super.dataOffset += len;
					}
					else if (id == ROLES) {
						parseRoles(user);
					}
					else {
						super.dataOffset += len;
					}
				}

				if (user.name == null && user.roles == null) {
					continue;
				}

				if (user.roles == null) {
					user.roles = new ArrayList<String>(0);
				}
				list.add(user);
			}
			return 0;
		}

		private void parseRoles(User user) {
			int size = super.dataBuffer[super.dataOffset++] & 0xFF;
			user.roles = new ArrayList<String>(size);

			for (int i = 0; i < size; i++) {
				int len = super.dataBuffer[super.dataOffset++] & 0xFF;
				String role = Buffer.utf8ToString(super.dataBuffer, super.dataOffset, len);
				super.dataOffset += len;
				user.roles.add(role);
			}
		}
	}

	public static final class RoleCommand extends AdminCommand {
		private final List<Role> list;

		public RoleCommand(int capacity) {
			list = new ArrayList<Role>(capacity);
		}

		public Role queryRole(Cluster cluster, AdminPolicy policy, String roleName) throws AerospikeException {
			super.writeHeader(QUERY_ROLES, 1);
			super.writeField(ROLE, roleName);
			super.executeQuery(cluster, policy);
			return (list.size() > 0) ? list.get(0) : null;
		}

		public List<Role> queryRoles(Cluster cluster, AdminPolicy policy) throws AerospikeException {
			super.writeHeader(QUERY_ROLES, 0);
			super.executeQuery(cluster, policy);
			return list;
		}

		@Override
		int parseBlock(int receiveSize)
		{
			super.dataOffset = 0;

			while (super.dataOffset < receiveSize) {
				int resultCode = super.dataBuffer[super.dataOffset + 1] & 0xFF;

				if (resultCode != 0) {
					return resultCode;
				}

				Role role = new Role();
				int fieldCount = super.dataBuffer[super.dataOffset + 3] & 0xFF;
				super.dataOffset += HEADER_REMAINING;

				for (int i = 0; i < fieldCount; i++) {
					int len = Buffer.bytesToInt(super.dataBuffer, super.dataOffset);
					super.dataOffset += 4;
					int id = super.dataBuffer[super.dataOffset++] & 0xFF;
					len--;

					if (id == ROLE) {
						role.name = Buffer.utf8ToString(super.dataBuffer, super.dataOffset, len);
						super.dataOffset += len;
					}
					else if (id == PRIVILEGES) {
						parsePrivileges(role);
					}
					else if (id == WHITELIST) {
						role.whitelist = parseWhitelist(len);
					}
					else {
						super.dataOffset += len;
					}
				}

				if (role.name == null) {
					throw new AerospikeException(ResultCode.INVALID_ROLE);
				}

				if (role.privileges == null) {
					role.privileges = new ArrayList<Privilege>(0);
				}

				if (role.whitelist == null) {
					role.whitelist = new ArrayList<String>(0);
				}

				list.add(role);
			}
			return 0;
		}

		private void parsePrivileges(Role role) {
			int size = super.dataBuffer[super.dataOffset++] & 0xFF;
			role.privileges = new ArrayList<Privilege>(size);

			for (int i = 0; i < size; i++) {
				Privilege priv = new Privilege();
				priv.code = PrivilegeCode.fromId(super.dataBuffer[super.dataOffset++] & 0xFF);

				if (priv.code.canScope()) {
					int len = super.dataBuffer[super.dataOffset++] & 0xFF;
					priv.namespace = Buffer.utf8ToString(super.dataBuffer, super.dataOffset, len);
					super.dataOffset += len;

					len = super.dataBuffer[super.dataOffset++] & 0xFF;
					priv.setName = Buffer.utf8ToString(super.dataBuffer, super.dataOffset, len);
					super.dataOffset += len;
				}
				role.privileges.add(priv);
			}
		}

		private List<String> parseWhitelist(int len) {
			ArrayList<String> list = new ArrayList<String>();
			StringBuilder sb = new StringBuilder(256);
			int begin = super.dataOffset;
			int max = super.dataOffset + len;
			int l;

			while (super.dataOffset < max) {
				if (super.dataBuffer[super.dataOffset] == ',') {
					l = super.dataOffset - begin;

					if (l > 0) {
						String s = Buffer.utf8ToString(super.dataBuffer, begin, l, sb);
						list.add(s);
					}
					begin = ++super.dataOffset;
				}
				else {
					super.dataOffset++;
				}
			}
			l = super.dataOffset - begin;

			if (l > 0) {
				String s = Buffer.utf8ToString(super.dataBuffer, begin, l, sb);
				list.add(s);
			}
			return list;
		}
	}
}
