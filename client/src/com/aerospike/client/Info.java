/*
 * Copyright 2012-2025 Aerospike, Inc.
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
package com.aerospike.client;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.aerospike.client.cluster.Connection;
import com.aerospike.client.cluster.Node;
import com.aerospike.client.command.Buffer;
import com.aerospike.client.policy.InfoPolicy;
import com.aerospike.client.util.Crypto;

/**
 * Access server's info monitoring protocol.
 * <p>
 * The info protocol is a name/value pair based system, where an individual
 * database server node is queried to determine its configuration and status.
 * The list of supported names can be found at:
 * <p>
 * <a href="https://www.aerospike.com/docs/reference/info/index.html">https://www.aerospike.com/docs/reference/info/index.html</a>
 */
public class Info {
	//-------------------------------------------------------
	// Static variables.
	//-------------------------------------------------------

	private static final int DEFAULT_TIMEOUT = 1000;

	//-------------------------------------------------------
	// Get Info via Node
	//-------------------------------------------------------

	/**
	 * Get one info value by name from the specified database server node.
	 * This method supports user authentication.
	 *
	 * @param node		server node
	 * @param name		name of variable to retrieve
	 */
	public static String request(Node node, String name) throws AerospikeException {
		Connection conn = node.getConnection(DEFAULT_TIMEOUT);

		try	{
			Info info = new Info(node, conn, name);
			String response = info.parseSingleResponse(name);
			node.putConnection(conn);
			return response;
		}
		catch (Throwable e) {
			node.closeConnection(conn);
			throw e;
		}
	}

	/**
	 * Get one info value by name from the specified database server node.
	 * This method supports user authentication.
	 *
	 * @param policy	info command configuration parameters, pass in null for defaults
	 * @param node		server node
	 * @param name		name of variable to retrieve
	 */
	public static String request(InfoPolicy policy, Node node, String name) throws AerospikeException {
		int timeout = (policy == null) ? DEFAULT_TIMEOUT : policy.timeout;
		Connection conn = node.getConnection(timeout);

		try {
			Info info = new Info(node, conn, name);
			String result = info.parseSingleResponse(name);
			node.putConnection(conn);
			return result;
		}
		catch (Throwable e) {
			node.closeConnection(conn);
			throw e;
		}
	}

	/**
	 * Get many info values by name from the specified database server node.
	 * This method supports user authentication.
	 *
	 * @param policy	info command configuration parameters, pass in null for defaults
	 * @param node		server node
	 * @param names		names of variables to retrieve
	 */
	public static Map<String,String> request(InfoPolicy policy, Node node, String... names) throws AerospikeException {
		int timeout = (policy == null) ? DEFAULT_TIMEOUT : policy.timeout;
		Connection conn = node.getConnection(timeout);

		try {
			Info info = new Info(node, conn, names);
			Map<String,String> result = info.parseMultiResponse();
			node.putConnection(conn);
			return result;
		}
		catch (Throwable e) {
			node.closeConnection(conn);
			throw e;
		}
	}

	/**
	 * Get default info values from the specified database server node.
	 * This method supports user authentication.
	 *
	 * @param policy	info command configuration parameters, pass in null for defaults
	 * @param node		server node
	 */
	@Deprecated(since="8.1.0")
	public static Map<String,String> request(InfoPolicy policy, Node node) throws AerospikeException {
		int timeout = (policy == null) ? DEFAULT_TIMEOUT : policy.timeout;
		Connection conn = node.getConnection(timeout);

		try {
			Info info = new Info(node, conn);
			Map<String,String> result = info.parseMultiResponse();
			node.putConnection(conn);
			return result;
		}
		catch (Throwable e) {
			node.closeConnection(conn);
			throw e;
		}
	}

	//-------------------------------------------------------
	// Get Info via Host Name and Port
	//-------------------------------------------------------

	/**
	 * Get one info value by name from the specified database server node, using
	 * host name and port.
	 * This method does not support user authentication.
	 *
	 * @param hostname				host name
	 * @param port					host port
	 * @param name					name of value to retrieve
	 * @return						info value
	 */
	public static String request(String hostname, int port, String name)
		throws AerospikeException {
		return request(new InetSocketAddress(hostname, port), name);
	}

	/**
	 * Get many info values by name from the specified database server node,
	 * using host name and port.
	 * This method does not support user authentication.
	 *
	 * @param hostname				host name
	 * @param port					host port
	 * @param names					names of values to retrieve
	 * @return						info name/value pairs
	 */
	public static HashMap<String,String> request(String hostname, int port, String... names)
		throws AerospikeException {
		return request(new InetSocketAddress(hostname, port), names);
	}

	/**
	 * Get default info from the specified database server node, using host name and port.
	 * This method does not support user authentication.
	 *
	 * @param hostname				host name
	 * @param port					host port
	 * @return						info name/value pairs
	 */
	public static HashMap<String,String> request(String hostname, int port)
		throws AerospikeException {
		return request(new InetSocketAddress(hostname, port));
	}

	//-------------------------------------------------------
	// Get Info via Socket Address
	//-------------------------------------------------------

	/**
	 * Get one info value by name from the specified database server node.
	 * This method does not support TLS connections nor user authentication.
	 *
	 * @param socketAddress			<code>InetSocketAddress</code> of server node
	 * @param name					name of value to retrieve
	 * @return						info value
	 */
	public static String request(InetSocketAddress socketAddress, String name)
		throws AerospikeException {
		Connection conn = new Connection(socketAddress, DEFAULT_TIMEOUT);

		try {
			return request(conn, name);
		}
		finally {
			conn.close();
		}
	}

	/**
	 * Get many info values by name from the specified database server node.
	 * This method does not support TLS connections nor user authentication.
	 *
	 * @param socketAddress			<code>InetSocketAddress</code> of server node
	 * @param names					names of values to retrieve
	 * @return						info name/value pairs
	 */
	public static HashMap<String,String> request(InetSocketAddress socketAddress, String... names)
		throws AerospikeException {
		Connection conn = new Connection(socketAddress, DEFAULT_TIMEOUT);

		try {
			return request(conn, names);
		}
		finally {
			conn.close();
		}
	}

	/**
	 * Get all the default info from the specified database server node.
	 * This method does not support TLS connections nor user authentication.
	 *
	 * @param socketAddress			<code>InetSocketAddress</code> of server node
	 * @return						info name/value pairs
	 */
	public static HashMap<String,String> request(InetSocketAddress socketAddress)
		throws AerospikeException {
		Connection conn = new Connection(socketAddress, DEFAULT_TIMEOUT);

		try {
			return request(conn);
		}
		finally {
			conn.close();
		}
	}

	//-------------------------------------------------------
	// Get Info via Connection
	//-------------------------------------------------------

	/**
	 * Get one info value by name from the specified database server node.
	 *
	 * @param conn					socket connection to server node
	 * @param name					name of value to retrieve
	 * @return						info value
	 */
	public static String request(Connection conn, String name)
		throws AerospikeException {

		Info info = new Info(conn, name);
		return info.parseSingleResponse(name);
	}

	/**
	 * Get many info values by name from the specified database server node.
	 *
	 * @param conn					socket connection to server node
	 * @param names					names of values to retrieve
	 * @return						info name/value pairs
	 */
	public static HashMap<String,String> request(Connection conn, String... names)
		throws AerospikeException {

		Info info = new Info(conn, names);
		return info.parseMultiResponse();
	}

	/**
	 * Get many info values by name from the specified database server node.
	 *
	 * @param conn					socket connection to server node
	 * @param names					names of values to retrieve
	 * @return						info name/value pairs
	 */
	public static HashMap<String,String> request(Connection conn, List<String> names)
		throws AerospikeException {

		Info info = new Info(conn, names);
		return info.parseMultiResponse();
	}

	/**
	 * Get all the default info from the specified database server node.
	 *
	 * @param conn					socket connection to server node
	 * @return						info name/value pairs
	 */
	public static HashMap<String,String> request(Connection conn)
		throws AerospikeException {
		Info info = new Info(conn);
		return info.parseMultiResponse();
	}

	//-------------------------------------------------------
	// Parse Methods
	//-------------------------------------------------------

	/**
	 * Parse info response string and return the result code for info commands
	 * that only return OK or an error string. Info commands that return other
	 * data are not handled by this method.
	 */
	public static int parseResultCode(String response) {
		if (response.regionMatches(true, 0, "OK", 0, 2)) {
			return ResultCode.OK;
		}

		Info.Error error = new Info.Error(response);

		if (error.code >= 0) {
			// Server errors return error code.
			return error.code;
		}
		else {
			// Client errors result in a exception.
			throw new AerospikeException(error.code, "Unrecognized info response: " + response);
		}
	}

	//-------------------------------------------------------
	// Member variables.
	//-------------------------------------------------------

	public byte[] buffer;
	public int length;
	public int offset;

	//-------------------------------------------------------
	// Constructor
	//-------------------------------------------------------

	/**
	 * Send single command to server and store results.
	 * This constructor is used internally.
	 * The static request methods should be used instead.
	 *
	 * @param conn			connection to server node
	 * @param command		command sent to server
	 */
	public Info(Connection conn, String command) throws AerospikeException {
		int size = Buffer.estimateSizeUtf8Quick(command) + 9;

		buffer = new byte[size];
		offset = 8; // Skip size field.

		// The command format is: <name1>\n<name2>\n...
		offset += Buffer.stringToUtf8(command, buffer, offset);
		buffer[offset++] = '\n';

		sendCommand(null, conn);
	}

	/**
	 * Send single command to server and store results.
	 * This constructor is used internally.
	 * The static request methods should be used instead.
	 *
	 * @param node			server node
	 * @param conn			connection to server node
	 * @param command		command sent to server
	 */
	public Info(Node node, Connection conn, String command) throws AerospikeException {
		int size = Buffer.estimateSizeUtf8Quick(command) + 9;

		buffer = new byte[size];
		offset = 8; // Skip size field.

		// The command format is: <name1>\n<name2>\n...
		offset += Buffer.stringToUtf8(command, buffer, offset);
		buffer[offset++] = '\n';

		sendCommand(node, conn);
	}

	/**
	 * Send multiple commands to server and store results.
	 * This constructor is used internally.
	 * The static request methods should be used instead.
	 *
	 * @param conn			connection to server node
	 * @param commands		commands sent to server
	 */
	public Info(Connection conn, String... commands) throws AerospikeException {
		int size = 8;

		for (String command : commands) {
			size += Buffer.estimateSizeUtf8Quick(command) + 1;
		}

		buffer = new byte[size];
		offset = 8; // Skip size field.

		// The command format is: <name1>\n<name2>\n...
		for (String command : commands) {
			offset += Buffer.stringToUtf8(command, buffer, offset);
			buffer[offset++] = '\n';
		}
		sendCommand(null, conn);
	}

	/**
	 * Send multiple commands to server and store results.
	 * This constructor is used internally.
	 * The static request methods should be used instead.
	 *
	 * @param node			server node
	 * @param conn			connection to server node
	 * @param commands		commands sent to server
	 */
	public Info(Node node, Connection conn, String... commands) throws AerospikeException {
		int size = 8;

		for (String command : commands) {
			size += Buffer.estimateSizeUtf8Quick(command) + 1;
		}

		buffer = new byte[size];
		offset = 8; // Skip size field.

		// The command format is: <name1>\n<name2>\n...
		for (String command : commands) {
			offset += Buffer.stringToUtf8(command, buffer, offset);
			buffer[offset++] = '\n';
		}
		sendCommand(node, conn);
	}

	/**
	 * Send multiple commands to server and store results.
	 * This constructor is used internally.
	 * The static request methods should be used instead.
	 *
	 * @param conn     connection to server node
	 * @param commands commands sent to server
	 */
	public Info(Connection conn, List<String> commands) throws AerospikeException {
		int size = 8;

		for (String command : commands) {
			size += Buffer.estimateSizeUtf8Quick(command) + 1;
		}

		buffer = new byte[size];
		offset = 8; // Skip size field.

		// The command format is: <name1>\n<name2>\n...
		for (String command : commands) {
			offset += Buffer.stringToUtf8(command, buffer, offset);
			buffer[offset++] = '\n';
		}
		sendCommand(null, conn);
	}

	/**
	 * Send default empty command to server and store results.
	 * This constructor is used internally.
	 * The static request methods should be used instead.
	 *
	 * @param conn			connection to server node
	 */
	public Info(Connection conn) throws AerospikeException {
		buffer = new byte[8];
		offset = 8;  // Skip size field.
		sendCommand(null, conn);
	}

	/**
	 * Send default empty command to server and store results.
	 * This constructor is used internally.
	 * The static request methods should be used instead.
	 *
	 * @param node			server node
	 * @param conn			connection to server node
	 */
	public Info(Node node, Connection conn) throws AerospikeException {
		buffer = new byte[8];
		offset = 8;  // Skip size field.
		sendCommand(node, conn);
	}

	/**
	 * Internal constructor.  Do not use.
	 */
	public Info(byte[] buffer, int length) {
		this.buffer = buffer;
		this.length = length;
	}

	/**
	 * Issue request and set results buffer. This method is used internally.
	 * The static request methods should be used instead.
	 */
	private void sendCommand(Node node, Connection conn) {
		try {
			long bytesIn = 0;

			// Write size field.
			long size = (offset - 8L) | (2L << 56) | (1L << 48);
			Buffer.longToBytes(size, buffer, 0);

			// Write.
			conn.write(buffer, offset);
			if (node != null && node.areMetricsEnabled()) {
				node.addBytesOut(null, offset);
			}

			// Read - reuse input buffer.
			conn.readFully(buffer, 8);
			bytesIn += 8;

			size = Buffer.bytesToLong(buffer, 0);
			length = (int)(size & 0xFFFFFFFFFFFFL);

			if (length > buffer.length) {
				buffer = new byte[length];
			}
			conn.readFully(buffer, length);
			bytesIn += length;
			if (node != null && node.areMetricsEnabled()) {
				node.addBytesIn(null, bytesIn);
			}
			conn.updateLastUsed();
			offset = 0;
		}
		catch (IOException ioe) {
			throw new AerospikeException.Connection(ioe);
		}
	}

	private String parseSingleResponse(String name) {
		// Convert the UTF8 byte array into a string.
		String response = Buffer.utf8ToString(buffer, 0, length);

		if (response.startsWith(name)) {
			if (response.length() > name.length() + 1) {
				// Remove field name, tab and trailing newline from response.
				// This is faster than calling parseMultiResponse()
				return response.substring(name.length() + 1, response.length() - 1);
			}
			else {
				return null;
			}
		}
		else {
			throw new AerospikeException.Parse("Info response does not include: " + name);
		}
	}

	public HashMap<String,String> parseMultiResponse() throws AerospikeException {
		HashMap<String, String> responses = new HashMap<String,String>();
		int offset = 0;
		int begin = 0;

		while (offset < length) {
			byte b = buffer[offset];

			if (b == '\t') {
				String name = Buffer.utf8ToString(buffer, begin, offset - begin);
				offset++;
				checkError();
				begin = offset;

				// Parse field value.
				while (offset < length) {
					if (buffer[offset] == '\n') {
						break;
					}
					offset++;
				}

				if (offset > begin) {
					String value = Buffer.utf8ToString(buffer, begin, offset - begin);
					responses.put(name, value);
				}
				else {
					responses.put(name, null);
				}
				begin = ++offset;
			}
			else if (b == '\n') {
				if (offset > begin) {
					String name = Buffer.utf8ToString(buffer, begin, offset - begin);
					responses.put(name, null);
				}
				begin = ++offset;
			}
			else {
				offset++;
			}
		}

		if (offset > begin) {
			String name = Buffer.utf8ToString(buffer, begin, offset - begin);
			responses.put(name, null);
		}
		return responses;
	}

	/**
	 * Parse request name, verify the name is expected and check for error message.
	 */
	public void parseName(String name) {
		int begin = offset;

		while (offset < length) {
			if (buffer[offset] == '\t') {
				String s = Buffer.utf8ToString(buffer, begin, offset - begin).trim();

				if (name.equals(s)) {
					offset++;

					// Check for error message.
					checkError();
					return;
				}
				break;
			}
			offset++;
		}
		throw new AerospikeException.Parse("Failed to find " + name);
	}

	/**
	 * Check if the info command returned an error.
	 * If so, include the error code and string in the exception.
	 */
	private void checkError() {
		// Error format: ERROR:[<code>:][<message>][\n]
		if (offset + 4 >= length) {
			return; // Error did not occur.
		}

		if (!(buffer[offset] == 'E' && buffer[offset + 1] == 'R' && buffer[offset + 2] == 'R' &&
			  buffer[offset + 3] == 'O' && buffer[offset + 4] == 'R')) {
			return; // Error did not occur.
		}

		// Parse error.
		offset += 5;
		skipDelimiter(':');

		int begin = offset;
		int code = parseInt();

		if (code == 0) {
			code = ResultCode.SERVER_ERROR;
		}

		if (offset > begin) {
			skipDelimiter(':');
		}
		else if (buffer[offset] == ':') {
			offset++;
		}

		String message = parseString('\n');

		throw new AerospikeException(code, message);
	}

	/**
	 * Return single value from response buffer.
	 */
	public String getValue() {
		//Log.debug("Response=" + Buffer.utf8ToString(buffer, offset, length) + " length=" + length + " offset=" + offset);
		skipToValue();
		return Buffer.utf8ToString(buffer, offset, length - offset - 1);
	}

	/**
	 * Parse response in name/value pair format:
	 * <p>
	 * {@code <command>\t<name1>=<value1>;<name2>=<value2>;...\n}
	 *
	 * @return				parser for name/value pairs
	 */
	public NameValueParser getNameValueParser() {
		skipToValue();
		return new NameValueParser();
	}

	public void skipToValue() {
		// Skip past command.
		while (offset < length) {
			byte b = buffer[offset];

			if (b == '\t') {
				offset++;
				break;
			}

			if (b == '\n') {
				break;
			}
			offset++;
		}
	}

	/**
	 * Find next delimeter and skip over it.
	 */
	public void skipDelimiter(char stop) {
		while (offset < length) {
			byte b = buffer[offset++];

			if (b == stop) {
				break;
			}
		}
	}

	/**
	 * Convert UTF8 numeric digits to an integer.  Negative integers are not supported.
	 * Input format: 1234
	 */
	public int parseInt() {
		int begin = offset;
		int end = offset;
		byte b;

		// Skip to end of integer.
		while (offset < length) {
			b = buffer[offset];

			if (b < 48 || b > 57) {
				end = offset;
				break;
			}
			offset++;
		}

		// Convert digits into an integer.
		return Buffer.utf8DigitsToInt(buffer, begin, end);
	}

	public String parseString(char stop) {
		int begin = offset;
		byte b;

		while (offset < length) {
			b = buffer[offset];

			if (b == stop) {
				break;
			}
			offset++;
		}
		return Buffer.utf8ToString(buffer, begin, offset - begin);
	}

	public String parseString(char stop1, char stop2, char stop3) {
		int begin = offset;
		byte b;

		while (offset < length) {
			b = buffer[offset];

			if (b == stop1 || b == stop2 || b == stop3) {
				break;
			}
			offset++;
		}
		return Buffer.utf8ToString(buffer, begin, offset - begin);
	}

	public void expect(char expected) {
		if (expected != buffer[offset]) {
			throw new AerospikeException.Parse("Expected " + expected + " Received: " + (char)(buffer[offset] & 0xFF));
		}
		offset++;
	}

	public String getTruncatedResponse() {
		int max = (length > 200) ? 200 : length;
		return Buffer.utf8ToString(buffer, 0, max);
	}

	/**
	 * Parser for responses in name/value pair format:
	 * <p>
	 * {@code <command>\t<name1>=<value1>;<name2>=<value2>;...\n}
	 */
	public class NameValueParser {
		private int nameBegin;
		private int nameEnd;
		private int valueBegin;
		private int valueEnd;

		/**
		 * Set pointers to next name/value pair.
		 *
		 * @return		true if next name/value pair exists; false if at end
		 */
		public boolean next() {
			nameBegin = offset;

			while (offset < length) {
				byte b = buffer[offset];

				if (b == '=') {
					if (offset <= nameBegin) {
						return false;
					}
					nameEnd = offset;
					parseValue();
					return true;
				}

				if (b == '\n') {
					break;
				}
				offset++;
			}
			nameEnd = offset;
			valueBegin = offset;
			valueEnd = offset;
			return offset > nameBegin;
		}

		private void parseValue() {
			valueBegin = ++offset;

			while (offset < length) {
				byte b = buffer[offset];

				if (b == ';') {
					valueEnd = offset++;
					return;
				}

				if (b == '\n') {
					break;
				}
				offset++;
			}
			valueEnd = offset;
		}

		/**
		 * Get name.
		 */
		public String getName() {
			int len = nameEnd - nameBegin;
			return Buffer.utf8ToString(buffer, nameBegin, len);
		}

		/**
		 * Get value.
		 */
		public String getValue() {
			int len = valueEnd - valueBegin;

			if (len <= 0) {
				return null;
			}
			return Buffer.utf8ToString(buffer, valueBegin, len);
		}

		/**
		 * Get Base64 string value.
		 */
		public String getStringBase64() {
			int len = valueEnd - valueBegin;

			if (len <= 0) {
				return null;
			}
			byte[] bytes = Crypto.decodeBase64(buffer, valueBegin, len);
			return Buffer.utf8ToString(bytes, 0, bytes.length);
		}
	}

	/**
	 * Info command error response.
	 */
	public static class Error {
		public final int code;
		public final String message;

		/**
		 * Parse info command response into code and message.
		 * If the response is not a recognized error format, the code is set to
		 * {@link ResultCode#CLIENT_ERROR} and the message is set to the full
		 * response string.
		 */
		public Error(String response) {
			// Error format: ERROR|FAIL[:<code>][:<message>]
			int rc = ResultCode.CLIENT_ERROR;
			String msg = response;

			try {
				String[] list = response.split(":");
				String s = list[0];

				if (s.regionMatches(true, 0, "FAIL", 0, 4) ||
					s.regionMatches(true, 0, "ERROR", 0, 5)) {

					if (list.length >= 3) {
						msg = list[2].trim();
						s = list[1].trim();

						if (! s.isEmpty()) {
							rc = Integer.parseInt(s);
						}
					}
					else if (list.length == 2) {
						s = list[1].trim();

						if (! s.isEmpty()) {
							try {
								rc = Integer.parseInt(s);
							}
							catch (Throwable t) {
								// Some error strings omit the code and just have a message.
								msg = s;
							}
						}
					}
				}
			}
			catch (Throwable t) {
			}
			finally {
				this.code = rc;
				this.message = msg;
			}
		}
	}
}
