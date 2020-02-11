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
import com.aerospike.client.util.ThreadLocalData;

/**
 * Access server's info monitoring protocol.
 * <p>
 * The info protocol is a name/value pair based system, where an individual
 * database server node is queried to determine its configuration and status.
 * The list of supported names can be found at:
 * <p>
 * <a href="https://www.aerospike.com/docs/reference/info/index.html">https://www.aerospike.com/docs/reference/info/index.html</a>
 * <p>
 *
 */
public final class Info {
	//-------------------------------------------------------
	// Static variables.
	//-------------------------------------------------------

	private static final int DEFAULT_TIMEOUT = 1000;

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
		buffer = ThreadLocalData.getBuffer();

		// If conservative estimate may be exceeded, get exact estimate
		// to preserve memory and resize buffer.
		if ((command.length() * 2 + 9) > buffer.length) {
			offset = Buffer.estimateSizeUtf8(command) + 9;
			resizeBuffer(offset);
		}
		offset = 8; // Skip size field.

		// The command format is: <name1>\n<name2>\n...
		offset += Buffer.stringToUtf8(command, buffer, offset);
		buffer[offset++] = '\n';

		sendCommand(conn);
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
		buffer = ThreadLocalData.getBuffer();

		// First, do quick conservative buffer size estimate.
		offset = 8;

		for (String command : commands) {
			offset += command.length() * 2 + 1;
		}

		// If conservative estimate may be exceeded, get exact estimate
		// to preserve memory and resize buffer.
		if (offset > buffer.length) {
			offset = 8;

			for (String command : commands) {
				offset += Buffer.estimateSizeUtf8(command) + 1;
			}
			resizeBuffer(offset);
		}
		offset = 8; // Skip size field.

		// The command format is: <name1>\n<name2>\n...
		for (String command : commands) {
			offset += Buffer.stringToUtf8(command, buffer, offset);
			buffer[offset++] = '\n';
		}
		sendCommand(conn);
	}

	/**
	 * Send multiple commands to server and store results.
	 * This constructor is used internally.
	 * The static request methods should be used instead.
	 *
	 * @param conn			connection to server node
	 * @param commands		commands sent to server
	 */
	public Info(Connection conn, List<String> commands) throws AerospikeException {
		buffer = ThreadLocalData.getBuffer();

		// First, do quick conservative buffer size estimate.
		offset = 8;

		for (String command : commands) {
			offset += command.length() * 2 + 1;
		}

		// If conservative estimate may be exceeded, get exact estimate
		// to preserve memory and resize buffer.
		if (offset > buffer.length) {
			offset = 8;

			for (String command : commands) {
				offset += Buffer.estimateSizeUtf8(command) + 1;
			}
			resizeBuffer(offset);
		}
		offset = 8; // Skip size field.

		// The command format is: <name1>\n<name2>\n...
		for (String command : commands) {
			offset += Buffer.stringToUtf8(command, buffer, offset);
			buffer[offset++] = '\n';
		}
		sendCommand(conn);
	}

	/**
	 * Send default empty command to server and store results.
	 * This constructor is used internally.
	 * The static request methods should be used instead.
	 *
	 * @param conn			connection to server node
	 */
	public Info(Connection conn) throws AerospikeException {
		buffer = ThreadLocalData.getBuffer();
		offset = 8;  // Skip size field.
		sendCommand(conn);
	}

	/**
	 * Internal constructor.  Do not use.
	 */
	public Info(byte[] buffer, int length) {
		this.buffer = buffer;
		this.length = length;
	}

	/**
	 * Parse response in name/value pair format:
	 * <p>
	 * <command>\t<name1>=<value1>;<name2>=<value2>;...\n
	 *
	 * @return				parser for name/value pairs
	 */
	public NameValueParser getNameValueParser() {
		skipToValue();
		return new NameValueParser();
	}

	/**
	 * Return single value from response buffer.
	 */
	public String getValue() {
		//Log.debug("Response=" + Buffer.utf8ToString(buffer, offset, length) + " length=" + length + " offset=" + offset);
		skipToValue();
		return Buffer.utf8ToString(buffer, offset, length - offset - 1);
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
			String response = Info.request(conn, name);
			node.putConnection(conn);
			return response;
		}
		catch (RuntimeException re) {
			node.closeConnection(conn);
			throw re;
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
			String result = request(conn, name);
			node.putConnection(conn);
			return result;
		}
		catch (RuntimeException re) {
			node.closeConnection(conn);
			throw re;
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
			Map<String,String> result = request(conn, names);
			node.putConnection(conn);
			return result;
		}
		catch (RuntimeException re) {
			node.closeConnection(conn);
			throw re;
		}
	}

	/**
	 * Get default info values from the specified database server node.
	 * This method supports user authentication.
	 *
	 * @param policy	info command configuration parameters, pass in null for defaults
	 * @param node		server node
	 */
	public static Map<String,String> request(InfoPolicy policy, Node node) throws AerospikeException {
		int timeout = (policy == null) ? DEFAULT_TIMEOUT : policy.timeout;
		Connection conn = node.getConnection(timeout);

		try {
			Map<String,String> result = request(conn);
			node.putConnection(conn);
			return result;
		}
		catch (RuntimeException re) {
			node.closeConnection(conn);
			throw re;
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
	// Private methods.
	//-------------------------------------------------------

	/**
	 * Issue request and set results buffer. This method is used internally.
	 * The static request methods should be used instead.
	 */
	private void sendCommand(Connection conn) throws AerospikeException {
		try {
			// Write size field.
			long size = ((long)offset - 8L) | (2L << 56) | (1L << 48);
			Buffer.longToBytes(size, buffer, 0);

			// Write.
			conn.write(buffer, offset);

			// Read - reuse input buffer.
			conn.readFully(buffer, 8);

			size = Buffer.bytesToLong(buffer, 0);
			length = (int)(size & 0xFFFFFFFFFFFFL);
			resizeBuffer(length);
			conn.readFully(buffer, length);
			conn.updateLastUsed();
			offset = 0;
		}
		catch (IOException ioe) {
			throw new AerospikeException.Connection(ioe);
		}
	}

	private void resizeBuffer(int size) {
		if (size > buffer.length) {
			buffer = ThreadLocalData.resizeBuffer(size);
		}
	}

	private String parseSingleResponse(String name) throws AerospikeException {
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

		// Create reusable StringBuilder for performance.
		StringBuilder sb = new StringBuilder(length);

		while (offset < length) {
			byte b = buffer[offset];

			if (b == '\t') {
				String name = Buffer.utf8ToString(buffer, begin, offset - begin, sb);
				checkError(name);
				begin = ++offset;

				// Parse field value.
				while (offset < length) {
					if (buffer[offset] == '\n') {
						break;
					}
					offset++;
				}

				if (offset > begin) {
					String value = Buffer.utf8ToString(buffer, begin, offset - begin, sb);
					responses.put(name, value);
				}
				else {
					responses.put(name, null);
				}
				begin = ++offset;
			}
			else if (b == '\n') {
				if (offset > begin) {
					String name = Buffer.utf8ToString(buffer, begin, offset - begin, sb);
					checkError(name);
					responses.put(name, null);
				}
				begin = ++offset;
			}
			else {
				offset++;
			}
		}

		if (offset > begin) {
			String name = Buffer.utf8ToString(buffer, begin, offset - begin, sb);
			checkError(name);
			responses.put(name, null);
		}
		return responses;
	}

	private void checkError(String str) {
		if (str.startsWith("ERROR:")) {
			// Parse format: ERROR:[<code>:][<message>][\n]
			int len = str.length();

			if (str.charAt(len - 1) == '\n') {
				len--;
			}

			int begin = 6;
			int end = str.indexOf(':', begin);

			if (end < 0) {
				end = len;
			}

			int code = ResultCode.SERVER_ERROR;

			if (end > begin) {
				try {
					code = Integer.parseInt(str.substring(begin, end));
					begin = end + 1;
				}
				catch (Exception e) {
					// Show full string after "ERROR:" if code is invalid.
					// Do not change begin offset.
				}
			}
			else {
				begin = end + 1;
			}
			end = len;

			String message = "";

			if (end > begin) {
				message = str.substring(begin, end);
			}
			throw new AerospikeException(code, message);
		}
	}

	/**
	 * Parser for responses in name/value pair format:
	 * <p>
	 * <command>\t<name1>=<value1>;<name2>=<value2>;...\n
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
}
