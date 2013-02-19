/*
 * Aerospike Client - Java Library
 *
 * Copyright 2012 by Aerospike, Inc. All rights reserved.
 *
 * Availability of this source code to partners and customers includes
 * redistribution rights covered by individual contract. Please check your
 * contract for exact rights and responsibilities.
 */
package com.aerospike.client;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.util.HashMap;

import com.aerospike.client.cluster.Connection;
import com.aerospike.client.command.Buffer;
import com.aerospike.client.util.ThreadLocalData;

/**
 * Access Citrusleaf's monitoring protocol - the "Info" protocol.
 * <p>
 * The info protocol is a name/value pair based system, where an individual
 * database server node is queried to determine its configuration and status.
 * The list of supported names can be found on the Citrusleaf Wiki under the TCP
 * wire protocol specification.
 */
public final class Info {	
	//-------------------------------------------------------
	// Static variables.
	//-------------------------------------------------------
	
	private static final int DEFAULT_TIMEOUT = 2000;
	
	//-------------------------------------------------------
	// Member variables.
	//-------------------------------------------------------

	private byte[] buffer;
	private int length;
	private int offset;

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
		buffer = ThreadLocalData.getSendBuffer();		
		offset = 8;  // Skip size field.

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
		buffer = ThreadLocalData.getSendBuffer();		
		offset = 8;  // Skip size field.
		
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
		buffer = ThreadLocalData.getSendBuffer();		
		offset = 8;  // Skip size field.		
		sendCommand(conn);
	}

	/**
	 * Parse response in name/value pair format:
	 * <p>
	 * <command>\t<name1>=<value1>;<name2>=<value2>;...\n
	 *     
	 * @return				parser for name/value pairs
	 */
	public NameValueParser getNameValueParser() {
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
		return new NameValueParser();
	}

	//-------------------------------------------------------
	// Get Info via Host Name and Port
	//-------------------------------------------------------
	
	/**
	 * Get one info value by name from the specified database server node, using
	 * host name and port.
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
	// Get Info via Socket Address
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

	/**
	 * Get response buffer. For internal use only.
	 */
	public byte[] getBuffer() {
		return buffer;
	}

	/**
	 * Get response length. For internal use only.
	 */
	public int getLength() {
		return length;
	}

	//-------------------------------------------------------
	// Private methods.
	//-------------------------------------------------------
	
	/**
	 * Issue request and set results buffer. This method is used internally.
	 * The static request methods should be used instead.
	 * 
	 * @param conn			socket connection to server node
	 * @throws IOException	if socket send or receive fails
	 */
	private void sendCommand(Connection conn) throws AerospikeException {		
		try {		
			// Write size field.
			long size = ((long)offset - 8L) | (2L << 56) | (1L << 48);
			Buffer.longToBytes(size, buffer, 0);

			// Write.
			OutputStream out = conn.getOutputStream();
			out.write(buffer, 0, offset);

			// Read - reuse input buffer.
			InputStream in = conn.getInputStream();
			readFully(in, buffer, 8);
			
			size = Buffer.bytesToLong(buffer, 0);
			length = (int)(size & 0xFFFFFFFFFFFFL);

			if (length > buffer.length) {
				buffer = ThreadLocalData.resizeSendBuffer(length);
			}
			readFully(in, buffer, length);		
			offset = 0;
		}
		catch (IOException ioe) {
			throw new AerospikeException(ioe);
		}
	}

	private static void readFully(InputStream in, byte[] buffer, int length) 
		throws IOException {
		int pos = 0;
	
		while (pos < length) {
			int count = in.read(buffer, pos, length - pos);
		    
			if (count < 0)
		    	throw new EOFException();
			
			pos += count;
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

	private HashMap<String,String> parseMultiResponse() throws AerospikeException {
		HashMap<String, String> responses = new HashMap<String,String>();
		int offset = 0;
		int begin = 0;
		
		// Create reusable StringBuilder for performance.
		StringBuilder sb = new StringBuilder(length);
		
		while (offset < length) {
			byte b = buffer[offset];
			
			if (b == '\t') {
				String name = Buffer.utf8ToString(buffer, begin, offset - begin, sb);
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
			responses.put(name, null);
		}
		return responses;
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
	}
}
