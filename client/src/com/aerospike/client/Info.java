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

	//-------------------------------------------------------
	// Constructor
	//-------------------------------------------------------

	/**
	 * Initialize info request buffer. This constructor is used internally.
	 * The static request methods should be used instead.
	 * 
	 * The request format is: <name1>\n<name2>\n...
	 * 
	 * @param request						named values to retrieve
	 * @throws AerospikeException.Serialize if UTF8 encoding fails
	 */
	public Info(String request) throws AerospikeException.Serialize {
		//buffer = new byte[256];
		buffer = ThreadLocalData.getSendBuffer();
		
		int len = Buffer.stringToUtf8(request, buffer, 8);
		long sizeField = (len) | (2L << 56) | (1L << 48);
		Buffer.longToBytes(sizeField, buffer, 0);		
		length = len + 8;
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
	 * @param socket				socket connection to server node
	 * @param name					name of value to retrieve
	 * @return						info value
	 */
	public static String request(Connection conn, String name) 
		throws AerospikeException {		
		try {		
			Info info = new Info(name + '\n');
			info.requestInfo(conn);
			return info.parseSingleResponse(name);
		}
		catch (IOException ioe) {
			throw new AerospikeException(ioe);
		}
	}

	/**
	 * Get many info values by name from the specified database server node.
	 * 
	 * @param socket				socket connection to server node
	 * @param names					names of values to retrieve
	 * @return						info name/value pairs
	 */
	public static HashMap<String,String> request(Connection conn, String... names) 
		throws AerospikeException {		
		try {		
			StringBuilder sb = new StringBuilder(names.length * 20);
	
			for (String name : names) {
				sb.append(name);
				sb.append('\n');
			}
	
			Info info = new Info(sb.toString());
			info.requestInfo(conn);
			return info.parseMultiResponse();
		}
		catch (IOException ioe) {
			throw new AerospikeException(ioe);
		}
	}

	/**
	 * Get all the default info from the specified database server node.
	 * 
	 * @param socket				socket connection to server node
	 * @return						info name/value pairs
	 */
	public static HashMap<String,String> request(Connection conn) 
		throws AerospikeException {		
		try {
			Info info = new Info(null);
			info.requestInfo(conn);
			return info.parseMultiResponse();
		}
		catch (IOException ioe) {
			throw new AerospikeException(ioe);
		}
	}

	/**
	 * Issue request and set results buffer. This method is used internally.
	 * The static request methods should be used instead.
	 * 
	 * @param socket		socket connection to server node
	 * @throws IOException	if socket send or receive fails
	 */
	public void requestInfo(Connection conn) throws IOException {
		// Write.
		OutputStream out = conn.getOutputStream();
		out.write(buffer, 0, length);

		// Read - reuse input buffer.
		InputStream in = conn.getInputStream();
		readFully(in, buffer, 8);
		
		long size = Buffer.bytesToLong(buffer, 0);
		length = (int)(size & 0xFFFFFFFFFFFFL);

		if (length > buffer.length) {
			buffer = ThreadLocalData.resizeSendBuffer(length);
		}
		readFully(in, buffer, length);
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
}
