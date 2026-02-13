/*
 * Copyright 2012-2023 Aerospike, Inc.
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

import java.util.ArrayList;
import java.util.List;

/**
 * Represents a database server endpoint by host name (or IP) and port, with optional TLS name.
 *
 * <p>Used as seed nodes when constructing {@link com.aerospike.client.AerospikeClient}. Parse from a string
 * using {@link #parseHosts(String, int)} or {@link #parseServiceHosts(String)}.
 *
 * <p>Parse hosts from a string and create a client with the Host array.</p>
 * <pre>{@code
 * Host[] hosts = Host.parseHosts("192.168.1.10:3000,192.168.1.11:3000", 3000);
 * IAerospikeClient client = new AerospikeClient(clientPolicy, hosts);
 * }</pre>
 *
 * @see #parseHosts(String, int)
 * @see #parseServiceHosts(String)
 */
public final class Host {
	/**
	 * Host name or IP address of the server; never {@code null}.
	 */
	public final String name;

	/**
	 * TLS certificate name for secure connections; {@code null} for non-TLS.
	 */
	public final String tlsName;

	/**
	 * Port number of the server.
	 */
	public final int port;

	/**
	 * Constructs a host with the given name and port (no TLS).
	 *
	 * @param name host name or IP address; must not be {@code null}
	 * @param port port number
	 */
	public Host(String name, int port) {
		this.name = name;
		this.tlsName = null;
		this.port = port;
	}

	/**
	 * Constructs a host with the given name, optional TLS name, and port.
	 *
	 * @param name host name or IP address; must not be {@code null}
	 * @param tlsName TLS certificate name for secure connections, or {@code null}
	 * @param port port number
	 */
	public Host(String name, String tlsName, int port) {
		this.name = name;
		this.tlsName = tlsName;
		this.port = port;
	}

	@Override
	public String toString() {
		// Ignore tlsName in string representation.
		// Use space separator to avoid confusion with IPv6 addresses that contain colons.
		return name + ' ' + port;
	}

	@Override
	public int hashCode() {
		// Ignore tlsName in default hash code.
		final int prime = 31;
		int result = prime + name.hashCode();
		return prime * result + port;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj) {
			return true;
		}
		if (obj == null || getClass() != obj.getClass()) {
			return false;
		}
		Host other = (Host) obj;
		// Ignore tlsName in default equality comparison.
		return this.name.equals(other.name) && this.port == other.port;
	}

	/**
	 * Parses a comma-separated list of hosts from the format: hostname1[:tlsname1][:port1],...
	 *
	 * <p>
	 * Hostname may also be an IP address in the following formats.
	 * <ul>
	 * <li>IPv4: xxx.xxx.xxx.xxx</li>
	 * <li>IPv6: [xxxx:xxxx:xxxx:xxxx:xxxx:xxxx:xxxx:xxxx]</li>
	 * <li>IPv6: [xxxx::xxxx]</li>
	 * </ul>
	 * IPv6 addresses must be enclosed by brackets.
	 * TLS name and port are optional; if port is omitted, {@code defaultPort} is used.
	 *
	 * @param str the host string (e.g. "host1:3000,host2:3000"); must not be {@code null}
	 * @param defaultPort port to use when not specified in the string
	 * @return array of parsed hosts
	 * @throws AerospikeException	when the string format is invalid (e.g. missing host:port or bad port).
	 */
	public static Host[] parseHosts(String str, int defaultPort) {
		try {
			return new HostParser(str).parseHosts(defaultPort);
		}
		catch (Throwable e) {
			throw new AerospikeException("Invalid hosts string: " + str);
		}
	}

	/**
	 * Parses a comma-separated list of service hosts from the format: hostname1:port1,...
	 * <p>
	 * Hostname may also be an IP address in the following formats.
	 * <ul>
	 * <li>IPv4: xxx.xxx.xxx.xxx</li>
	 * <li>IPv6: [xxxx:xxxx:xxxx:xxxx:xxxx:xxxx:xxxx:xxxx]</li>
	 * <li>IPv6: [xxxx::xxxx]</li>
	 * </ul>
	 * IPv6 addresses must be enclosed by brackets.
	 *
	 * @param str the service host string (e.g. "host1:3000,host2:3000"); must not be {@code null}
	 * @return list of parsed hosts
	 * @throws AerospikeException	when the string format is invalid (e.g. missing host:port or bad port).
	 */
	public static List<Host> parseServiceHosts(String str) {
		try {
			return new HostParser(str).parseServiceHosts();
		}
		catch (Throwable e) {
			throw new AerospikeException("Invalid service hosts string: " + str);
		}
	}

	private static class HostParser {
		private final String str;
		private int offset;
		private final int length;
		private char c;

		private HostParser(String str) {
			this.str = str;
			this.length = str.length();
			this.offset = 0;
			this.c = ',';
		}

		private Host[] parseHosts(int defaultPort) {
			ArrayList<Host> list = new ArrayList<Host>();
			String hostname;
			String tlsname;
			int port;

			while (offset < length) {
				if (c != ',') {
					throw new RuntimeException();
				}
				hostname = parseHost();
				tlsname = null;
				port = defaultPort;

				if (offset < length && c == ':') {
					String s = parseString();

					if (s.length() > 0) {
						if (Character.isDigit(s.charAt(0))) {
							// Found port.
							port = Integer.parseInt(s);
						}
						else {
							// Found tls name.
							tlsname = s;

							// Parse port.
							s = parseString();

							if (s.length() > 0) {
								port = Integer.parseInt(s);
							}
						}
					}
				}
				list.add(new Host(hostname, tlsname, port));
			}
			return list.toArray(new Host[list.size()]);
		}

		private List<Host> parseServiceHosts() {
			ArrayList<Host> list = new ArrayList<Host>();
			String hostname;
			int port;

			while (offset < length) {
				if (c != ',') {
					throw new RuntimeException();
				}
				hostname = parseHost();

				if (c != ':') {
					throw new RuntimeException();
				}

				String s = parseString();
				port = Integer.parseInt(s);

				list.add(new Host(hostname, port));
			}
			return list;
		}

		private String parseHost() {
			c = str.charAt(offset);

			if (c == '[') {
				// IPv6 addresses are enclosed by brackets.
				int begin = ++offset;

				while (offset < length) {
					c = str.charAt(offset);

					if (c == ']') {
						String s = str.substring(begin, offset++);

						if (offset < length) {
							c = str.charAt(offset++);
						}
						return s;
					}
					offset++;
				}
				throw new RuntimeException("Unterminated bracket");
			}
			else {
				return parseString();
			}
		}

		private String parseString() {
			int begin = offset;

			while (offset < length) {
				c = str.charAt(offset);

				if (c == ':' || c == ',') {
					return str.substring(begin, offset++);
				}
				offset++;
			}
			return str.substring(begin, offset);
		}
	}
}
