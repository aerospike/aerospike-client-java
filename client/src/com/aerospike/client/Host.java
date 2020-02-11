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

import java.util.ArrayList;
import java.util.List;

/**
 * Host name/port of database server.
 */
public final class Host {
	/**
	 * Host name or IP address of database server.
	 */
	public final String name;

	/**
	 * TLS certificate name used for secure connections.
	 */
	public final String tlsName;

	/**
	 * Port of database server.
	 */
	public final int port;

	/**
	 * Initialize host.
	 */
	public Host(String name, int port) {
		this.name = name;
		this.tlsName = null;
		this.port = port;
	}

	/**
	 * Initialize host.
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
		// Ignore tlsName in default equality comparison.
		if (obj == null) {
			return false;
		}
		Host other = (Host) obj;
		return this.name.equals(other.name) && this.port == other.port;
	}

	/**
	 * Parse command-line hosts from string format: hostname1[:tlsname1][:port1],...
	 * <p>
	 * Hostname may also be an IP address in the following formats.
	 * <ul>
	 * <li>IPv4: xxx.xxx.xxx.xxx</li>
	 * <li>IPv6: [xxxx:xxxx:xxxx:xxxx:xxxx:xxxx:xxxx:xxxx]</li>
	 * <li>IPv6: [xxxx::xxxx]</li>
	 * </ul>
	 * IPv6 addresses must be enclosed by brackets.
	 * tlsname and port are optional.
	 */
	public static Host[] parseHosts(String str, int defaultPort) {
		try {
			return new HostParser(str).parseHosts(defaultPort);
		}
		catch (Exception e) {
			throw new AerospikeException("Invalid hosts string: " + str);
		}
	}

	/**
	 * Parse server service hosts from string format: hostname1:port1,...
	 * <p>
	 * Hostname may also be an IP address in the following formats.
	 * <ul>
	 * <li>IPv4: xxx.xxx.xxx.xxx</li>
	 * <li>IPv6: [xxxx:xxxx:xxxx:xxxx:xxxx:xxxx:xxxx:xxxx]</li>
	 * <li>IPv6: [xxxx::xxxx]</li>
	 * </ul>
	 * IPv6 addresses must be enclosed by brackets.
	 */
	public static List<Host> parseServiceHosts(String str) {
		try {
			return new HostParser(str).parseServiceHosts();
		}
		catch (Exception e) {
			throw new AerospikeException("Invalid service hosts string: " + str);
		}
	}

	private static class HostParser {
		private final String str;
		private int offset;
		private int length;
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
