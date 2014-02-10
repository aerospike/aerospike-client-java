/*******************************************************************************
 * Copyright 2012-2014 by Aerospike.
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to
 * deal in the Software without restriction, including without limitation the
 * rights to use, copy, modify, merge, publish, distribute, sublicense, and/or
 * sell copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
 * FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS
 * IN THE SOFTWARE.
 ******************************************************************************/
package com.aerospike.client;

/**
 * Host name/port of database server. 
 */
public final class Host {
	/**
	 * Host name or IP address of database server.
	 */
	public final String name;
	
	/**
	 * Port of database server.
	 */
	public final int port;
	
	/**
	 * Initialize host.
	 */
	public Host(String name, int port) {
		this.name = name;
		this.port = port;
	}

	@Override
	public String toString() {
		return name + ':' + port;
	}
	
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = prime + name.hashCode();
		return prime * result + port;
	}

	@Override
	public boolean equals(Object obj) {
		Host other = (Host) obj;
		return this.name.equals(other.name) && this.port == other.port;
	}
}
