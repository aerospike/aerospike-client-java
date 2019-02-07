/*
 * Copyright 2012-2019 Aerospike, Inc.
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
package com.aerospike.client.cluster;

import java.io.EOFException;
import java.io.IOException;
import java.net.SocketTimeoutException;
import java.util.concurrent.TimeUnit;

import com.aerospike.client.command.Buffer;
import com.aerospike.client.command.Command;

public final class ConnectionRecover {
	//private static final AtomicInteger Counter = new AtomicInteger();

	private final Connection conn;
	private final Node node;
	private byte[] headerBuf;
	//private int tranId;
	private int timeoutDelay;
	private int offset;
	private int length;
	private final boolean isSingle;
	private final boolean checkReturnCode;
	private boolean lastGroup;
	private byte state;

	public ConnectionRecover(Connection conn, Node node, int timeoutDelay, Connection.ReadTimeout crt) {
		//tranId = Counter.getAndIncrement();
		//System.out.println("" + tranId + " timeout:" + crt.state + ',' + crt.offset + ',' + crt.length);
		this.conn = conn;
		this.node = node;
		this.timeoutDelay = timeoutDelay;
		this.offset = crt.offset;

		switch (crt.state) {
		case Command.STATE_READ_AUTH_HEADER:
			this.length = 10;
			this.isSingle = true;
			this.checkReturnCode = true;
			this.state = Command.STATE_READ_HEADER;

			if (offset >= length) {
				if (crt.buffer[length - 1] != 0) {
					// Authentication failed.
					//System.out.println("" + tranId + " invalid user/password:");
					abort();
					return;
				}
				length = getSize(crt.buffer) - (offset - 8);
				offset = 0;
				state = Command.STATE_READ_DETAIL;
			}
			else if (offset > 0) {
				copyHeaderBuf(crt.buffer);
			}
			break;

		case Command.STATE_READ_HEADER:
			this.length = crt.isSingle ? 8 : 12;
			this.isSingle = crt.isSingle;
			this.checkReturnCode = false;
			this.state = crt.state;

			if (offset >= length) {
				if (! isSingle) {
					checkLastGroup(crt.buffer);
				}
				length = getSize(crt.buffer) - (offset - 8);
				offset = 0;
				state = Command.STATE_READ_DETAIL;
			}
			else if (offset > 0) {
				copyHeaderBuf(crt.buffer);
			}
			break;

		case Command.STATE_READ_DETAIL:
		default:
			this.length = crt.length;
			this.isSingle = crt.isSingle;
			this.checkReturnCode = false;
			this.state = crt.state;
			break;
		}

		conn.updateLastUsed();

		try {
			conn.setTimeout(1);
		}
		catch (Exception e) {
			//System.out.println("" + tranId + " set timeout failed");
			abort();
		}
	}

	/**
	 * Drain connection.
	 * @return true if draining is complete.
	 */
	public boolean drain(byte[] buf) {
		try {
			if (isSingle) {
				if (state == Command.STATE_READ_HEADER) {
					drainHeader(buf);
				}
				drainDetail(buf);
				recover();
				return true;
			}
			else {
				while (true) {
					if (state == Command.STATE_READ_HEADER) {
						drainHeader(buf);
					}
					drainDetail(buf);

					if (lastGroup) {
						break;
					}
					length = 12;
					offset = 0;
					state = Command.STATE_READ_HEADER;
				}
				recover();
				return true;
			}
		}
		catch (SocketTimeoutException ste) {
			if (System.nanoTime() - conn.getLastUsed() >= TimeUnit.MILLISECONDS.toNanos(timeoutDelay)) {
				// Forcibly close connection.
				//System.out.println("" + tranId + " timeout expired. close connection");
				abort();
				return true;
			}
			// Put back on queue for later draining.
			return false;
		}
		catch (Exception e) {
			// Forcibly close connection.
			//System.out.println("" + tranId + " socket error:");
			//e.printStackTrace();
			abort();
			return true;
		}
	}

	/**
	 * Has connection been recovered or closed.
	 */
	public boolean isComplete() {
		return state == Command.STATE_COMPLETE;
	}

	/**
	 * Close connection.
	 */
	public void abort() {
		node.closeConnection(conn);
		state = Command.STATE_COMPLETE;
	}

	private void recover() {
		//System.out.println("" + tranId + " connection drained");
		node.putConnection(conn);
		state = Command.STATE_COMPLETE;
	}

	private void drainHeader(byte[] buf) throws IOException {
		byte[] b = (offset == 0)? buf : headerBuf;

		while (true) {
			int count = conn.read(b, offset, length - offset);

			if (count < 0) {
				// Connection closed by server.
				throw new EOFException();
			}
			offset += count;

			if (offset >= length) {
				break;
			}

			// Partial read.
			if (b == buf) {
				// Convert to header buf.
				copyHeaderBuf(b);
				b = headerBuf;
			}
		}

		if (checkReturnCode) {
			if (b[length - 1] != 0) {
				// Authentication failed.
				//System.out.println("" + tranId + " invalid user/password:");
				abort();
				return;
			}
		}
		else if (! isSingle) {
			checkLastGroup(b);
		}
		length = getSize(b) - (length - 8);
		offset = 0;
		state = Command.STATE_READ_DETAIL;
	}

	private void drainDetail(byte[] buf) throws IOException {
		while (offset < length) {
			int rem = length - offset;
			int len = (rem <= buf.length)? rem : buf.length;
			int count = conn.read(buf, 0, len);

			if (count < 0) {
				// Connection closed by server.
				throw new EOFException();
			}
			offset += count;
		}
	}

	private void copyHeaderBuf(byte[] buf) {
		if (headerBuf == null) {
			headerBuf = new byte[length];
		}

		for (int i = 0; i < offset; i++) {
			headerBuf[i] = buf[i];
		}
	}

	private int getSize(byte[] buf) {
		long sz = Buffer.bytesToLong(buf, 0);
		return (int)(sz & 0xFFFFFFFFFFFFL);
	}

	private void checkLastGroup(byte[] buf) {
		byte info3 = buf[length - 1];

		if ((info3 & Command.INFO3_LAST) == Command.INFO3_LAST) {
			lastGroup = true;
		}
	}
}
