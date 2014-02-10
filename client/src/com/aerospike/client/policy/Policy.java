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
package com.aerospike.client.policy;

/**
 * Container object for transaction policy attributes used in all database
 * operation calls.
 */
public class Policy {
	/**
	 * Priority of request relative to other transactions.
	 * Currently, only used for scans.
	 */
	public Priority priority = Priority.DEFAULT;
	
	/**
	 * Transaction timeout in milliseconds.
	 * This timeout is used to set the socket timeout and is also sent to the 
	 * server along with the transaction in the wire protocol.
	 * Default to no timeout (0).
	 */
	public int timeout;
	
	/**
	 * Maximum number of retries before aborting the current transaction.
	 * A retry is attempted when there is a network error other than timeout.  
	 * If maxRetries is exceeded, the abort will occur even if the timeout 
	 * has not yet been exceeded.
	 */
	public int maxRetries = 2;

	/**
	 * Milliseconds to sleep between retries if a transaction fails and the 
	 * timeout was not exceeded.  Enter zero to skip sleep.
	 */
	public int sleepBetweenRetries = 500;
}
