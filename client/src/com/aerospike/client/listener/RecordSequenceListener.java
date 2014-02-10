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
package com.aerospike.client.listener;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.Key;
import com.aerospike.client.Record;

/**
 * Asynchronous result notifications for batch get and scan commands.
 * The results are sent one record at a time.
 */
public interface RecordSequenceListener {
	/**
	 * This method is called when an asynchronous record is received from the server.
	 * The receive sequence is not ordered.
	 * <p>
	 * The user may throw a 
	 * {@link com.aerospike.client.AerospikeException.QueryTerminated AerospikeException.QueryTerminated} 
	 * exception if the command should be aborted.  If any exception is thrown, parallel command threads
	 * to other nodes will also be terminated and the exception will be propagated back through the
	 * commandFailed() call.
	 * 
	 * @param key					unique record identifier
	 * @param record				record instance, will be null if the key is not found
	 * @throws AerospikeException	if error occurs or scan should be terminated.
	 */
	public void onRecord(Key key, Record record) throws AerospikeException;
	
	/**
	 * This method is called when the asynchronous batch get or scan command completes.
	 */
	public void onSuccess();
	
	/**
	 * This method is called when an asynchronous batch get or scan command fails.
	 * 
	 * @param exception				error that occurred
	 */
	public void onFailure(AerospikeException exception);
}
