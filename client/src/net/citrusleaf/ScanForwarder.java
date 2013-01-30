/*
 * Citrusleaf Aerospike Client - Java Library
 *
 * Copyright 2009-2010 by Citrusleaf, Inc. All rights reserved.
 *
 * Availability of this source code to partners and customers includes
 * redistribution rights covered by individual contract. Please check your
 * contract for exact rights and responsibilities.
 */
package net.citrusleaf;

import com.aerospike.client.Key;
import com.aerospike.client.Record;

/**
 * Legacy compatibility Layer. This class should only be used for legacy code.
 * Please use package <code>com.aerospike.client</code> for all new code.
 */
public final class ScanForwarder implements com.aerospike.client.ScanCallback  {
	
	private CitrusleafClient.ScanCallback callback;
	private Object userData;
	
	public ScanForwarder(CitrusleafClient.ScanCallback callback, Object userData) {
		this.callback = callback;
		this.userData = userData;
	}
	
	@Override
	public void scanCallback(Key key, Record record) {
		callback.scanCallback(key.namespace, key.setName, key.digest, record.bins, record.generation, record.expiration, userData);
	}	
}
