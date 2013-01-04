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

import java.util.Map;

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
	public void scanCallback(String namespace, String set, byte[] digest,
			Map<String, Object> bins, int generation, int expirationDate) {
		callback.scanCallback(namespace, set, digest, bins, generation, expirationDate, userData);
	}
}
