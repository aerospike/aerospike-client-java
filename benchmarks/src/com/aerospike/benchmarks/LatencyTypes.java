package com.aerospike.benchmarks;

public enum LatencyTypes {
	//Get
	READ,
	//Get where the PK was not found
	READNOTFOUND,
	//Put
	WRITE,
	//Business Transactions (not a MRT)
	TRANSACTION,
	//Unit of Work which consist of all the actions (get/puts) within a MRT. This will not include the latency for the commit or abort
	MRTUOW,
	//The MRT Commit segment
	MRTCOMMIT,
	//The MRT abort segment
	MRTABORT,
	//Unit of Work which consist of all the actions (get/puts) including the commit/abort within a MRT.
	MRTUOWTOTAL
}
