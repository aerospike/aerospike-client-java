package com.aerospike.benchmarks;


public class ResponseObj {
	Object value;       // Object gotten from database
	long td;            // Time, in nanoseconds, that the operation took 
	int generation;     // Generation of the response data
}