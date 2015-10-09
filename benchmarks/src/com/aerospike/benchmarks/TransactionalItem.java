package com.aerospike.benchmarks;

public class TransactionalItem {
	private TransactionalType type;
	private int repetitions;
	public TransactionalItem(TransactionalType type, int repetitions) {
		super();
		this.type = type;
		this.repetitions = repetitions;
	}
	
	public TransactionalItem(TransactionalType type) {
		super();
		this.type = type;
		this.repetitions = 1;
	}
	
	public TransactionalType getType() {
		return type;
	}
	public int getRepetitions() {
		return repetitions;
	}
}
