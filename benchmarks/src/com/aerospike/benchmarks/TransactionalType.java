package com.aerospike.benchmarks;

public enum TransactionalType {
	SINGLE_BIN_READ('r', true),
	SINGLE_BIN_BATCH_READ('b', true, true),
	SINGLE_BIN_UPDATE('u', false),
	SINGLE_BIN_REPLACE('p', false),
	SINGLE_BIN_INCREMENT('i', false),
	SINGLE_BIN_WRITE('w', false),		// either an update or a replace
	MULTI_BIN_READ('R', true),
	MULTI_BIN_BATCH_READ('B', true, true),
	MULTI_BIN_UPDATE('U', false),
	MULTI_BIN_REPLACE('P', false),
	MULTI_BIN_WRITE('W', false);
	
	private char code;
	private boolean read;
	private boolean batch;
	private TransactionalType(char code, boolean isRead, boolean isBatch) {
		this.code = code;
		this.read = isRead;
		this.batch = isBatch;
	}
	private TransactionalType(char code, boolean isRead) {
		this(code, isRead, false);
	}
	
	public char getCode() {
		return code;
	}
	
	public boolean isRead() {
		return this.read;
	}
	
	public boolean isBatch() {
		return batch;
	}
	
	public static TransactionalType lookupCode(char code) {
		for (TransactionalType thisItem : TransactionalType.values()) {
			if (thisItem.code == code) {
				return thisItem;
			}
		}
		return null;
	}
}
