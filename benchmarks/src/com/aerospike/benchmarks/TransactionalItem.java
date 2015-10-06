package com.aerospike.benchmarks;

public enum TransactionalItem {
	SINGLE_BIN_READ('r', true),
	SINGLE_BIN_UPDATE('u', false),
	SINGLE_BIN_REPLACE('p', false),
	SINGLE_BIN_INCREMENT('i', false),
	SINGLE_BIN_WRITE('w', false),		// either an update or a replace
	MULTI_BIN_READ('R', true),
	MULTI_BIN_UPDATE('U', false),
	MULTI_BIN_REPLACE('P', false),
	MULTI_BIN_WRITE('W', false);
	
	private char code;
	private boolean read;
	private TransactionalItem(char code, boolean isRead) {
		this.code = code;
		this.read = isRead;
	}
	
	public char getCode() {
		return code;
	}
	
	public boolean isRead() {
		return this.read;
	}
	
	public static TransactionalItem lookupCode(char code) {
		for (TransactionalItem thisItem : TransactionalItem.values()) {
			if (thisItem.code == code) {
				return thisItem;
			}
		}
		return null;
	}
}
