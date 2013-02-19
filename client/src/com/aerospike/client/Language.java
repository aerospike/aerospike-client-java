package com.aerospike.client;

/**
 * User defined function languages.
 */
public enum Language {
	/**
	 * Lua embedded programming language.
	 */
	LUA(0);
	
	/**
	 * Language ID.
	 */
	public final int id;
	
	private Language(int id) {
		this.id = id;
	}
}
