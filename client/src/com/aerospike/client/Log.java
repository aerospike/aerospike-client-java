/* 
 * Copyright 2012-2014 Aerospike, Inc.
 *
 * Portions may be licensed to Aerospike, Inc. under one or more contributor
 * license agreements.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package com.aerospike.client;

/**
 * Aerospike client logging facility. Logs can be filtered and message callbacks 
 * can be defined to control how log messages are written.
 */
public final class Log {
	/**
	 * Log escalation level.
	 */
	public enum Level {
		/**
		 * Error condition has occurred.
		 */
		ERROR,

		/**
		 * Unusual non-error condition has occurred.
		 */
		WARN,

		/**
		 * Normal information message.
		 */
		INFO,

		/**
		 * Message used for debugging purposes.
		 */
		DEBUG
	}
	
	/**
	 * An object implementing this interface may be passed in to 
	 * {@link #setCallback(Callback callback) setCallback()}, 
	 * so the caller can channel Aerospike client logs as desired.
	 */
	public static interface Callback {
		/**
		 * This method will be called for each client log statement.
		 * 
		 * @param level		{@link Level log level}
		 * @param message	log message
		 */
		public void log(Level level, String message);
	}

	private static volatile Level gLevel = Level.INFO;
	private static volatile Callback gCallback = null;

	/**
	 * Set log level filter.
	 * 
	 * @param level			only show logs at this or more urgent level
	 */
	public static void setLevel(Level level) {
		gLevel = level;
	}

	/**
	 * Set optional log callback implementation. If the callback is not defined (or null), 
	 * log messages will not be displayed.
	 * 
	 * @param callback		{@link Callback} implementation
	 */
	public static void setCallback(Callback callback) {
		gCallback = callback;
	}
		
	/**
	 * Determine if warning log level is enabled.
	 */
	public static boolean warnEnabled() {
		return gCallback != null && Level.WARN.ordinal() <= gLevel.ordinal();
	}

	/**
	 * Determine if info log level is enabled.
	 */
	public static boolean infoEnabled() {
		return gCallback != null && Level.INFO.ordinal() <= gLevel.ordinal();
	}

	/**
	 * Determine if debug log level is enabled.
	 */
	public static boolean debugEnabled() {
		return gCallback != null && Level.DEBUG.ordinal() <= gLevel.ordinal();
	}

	/**
	 * Log an error message. 
	 * 
	 * @param message		message string not terminated with a newline
	 */
	public static void error(String message) {
		log(Level.ERROR, message);
	}

	/**
	 * Log a warning message. 
	 * 
	 * @param message		message string not terminated with a newline
	 */
	public static void warn(String message) {
		log(Level.WARN, message);
	}

	/**
	 * Log an info message. 
	 * 
	 * @param message		message string not terminated with a newline
	 */
	public static void info(String message) {
		log(Level.INFO, message);
	}

	/**
	 * Log an debug message. 
	 * 
	 * @param message		message string not terminated with a newline
	 */
	public static void debug(String message) {
		log(Level.DEBUG, message);
	}

	/**
	 * Filter and forward message to callback.
	 * 
	 * @param level			message severity level				
	 * @param message		message string not terminated with a newline
	 */
	public static void log(Level level, String message) {
		if (gCallback != null && level.ordinal() <= gLevel.ordinal() ) {
			gCallback.log(level, message);
		}
	}
}
