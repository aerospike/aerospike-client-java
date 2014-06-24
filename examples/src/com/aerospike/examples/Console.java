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
package com.aerospike.examples;

import java.text.SimpleDateFormat;
import java.util.Calendar;

import com.aerospike.client.Log;
import com.aerospike.client.Log.Level;

public class Console implements Log.Callback {

	private static final SimpleDateFormat Formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss z");

	public Console() {
		Log.setLevel(Level.INFO);
		Log.setCallback(this);
	}

	public void info(String format, Object... args) {
		write(Level.INFO, format, args);
	}

	public void info(String message) {
		write(Level.INFO, message);
	}

	public void warn(String format, Object... args) {
		write(Level.WARN, format, args);
	}

	public void warn(String message) {
		write(Level.WARN, message);
	}

	public void error(String format, Object... args) {
		write(Level.ERROR, format, args);
	}

	public void error(String message) {
		write(Level.ERROR, message);
	}

	public void write(Level level, String format, Object... args) {
		write(level, String.format(format, args));
	}

	public void write(Level level, String message) {
		write(Formatter.format(Calendar.getInstance().getTime()) + ' ' + level + ' ' + message);
	}

	public void write(String format, Object... args) {
		write(String.format(format, args));
	}

	public void write(String message) {
		System.out.println(message);
	}

	@Override
	public void log(Level level, String message) {
		write(level, message);
	}
}
