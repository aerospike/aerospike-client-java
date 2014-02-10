/*******************************************************************************
 * Copyright 2012-2014 by Aerospike.
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to
 * deal in the Software without restriction, including without limitation the
 * rights to use, copy, modify, merge, publish, distribute, sublicense, and/or
 * sell copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
 * FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS
 * IN THE SOFTWARE.
 ******************************************************************************/
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
