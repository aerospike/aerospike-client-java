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
