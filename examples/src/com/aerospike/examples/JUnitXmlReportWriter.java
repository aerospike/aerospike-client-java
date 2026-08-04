/*
 * Copyright 2012-2026 Aerospike, Inc.
 *
 * Portions may be licensed to Aerospike, Inc. under one or more contributor
 * license agreements WHICH ARE COMPATIBLE WITH THE APACHE LICENSE, VERSION 2.0.
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

import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Locale;

public final class JUnitXmlReportWriter {
	private JUnitXmlReportWriter() {
	}

	public static void write(Path reportPath, ExampleRunResult runResult) throws IOException {
		Path parent = reportPath.getParent();

		if (parent != null) {
			Files.createDirectories(parent);
		}

		List<ExampleResult> results = runResult.results();
		long elapsedMillis = 0L;

		for (ExampleResult result : results) {
			elapsedMillis += result.elapsedMillis();
		}

		StringBuilder xml = new StringBuilder();
		xml.append("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n");
		xml.append("<testsuite name=\"Aerospike Examples\"");
		xml.append(" tests=\"").append(results.size()).append("\"");
		xml.append(" failures=\"").append(runResult.failedCount()).append("\"");
		xml.append(" skipped=\"").append(runResult.skippedCount()).append("\"");
		xml.append(" time=\"").append(seconds(elapsedMillis)).append("\">\n");

		for (ExampleResult result : results) {
			xml.append("  <testcase classname=\"com.aerospike.examples\" name=\"");
			xml.append(escape(result.name())).append("\" time=\"").append(seconds(result.elapsedMillis())).append("\">");

			if (result.status() == ExampleStatus.FAILED) {
				xml.append("\n    <failure message=\"").append(escape(nullToEmpty(result.message()))).append("\">");
				xml.append(escape(stackTrace(result.error())));
				xml.append("</failure>\n  ");
			}
			else if (result.status() == ExampleStatus.SKIPPED) {
				xml.append("\n    <skipped message=\"").append(escape(nullToEmpty(result.message()))).append("\" />\n  ");
			}

			xml.append("</testcase>\n");
		}

		xml.append("</testsuite>\n");
		Files.writeString(reportPath, xml.toString());
	}

	private static String seconds(long millis) {
		return String.format(Locale.US, "%.3f", millis / 1000.0);
	}

	private static String stackTrace(Throwable error) {
		if (error == null) {
			return "";
		}

		StringWriter sw = new StringWriter();
		PrintWriter pw = new PrintWriter(sw);
		error.printStackTrace(pw);
		pw.flush();
		return sw.toString();
	}

	private static String nullToEmpty(String value) {
		return value == null ? "" : value;
	}

	private static String escape(String value) {
		return value
			.replace("&", "&amp;")
			.replace("<", "&lt;")
			.replace(">", "&gt;")
			.replace("\"", "&quot;")
			.replace("'", "&apos;");
	}
}
