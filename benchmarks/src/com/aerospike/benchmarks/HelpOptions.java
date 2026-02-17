/*
 * Copyright 2012-2025 Aerospike, Inc.
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
package com.aerospike.benchmarks;

import picocli.CommandLine.Option;

/**
 * HelpOptions provides command-line options for help and version information.
 *
 * <p>This class uses picocli annotations to define command-line flags for: - Displaying usage help
 * information - Showing version information
 *
 * <p>These options are typically included in command-line applications to provide standard help
 * functionality to users.
 */
public class HelpOptions {
	@Option(
		names = {"-u", "-usage", "--usage"},
		usageHelp = true,
		description = "prints usage options")
	private boolean usageHelpRequested;

	@Option(
		names = {"-V", "-version", "--version"},
		versionHelp = true,
		description = "Show version info")
	private boolean versionRequested;

	public boolean isUsageHelpRequested() {
		return usageHelpRequested;
	}

	public boolean isVersionRequested() {
		return versionRequested;
	}
}
