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

import com.aerospike.client.async.EventLoopType;
import java.util.Arrays;
import picocli.CommandLine.Model.CommandSpec;
import picocli.CommandLine.Option;
import picocli.CommandLine.ParameterException;
import picocli.CommandLine.Spec;

/**
 * Configuration options for the Aerospike Java Benchmark tool.
 *
 * <p>This class encapsulates all the command-line options available for configuring benchmark
 * execution parameters including: - Threading options (OS threads and virtual threads) - Latency
 * reporting configuration - Batch operation settings - Asynchronous operation modes and settings -
 * Debug and reporting options
 *
 * <p>The options are specified using command-line parameters when running the benchmark tool and
 * control various aspects of how the benchmarks are executed and how results are reported.
 *
 * <p>The class uses picocli annotations to define command-line options and their validation logic.
 */
public class BenchmarkOptions {

	@Spec
	CommandSpec spec;

	/**
	 * Sets the number of OS threads the client will use to generate load.
	 *
	 * @param value The number of threads to use, must be greater than 0
	 * @throws ParameterException If the provided thread count is less than 1
	 */
	@Option(
		names = {"-z", "-threads", "--threads"},
		description = "Set the number of OS threads the client will use to generate load.")
	public void setThreads(int value) throws ParameterException {
		if (value < 1) {
			throw new ParameterException(
				getSpec().commandLine(), String.format(Constants.INVALID_THREADS_MESSAGE, value));
		}
		threads = value;
	}

	private Integer threads = 16;

	/**
	 * Set the number of virtual threads the client will use to generate load.
	 *
	 * @param value The number of virtual threads, must be at least 1
	 * @throws ParameterException If the value is less than 1
	 */
	@Option(
		names = {"-vt", "-virtualThreads", "--virtualThreads"},
		description =
			"Set the number of virtual threads the client will use to generate load.\n"
				+ "This option will override the OS threads setting (-z).")
	public void setVirtualThreads(int value) throws ParameterException {
		if (value < 1) {
			throw new ParameterException(
				getSpec().commandLine(), String.format(Constants.INVALID_VIRTUAL_THREADS_MESSAGE, value));
		}
		virtualThreads = value;
	}

	private Integer virtualThreads;

	@Option(
		names = {"-l", "-latency", "--latency"},
		description =
			"ycsb[,<warmup count>] | [alt,]<columns>,<range shift increment>[,us|ms] \n"
				+ "ycsb: Show the timings in ycsb format. \n"
				+ "alt: Show both count and percentage in each elapsed time bucket.\n"
				+ "default: Show percentage in each elapsed time bucket.\n"
				+ "<columns>: Number of elapsed time ranges.\n"
				+ "<range shift increment>: Power of 2 multiple between each range starting at column"
				+ " 3.\n"
				+ "(ms|us): display times in milliseconds (ms, default) or microseconds (us)\n\n"
				+ "A latency definition of '-latency 7,1' results in this layout:\n"
				+ "    <=1ms >1ms >2ms >4ms >8ms >16ms >32ms \n"
				+ "       x%%   x%%   x%%   x%%   x%%    x%%    x%% \n"
				+ "A latency definition of '-latency 4,3' results in this layout: \n"
				+ "    <=1ms >1ms >8ms >64ms \n"
				+ "       x%%   x%%   x%%    x%% \n\n"
				+ "Latency columns are cumulative. If a transaction takes 9ms, it will be included in"
				+ " both the >1ms and >8ms columns.")
	private String latency;

	@Option(
		names = {"-N", "-reportNotFound", "--reportNotFound"},
		description =
			"Report not found errors. Data should be fully initialized before using this option."
				+ " Default: false")
	private boolean reportNotFound;

	@Option(
		names = {"-D", "-debug", "--debug"},
		description = "Run benchmarks in debug mode. Default: false")
	private boolean debug;

	@Option(
		names = {"-B", "-batchSize", "--batchSize"},
		description =
			"Enable batch mode with number of records to process in each batch get call. Batch mode"
				+ " is valid only for RU (read update) workloads. Batch mode is disabled by default.")
	private Integer batchSize;

	@Option(
		names = {"-BSN", "-batchShowNodes", "--batchShowNodes"},
		description =
			"Print target nodes and count of keys directed at each node once on start of benchmarks."
				+ " Default: false")
	private boolean batchShowNodes;

	@Option(
		names = {"-prole", "-proleDistribution", "--proleDistribution"},
		description = "Distribute reads across proles in round-robin fashion. Default: false")
	private boolean proleDistribution;

	@Option(
		names = {"-a", "-async", "--async"},
		description = "Benchmark asynchronous methods instead of synchronous methods. Default: false")
	private boolean async;

	@Option(
		names = {"-C", "-asyncMaxCommands", "--asyncMaxCommands"},
		description = "Maximum number of concurrent asynchronous database commands. Default: 100")
	private Integer asyncMaxCommands;

	@Option(
		names = {"-W", "-eventLoops", "--eventLoops"},
		description = "Number of event loop threads when running in asynchronous mode. Default: 1")
	private Integer eventLoops;

	@Option(
		names = {"-netty", "--netty"},
		description = "Use Netty NIO event loops for async benchmarks. Default: false")
	private boolean netty;

	@Option(
		names = {"-nettyEpoll", "--nettyEpoll"},
		description = "Use Netty epoll event loops for async benchmarks (Linux only). Default: false")
	private boolean nettyEpoll;

	/**
	 * Sets the event loop type for asynchronous operations.
	 *
	 * @param value The event loop type string. Valid options are:
	 *              <ul>
	 *                <li>DIRECT_NIO - Direct NIO event loop
	 *                <li>NETTY_NIO - Netty NIO event loop
	 *                <li>NETTY_EPOLL - Netty epoll event loop (Linux only)
	 *                <li>NETTY_KQUEUE - Netty kqueue event loop (macOS only)
	 *                <li>NETTY_IOURING - Netty io_uring event loop (Linux with kernel 5.6+)
	 *              </ul>
	 * @throws ParameterException If the provided event loop type is invalid
	 */
	@Option(
		names = {"-elt", "-eventLoopType", "--eventLoopType"},
		description =
			"Use specified event loop type for async examples\n"
				+ "Value: DIRECT_NIO | NETTY_NIO | NETTY_EPOLL | NETTY_KQUEUE | NETTY_IOURING")
	public void setEventLoopType(String value) throws ParameterException {
		String valueUp = value.toUpperCase();
		try {
			EventLoopType.valueOf(valueUp);
		} catch (IllegalArgumentException e) {
			throw new ParameterException(
				getSpec().commandLine(),
				String.format(
					Constants.INVALID_EVENT_LOOP_TYPE_MESSAGE,
					value,
					Arrays.toString(EventLoopType.values())));
		}
		eventLoopType = valueUp;
	}

	private String eventLoopType;

	public CommandSpec getSpec() {
		return spec;
	}

	public Integer getThreads() {
		return threads;
	}

	public Integer getVirtualThreads() {
		return virtualThreads;
	}

	public String getLatency() {
		return latency;
	}

	public boolean isReportNotFound() {
		return reportNotFound;
	}

	public boolean isDebug() {
		return debug;
	}

	public Integer getBatchSize() {
		return batchSize;
	}

	public boolean isBatchShowNodes() {
		return batchShowNodes;
	}

	public boolean isProleDistribution() {
		return proleDistribution;
	}

	public boolean isAsync() {
		return async;
	}

	public Integer getAsyncMaxCommands() {
		return asyncMaxCommands;
	}

	public Integer getEventLoops() {
		return eventLoops;
	}

	public boolean isNetty() {
		return netty;
	}

	public boolean isNettyEpoll() {
		return nettyEpoll;
	}

	public String getEventLoopType() {
		return eventLoopType;
	}
}
