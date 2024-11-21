/*
 * Copyright 2012-2024 Aerospike, Inc.
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

import java.io.PrintWriter;
import java.io.StringWriter;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import com.aerospike.client.*;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

import com.aerospike.client.Log.Level;
import com.aerospike.client.async.EventLoop;
import com.aerospike.client.async.EventLoopType;
import com.aerospike.client.async.EventLoops;
import com.aerospike.client.async.EventPolicy;
import com.aerospike.client.async.NettyEventLoops;
import com.aerospike.client.async.NioEventLoops;
import com.aerospike.client.cluster.Node;
import com.aerospike.client.command.BatchNode;
import com.aerospike.client.command.BatchNodeList;
import com.aerospike.client.command.BatchStatus;
import com.aerospike.client.policy.AuthMode;
import com.aerospike.client.policy.ClientPolicy;
import com.aerospike.client.policy.CommitLevel;
import com.aerospike.client.policy.ReadModeAP;
import com.aerospike.client.policy.ReadModeSC;
import com.aerospike.client.policy.RecordExistsAction;
import com.aerospike.client.policy.Replica;
import com.aerospike.client.policy.TlsPolicy;
import com.aerospike.client.policy.WritePolicy;
import com.aerospike.client.proxy.AerospikeClientFactory;
import com.aerospike.client.util.Util;

import io.netty.channel.EventLoopGroup;
import io.netty.channel.epoll.Epoll;
import io.netty.channel.epoll.EpollEventLoopGroup;
import io.netty.channel.kqueue.KQueueEventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.incubator.channel.uring.IOUringEventLoopGroup;

public class Main implements Log.Callback {

	private static final DateTimeFormatter TimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS");
	public static List<String> keyList = null;

	public static void main(String[] args) {
		Main program = null;

		try {
			program = new Main(args);
			program.runBenchmarks();
		}
		catch (UsageException ignored) {
		}
		catch (ParseException pe) {
			System.out.println(pe.getMessage());
			System.out.println("Use -u option for program usage");
		}
		catch (Exception e) {
			System.out.println("Error: " + e.getMessage());
			e.printStackTrace();
		}
	}

	private final Arguments args = new Arguments();
	private final StringBuilder argsHdrGeneral = new StringBuilder();
	private final StringBuilder argsHdrPolicies = new StringBuilder();
	private final StringBuilder argsHdrOther = new StringBuilder();
	private final Host[] hosts;
	private EventLoopType eventLoopType = EventLoopType.DIRECT_NIO;
	private int port = 3000;
	private long nKeys;
	private long startKey;
	private int nThreads;
	private int asyncMaxCommands = 100;
	private int eventLoopSize = 1;
	private boolean useProxyClient;
	private boolean useVirtualThreads;
	private boolean asyncEnabled;
	private boolean initialize;
	private boolean batchShowNodes;
	private String filepath;
	private boolean mrtEnabled;
	private long nMRTs;
	private long keysPerMRT;

	private EventLoops eventLoops;
	private final ClientPolicy clientPolicy = new ClientPolicy();
	private final CounterStore counters = new CounterStore();

	private int openTelemetryEndPointPort = 19090;

	public Main(String[] commandLineArgs) throws Exception {
		boolean hasTxns = false;

		this.args.commandLineArgs = commandLineArgs;

		Options options = getOptions();

		// parse the command line arguments
		CommandLineParser parser = new DefaultParser();
		CommandLine line = parser.parse(options, commandLineArgs);
		String[] extra = line.getArgs();

		if (line.hasOption("u")) {
			logUsage(options);
			throw new UsageException();
		}

		if (line.hasOption("V")) {
			printVersion();
			throw new UsageException();
		}

		if (extra.length > 0) {
			throw new Exception("Unexpected arguments: " + Arrays.toString(extra));
		}

		if (line.hasOption("async")) {
			this.asyncEnabled = true;
		}

		if (line.hasOption("proxy")) {
			this.useProxyClient = true;
		}

		args.readPolicy = clientPolicy.readPolicyDefault;
		args.writePolicy = clientPolicy.writePolicyDefault;
		args.batchPolicy = clientPolicy.batchPolicyDefault;

		if (line.hasOption("e")) {
			args.writePolicy.expiration = Integer.parseInt(line.getOptionValue("e"));
			if (args.writePolicy.expiration < -1) {
				throw new Exception("Invalid expiration: " + args.writePolicy.expiration + " It should be >= -1");
			}
		}

		if (line.hasOption("readTouchTtlPercent")) {
			args.readPolicy.readTouchTtlPercent = Integer.parseInt(line.getOptionValue("readTouchTtlPercent"));
			if (args.readPolicy.readTouchTtlPercent < 0 || args.readPolicy.readTouchTtlPercent > 100) {
				throw new Exception("Invalid readTouchTtlPercent: " + args.readPolicy.readTouchTtlPercent +
					" Range: 0 - 100");
			}
		}

		if (line.hasOption("port")) {
			this.port = Integer.parseInt(line.getOptionValue("port"));
		}
		else {
			this.port = 3000;
		}

		// If the Aerospike server's default port (3000) is used and the proxy client is used,
		// Reset the port to the proxy server's default port (4000).
		if (port == 3000 && useProxyClient) {
			System.out.println("Change proxy server port to 4000");
			port = 4000;
		}

		if (line.hasOption("hosts")) {
			this.hosts = Host.parseHosts(line.getOptionValue("hosts"), this.port);
		}
		else {
			this.hosts = new Host[1];
			this.hosts[0] = new Host("127.0.0.1", this.port);
		}

		if (line.hasOption("loginTimeout")) {
			clientPolicy.loginTimeout = Integer.parseInt(line.getOptionValue("loginTimeout"));
		}

		if (line.hasOption("tendTimeout")) {
			clientPolicy.timeout = Integer.parseInt(line.getOptionValue("tendTimeout"));
		}

		if (line.hasOption("tendInterval")) {
			clientPolicy.tendInterval = Integer.parseInt(line.getOptionValue("tendInterval"));
		}

		if (line.hasOption("tls")) {
			clientPolicy.tlsPolicy = new TlsPolicy();

			if (line.hasOption("tp")) {
				String s = line.getOptionValue("tp", "");
				clientPolicy.tlsPolicy.protocols = s.split(",");
			}

			if (line.hasOption("tlsCiphers")) {
				String s = line.getOptionValue("tlsCiphers", "");
				clientPolicy.tlsPolicy.ciphers = s.split(",");
			}

			if (line.hasOption("tr")) {
				String s = line.getOptionValue("tr", "");
				clientPolicy.tlsPolicy.revokeCertificates = Util.toBigIntegerArray(s);
			}

			if (line.hasOption("tlsLoginOnly")) {
				clientPolicy.tlsPolicy.forLoginOnly = true;
			}
		}

		if (line.hasOption("auth")) {
			clientPolicy.authMode = AuthMode.valueOf(line.getOptionValue("auth", "").toUpperCase());
		}

		clientPolicy.user = line.getOptionValue("user");
		clientPolicy.password = line.getOptionValue("password");

		if (clientPolicy.user != null && clientPolicy.password == null) {
			java.io.Console console = System.console();

			if (console != null) {
				char[] pass = console.readPassword("Enter password:");

				if (pass != null) {
					clientPolicy.password = new String(pass);
				}
			}
		}

		if (line.hasOption("namespace")) {
			args.namespace = line.getOptionValue("namespace");
		}
		else {
			args.namespace = "test";
		}

		if (line.hasOption("batchNamespaces")) {
			args.batchNamespaces = line.getOptionValue("batchNamespaces").split(",");
		}

		if (line.hasOption("set")) {
			args.setName = line.getOptionValue("set");
		}
		else {
			args.setName = "testset";
		}

		if (line.hasOption("clusterName")) {
			clientPolicy.clusterName = line.getOptionValue("clusterName");
		}

		if (line.hasOption("servicesAlternate")) {
			clientPolicy.useServicesAlternate = true;
		}

		if (line.hasOption("maxSocketIdle")) {
			clientPolicy.maxSocketIdle = Integer.parseInt(line.getOptionValue("maxSocketIdle"));
		}

		if (line.hasOption("maxErrorRate")) {
			clientPolicy.maxErrorRate = Integer.parseInt(line.getOptionValue("maxErrorRate"));
		}

		if (line.hasOption("errorRateWindow")) {
			clientPolicy.errorRateWindow = Integer.parseInt(line.getOptionValue("errorRateWindow"));
		}

		if (line.hasOption("minConnsPerNode")) {
			clientPolicy.minConnsPerNode = Integer.parseInt(line.getOptionValue("minConnsPerNode"));
		}

		if (line.hasOption("maxConnsPerNode")) {
			clientPolicy.maxConnsPerNode = Integer.parseInt(line.getOptionValue("maxConnsPerNode"));
		}

		if (line.hasOption("asyncMinConnsPerNode")) {
			clientPolicy.asyncMinConnsPerNode = Integer.parseInt(line.getOptionValue("asyncMinConnsPerNode"));
		}

		if (line.hasOption("asyncMaxConnsPerNode")) {
			clientPolicy.asyncMaxConnsPerNode = Integer.parseInt(line.getOptionValue("asyncMaxConnsPerNode"));
		}
		else {
			clientPolicy.asyncMaxConnsPerNode = clientPolicy.maxConnsPerNode;
		}

		if (line.hasOption("keys")) {
			this.nKeys = Long.parseLong(line.getOptionValue("keys"));
		}
		else {
			this.nKeys = 100000;
		}

		if (line.hasOption("mrtSize")) {
			this.mrtEnabled = true;
			this.keysPerMRT = Long.parseLong(line.getOptionValue("mrtSize"));
			if (this.keysPerMRT < 1 || this.keysPerMRT > this.nKeys) {
				throw new Exception("Invalid mrtSize value");
			}
			long rem = this.nKeys % this.keysPerMRT;
			if (rem != 0) {
				throw new Exception("Invalid key distribution per MRT");
			}
			this.nMRTs = this.nKeys / this.keysPerMRT;
		}

		if (line.hasOption("startkey")) {
			this.startKey = Long.parseLong(line.getOptionValue("startkey"));
		}

		// Variables setting in case of command arguments passed with keys in File
		if (line.hasOption("keyFile")) {
			if (startKey + nKeys > Integer.MAX_VALUE) {
				throw new Exception("Invalid arguments when keyFile specified.  startkey " + startKey +
									" + keys " + nKeys + " must be <= " + Integer.MAX_VALUE);
			}

			this.filepath = line.getOptionValue("keyFile");
			// Load the file
			keyList = Utils.readKeyFromFile(filepath);
			if (keyList.isEmpty()) {
				throw new Exception("File : '" + filepath + "' is empty,this file can't be processed.");
			}
			this.nKeys = keyList.size();
			this.startKey = 0;

			if (line.hasOption("keyType")) {
				String keyType = line.getOptionValue("keyType");

				if (keyType.equals("S")) {
					args.keyType = KeyType.STRING;
				}
				else if (keyType.equals("I")) {
					if (Utils.isNumeric(keyList.getFirst())) {
						args.keyType = KeyType.INTEGER;
					}
					else {
						throw new Exception("Invalid keyType '" + keyType + "' Key type doesn't match with file content type.");
					}
				}
				else {
					throw new Exception("Invalid keyType: " + keyType);
				}
			}
			else {
				args.keyType = KeyType.STRING;
			}

		}

		if (line.hasOption("bins")) {
			args.nBins = Integer.parseInt(line.getOptionValue("bins"));
		}
		else {
			args.nBins = 1;
		}

		if (line.hasOption("objectSpec")) {
			String[] objectsArr = line.getOptionValue("objectSpec").split(",");
			args.objectSpec = new DBObjectSpec[objectsArr.length];

			for (int i = 0; i < objectsArr.length; i++) {
				try {
					DBObjectSpec spec = new DBObjectSpec(objectsArr[i]);
					args.objectSpec[i] = spec;
				}
				catch (Throwable t) {
					throw new Exception("Invalid object spec: " + objectsArr[i] + "\n" +
						t.getMessage());
				}
			}
		}
		else {
			// If the object is not specified, it has one bin of integer type.
			args.objectSpec = new DBObjectSpec[1];
			args.objectSpec[0] = new DBObjectSpec();
		}

		if (line.hasOption("keyFile")) {
			args.workload = Workload.READ_FROM_FILE;
		}
		else {
			args.workload = Workload.READ_UPDATE;
		}
		args.readPct = 50;
		args.readMultiBinPct = 100;
		args.writeMultiBinPct = 100;

		if (line.hasOption("workload")) {
			String[] workloadOpts = line.getOptionValue("workload").split(",");
			String workloadType = workloadOpts[0];

			if (workloadType.equals("I")) {
				args.workload = Workload.INITIALIZE;
				this.initialize = true;

				if (workloadOpts.length > 1) {
					throw new Exception("Invalid workload number of arguments: " + workloadOpts.length + " Expected 1.");
				}
			}
			else if (workloadType.equals("RU") || workloadType.equals("RR")) {

				if (workloadType.equals("RR")) {
					args.writePolicy.recordExistsAction = RecordExistsAction.REPLACE;
					args.workload = Workload.READ_REPLACE;
				}

				if (workloadOpts.length < 2 || workloadOpts.length > 4) {
					throw new Exception("Invalid workload number of arguments: " + workloadOpts.length + " Expected 2 to 4.");
				}

				args.readPct = Integer.parseInt(workloadOpts[1]);

				if (args.readPct < 0 || args.readPct > 100) {
					throw new Exception("Read-update workload read percentage must be between 0 and 100");
				}

				if (workloadOpts.length >= 3) {
					args.readMultiBinPct = Integer.parseInt(workloadOpts[2]);
				}

				if (workloadOpts.length >= 4) {
					args.writeMultiBinPct = Integer.parseInt(workloadOpts[3]);
				}
			}
			else if (workloadType.equals("RMU")) {
				args.workload = Workload.READ_MODIFY_UPDATE;

				if (workloadOpts.length > 1) {
					throw new Exception("Invalid workload number of arguments: " + workloadOpts.length + " Expected 1.");
				}
			}
			else if (workloadType.equals("RMI")) {
				args.workload = Workload.READ_MODIFY_INCREMENT;

				if (workloadOpts.length > 1) {
					throw new Exception("Invalid workload number of arguments: " + workloadOpts.length + " Expected 1.");
				}
			}
			else if (workloadType.equals("RMD")) {
				args.workload = Workload.READ_MODIFY_DECREMENT;

				if (workloadOpts.length > 1) {
					throw new Exception("Invalid workload number of arguments: " + workloadOpts.length + " Expected 1.");
				}
			}
			else if (workloadType.equals("TXN")) {
				args.workload = Workload.TRANSACTION;
				args.transactionalWorkload = new TransactionalWorkload(workloadOpts);
				hasTxns = true;
			}
			else {
				throw new Exception("Unknown workload: " + workloadType);
			}
		}

		if (line.hasOption("throughput")) {
			args.throughput = Integer.parseInt(line.getOptionValue("throughput"));
		}

		if (line.hasOption("transactions")) {
			args.transactionLimit = Long.parseLong(line.getOptionValue("transactions"));
		}

		if (line.hasOption("connectTimeout")) {
			int timeout = Integer.parseInt(line.getOptionValue("connectTimeout"));
			args.readPolicy.connectTimeout = timeout;
			args.writePolicy.connectTimeout = timeout;
			args.batchPolicy.connectTimeout = timeout;
		}

		if (line.hasOption("timeout")) {
			int timeout = Integer.parseInt(line.getOptionValue("timeout"));
			args.readPolicy.socketTimeout = timeout;
			args.readPolicy.totalTimeout = timeout;
			args.writePolicy.socketTimeout = timeout;
			args.writePolicy.totalTimeout = timeout;
			args.batchPolicy.socketTimeout = timeout;
			args.batchPolicy.totalTimeout = timeout;
		}

		if (line.hasOption("socketTimeout")) {
			int timeout = Integer.parseInt(line.getOptionValue("socketTimeout"));
			args.readPolicy.socketTimeout = timeout;
			args.writePolicy.socketTimeout = timeout;
			args.batchPolicy.socketTimeout = timeout;
		}

		if (line.hasOption("readSocketTimeout")) {
			int timeout = Integer.parseInt(line.getOptionValue("readSocketTimeout"));
			args.readPolicy.socketTimeout = timeout;
			args.batchPolicy.socketTimeout = timeout;
		}

		if (line.hasOption("writeSocketTimeout")) {
			int timeout = Integer.parseInt(line.getOptionValue("writeSocketTimeout"));
			args.writePolicy.socketTimeout = timeout;
		}

		if (line.hasOption("totalTimeout")) {
			int timeout = Integer.parseInt(line.getOptionValue("totalTimeout"));
			args.readPolicy.totalTimeout = timeout;
			args.writePolicy.totalTimeout = timeout;
			args.batchPolicy.totalTimeout = timeout;
		}

		if (line.hasOption("readTotalTimeout")) {
			int timeout = Integer.parseInt(line.getOptionValue("readTotalTimeout"));
			args.readPolicy.totalTimeout = timeout;
			args.batchPolicy.totalTimeout = timeout;
		}

		if (line.hasOption("writeTotalTimeout")) {
			int timeout = Integer.parseInt(line.getOptionValue("writeTotalTimeout"));
			args.writePolicy.totalTimeout = timeout;
		}

		if (line.hasOption("timeoutDelay")) {
			int timeoutDelay = Integer.parseInt(line.getOptionValue("timeoutDelay"));
			args.readPolicy.timeoutDelay = timeoutDelay;
			args.writePolicy.timeoutDelay = timeoutDelay;
			args.batchPolicy.timeoutDelay = timeoutDelay;
		}

		if (line.hasOption("maxRetries")) {
			int maxRetries = Integer.parseInt(line.getOptionValue("maxRetries"));
			args.readPolicy.maxRetries = maxRetries;
			args.writePolicy.maxRetries = maxRetries;
			args.batchPolicy.maxRetries = maxRetries;
		}

		if (line.hasOption("sleepBetweenRetries")) {
			int sleepBetweenRetries = Integer.parseInt(line.getOptionValue("sleepBetweenRetries"));
			args.readPolicy.sleepBetweenRetries = sleepBetweenRetries;
			args.writePolicy.sleepBetweenRetries = sleepBetweenRetries;
			args.batchPolicy.sleepBetweenRetries = sleepBetweenRetries;
		}

		if (line.hasOption("rackId")) {
			int rackId = Integer.parseInt(line.getOptionValue("rackId"));
			clientPolicy.rackId = rackId;
		}

		if (line.hasOption("replica")) {
			String replica = line.getOptionValue("replica");

            switch (replica) {
                case "master" -> {
                    args.readPolicy.replica = Replica.MASTER;
                    args.batchPolicy.replica = Replica.MASTER;
                }
                case "any" -> {
                    args.readPolicy.replica = Replica.MASTER_PROLES;
                    args.batchPolicy.replica = Replica.MASTER_PROLES;
                }
                case "sequence" -> {
                    args.readPolicy.replica = Replica.SEQUENCE;
                    args.batchPolicy.replica = Replica.SEQUENCE;
                }
                case "preferRack" -> {
                    args.readPolicy.replica = Replica.PREFER_RACK;
                    args.batchPolicy.replica = Replica.PREFER_RACK;
                    clientPolicy.rackAware = true;
                }
                default -> throw new Exception("Invalid replica: " + replica);
            }
		}

		// Leave this in for legacy reasons.
		if (line.hasOption("prole")) {
			args.readPolicy.replica = Replica.MASTER_PROLES;
		}

		if (line.hasOption("readModeAP")) {
			String level = line.getOptionValue("readModeAP").toUpperCase();
			ReadModeAP mode = ReadModeAP.valueOf(level);
			args.readPolicy.readModeAP = mode;
			args.writePolicy.readModeAP = mode;
			args.batchPolicy.readModeAP = mode;
		}

		if (line.hasOption("readModeSC")) {
			String level = line.getOptionValue("readModeSC").toUpperCase();
			ReadModeSC mode = ReadModeSC.valueOf(level);
			args.readPolicy.readModeSC = mode;
			args.writePolicy.readModeSC = mode;
			args.batchPolicy.readModeSC = mode;
		}

		if (line.hasOption("commitLevel")) {
			String level = line.getOptionValue("commitLevel");

			if (level.equals("master")) {
				args.writePolicy.commitLevel = CommitLevel.COMMIT_MASTER;
			}
			else if (! level.equals("all")) {
				throw new Exception("Invalid commitLevel: " + level);
			}
		}

		if (line.hasOption("connPoolsPerNode")) {
			clientPolicy.connPoolsPerNode = Integer.parseInt(line.getOptionValue("connPoolsPerNode"));
		}

		if (line.hasOption("threads")) {
			this.nThreads = Integer.parseInt(line.getOptionValue("threads"));

			if (this.nThreads < 1) {
				throw new Exception("Client threads (-z) must be > 0");
			}
		}
		else {
			this.nThreads = 16;
		}

		if (line.hasOption("virtualThreads")) {
			this.useVirtualThreads = true;
			this.nThreads = Integer.parseInt(line.getOptionValue("virtualThreads"));

			if (this.nThreads < 1) {
				throw new Exception("Client virtual threads must be > 0");
			}
		}

		if (line.hasOption("reportNotFound")) {
			args.reportNotFound = true;
		}

		if (line.hasOption("debug")) {
			args.debug = true;
		}

		if (line.hasOption("batchSize")) {
			args.batchSize = Integer.parseInt(line.getOptionValue("batchSize"));
		}

		if (line.hasOption("batchShowNodes")) {
			this.batchShowNodes = true;
		}

		if (line.hasOption("asyncMaxCommands")) {
			this.asyncMaxCommands = Integer.parseInt(line.getOptionValue("asyncMaxCommands"));
		}

		if (line.hasOption("eventLoops")) {
			this.eventLoopSize = Integer.parseInt(line.getOptionValue("eventLoops"));
		}

		if (line.hasOption("latency")) {
			String latencyString = line.getOptionValue("latency");
			String[] latencyOpts = latencyString.split(",");

			if (latencyOpts.length < 1) {
				throw new Exception(getLatencyUsage(latencyString));
			}

			if ("ycsb".equalsIgnoreCase(latencyOpts[0])) {
				if (latencyOpts.length > 2) {
					throw new Exception(getLatencyUsage(latencyString));
				}

				int warmupCount = 0;
				if (latencyOpts.length == 2) {
					warmupCount = Integer.parseInt(latencyOpts[1]);
				}
				counters.read.latency = new LatencyManagerYcsb(LatencyTypes.READ, warmupCount);
				counters.write.latency = new LatencyManagerYcsb(LatencyTypes.WRITE, warmupCount);
			}
			else {
				boolean alt = false;
				int index = 0;

				if ("alt".equalsIgnoreCase(latencyOpts[index])) {
					if (latencyOpts.length > 4) {
						throw new Exception(getLatencyUsage(latencyString));
					}
					alt = true;
					index++;
				}
				else {
					if (latencyOpts.length > 3) {
						throw new Exception(getLatencyUsage(latencyString));
					}
				}
				int columns = Integer.parseInt(latencyOpts[index++]);
				int bitShift = Integer.parseInt(latencyOpts[index++]);

				boolean showMicroSeconds = false;
				if (index < latencyOpts.length) {
					if ("us".equalsIgnoreCase(latencyOpts[index])) {
						showMicroSeconds = true;

					}
				}
				counters.showMicroSeconds = showMicroSeconds;

				if (alt) {
					counters.read.latency = new LatencyManagerAlternate(LatencyTypes.READ, columns, bitShift, showMicroSeconds);
					counters.write.latency = new LatencyManagerAlternate(LatencyTypes.WRITE, columns, bitShift, showMicroSeconds);
				}
				else {
					counters.read.latency = new LatencyManagerAerospike(LatencyTypes.READ, columns, bitShift, showMicroSeconds);
					counters.write.latency = new LatencyManagerAerospike(LatencyTypes.WRITE, columns, bitShift, showMicroSeconds);
				}
			}
		}

		if (! line.hasOption("random")) {
			args.setFixedBins();
		}

		if (line.hasOption("netty")) {
			this.eventLoopType = EventLoopType.NETTY_NIO;
		}

		if (line.hasOption("nettyEpoll")) {
			this.eventLoopType = EventLoopType.NETTY_EPOLL;
		}

		if (line.hasOption("eventLoopType")) {
			this.eventLoopType = EventLoopType.valueOf(line.getOptionValue("eventLoopType", "").toUpperCase());
		}

		if (line.hasOption("udfPackageName")) {
			args.udfPackageName = line.getOptionValue("udfPackageName");
		}

		if (line.hasOption("udfFunctionName")) {
			if (args.udfPackageName == null) {
				throw new Exception("Udf Package name missing");
			}
			args.udfFunctionName = line.getOptionValue("udfFunctionName");
		}

		if (line.hasOption("udfFunctionValues")) {
			Object[] udfVals = line.getOptionValue("udfFunctionValues").split(",");
			if (args.udfPackageName == null) {
				throw new Exception("Udf Package name missing");
			}

			if (args.udfFunctionName == null) {
				throw new Exception("Udf Function name missing");
			}
			Value[] udfValues = new Value[udfVals.length];
			int index = 0;
			for (Object value : udfVals) {
				udfValues[index++] = Value.get(value);
			}
			args.udfValues = udfValues;
		}

		if (line.hasOption("sendKey")) {
			args.writePolicy.sendKey = true;
		}

		if (line.hasOption("partitionIds")) {
			String[] pids = line.getOptionValue("partitionIds").split(",");

			Set<Integer> partitionIds = new HashSet<>();

			for (String pid : pids) {
				int partitionId = -1;

				try {
					partitionId = Integer.parseInt(pid);
				}
				catch(NumberFormatException nfe) {
					throw new Exception("Partition ID has to be an integer");
				}

				if (partitionId < 0  || partitionId >= Node.PARTITIONS) {
					throw new Exception("Partition ID has to be a value between 0 and " + Node.PARTITIONS);
				}

				partitionIds.add(partitionId);
			}

			args.partitionIds = partitionIds;
		}

		if(line.hasOption("prometheusPort")) {
			this.openTelemetryEndPointPort = Integer.parseInt(line.getOptionValue("prometheusPort"));
		}
		if(line.hasOption("opentelEnable")) {
			this.args.opentelEnabled = true;
		}

		printOptions();

		Log.Level level = (args.debug)? Log.Level.DEBUG : Log.Level.INFO;
		Log.setLevel(level);
		Log.setCallback(this);
		args.updatePolicy = new WritePolicy(args.writePolicy);
		args.updatePolicy.recordExistsAction = RecordExistsAction.UPDATE;
		args.replacePolicy = new WritePolicy(args.writePolicy);
		args.replacePolicy.recordExistsAction = RecordExistsAction.REPLACE;

		clientPolicy.failIfNotConnected = true;
	}

	private static Options getOptions() {
		Options options = new Options();
		options.addOption("h", "hosts", true,
                """
                        List of seed hosts in format: \
                        hostname1[:tlsname][:port1],...
                        The tlsname is only used when connecting with a secure TLS enabled server. \
                        If the port is not specified, the default port is used. \
                        IPv6 addresses must be enclosed in square brackets.
                        Default: localhost
                        Examples:
                        host1
                        host1:3000,host2:3000
                        192.168.1.10:cert1:3000,[2001::1111]:cert2:3000
                        """
			);
		options.addOption("p", "port", true, "Set the default port on which to connect to Aerospike.");
		options.addOption("U", "user", true, "User name");
		options.addOption("P", "password", true, "Password");
		options.addOption("n", "namespace", true, "Set the Aerospike namespace. Default: test");
		options.addOption("bns","batchNamespaces", true, "Set batch namespaces. Default is regular namespace.");
		options.addOption("s", "set", true, "Set the Aerospike set name. Default: testset");
		options.addOption("c", "clusterName", true, "Set expected cluster name.");
		options.addOption("servicesAlternate", false, "Set to enable use of services-alternate instead of services in info request during cluster tending");

		options.addOption("lt", "loginTimeout", true,
			"Set expected loginTimeout in milliseconds. The timeout is used when user " +
			"authentication is enabled and a node login is being performed. Default: 5000"
			);
		options.addOption("tt", "tendTimeout", true,
			"Set cluster tend info call timeout in milliseconds. Default: 1000"
			);
		options.addOption("ti", "tendInterval", true,
			"Interval between cluster tends in milliseconds. Default: 1000"
			);
		options.addOption("maxSocketIdle", true,
			"Maximum socket idle in connection pool in seconds. Default: 0"
			);
		options.addOption("maxErrorRate", true,
			"Maximum number of errors allowed per node per tend iteration. Default: 100"
			);
		options.addOption("errorRateWindow", true,
			"Number of cluster tend iterations that defines the window for maxErrorRate. Default: 1"
			);
		options.addOption("minConnsPerNode", true,
			"Minimum number of sync connections pre-allocated per server node. Default: 0"
			);
		options.addOption("maxConnsPerNode", true,
			"Maximum number of sync connections allowed per server node. Default: 100"
			);
		options.addOption("asyncMinConnsPerNode", true,
			"Minimum number of async connections pre-allocated per server node. Default: 0"
			);
		options.addOption("asyncMaxConnsPerNode", true,
			"Maximum number of async connections allowed per server node. Default: 100"
			);
		options.addOption("mrtSize", true, "Number of records per multi record transaction.");
		options.addOption("k", "keys", true,
			"Set the number of keys the client is dealing with. " +
			"If using an 'insert' workload (detailed below), the client will write this " +
			"number of keys, starting from value = startkey. Otherwise, the client " +
			"will read and update randomly across the values between startkey and " +
			"startkey + num_keys.  startkey can be set using '-S' or '-startkey'."
			);
		options.addOption("b", "bins", true,
			"Set the number of Aerospike bins. " +
			"Each bin will contain an object defined with -o. The default is single bin (-b 1)."
			);
		options.addOption("o", "objectSpec", true,
			"I | S:<size> | B:<size> | R:<size>:<rand_pct>\n" +
			"Set the type of object(s) to use in Aerospike transactions. Type can be 'I' " +
			"for integer, 'S' for string, 'B' for byte[] or 'R' for random bytes. " +
			"If type is 'I' (integer), do not set a size (integers are always 8 bytes)."
			);
		options.addOption("R", "random", false,
			"Use dynamically generated random bin values instead of default static fixed bin values."
			);
		options.addOption("S", "startkey", true,
			"Set the starting value of the working set of keys. " +
			"If using an 'insert' workload, the start_value indicates the first value to write. " +
			"Otherwise, the start_value indicates the smallest value in the working set of keys."
			);
		options.addOption("w", "workload", true,
                """
                        I | RU,<percent>[,<percent2>][,<percent3>] | RR,<percent>[,<percent2>][,<percent3>], RMU | RMI | RMD
                        Set the desired workload.
                        
                           -w I sets a linear 'insert' workload.
                        
                           -w RU,80 sets a random read-update workload with 80% reads and 20% writes.
                        
                              100% of reads will read all bins.
                        
                              100% of writes will write all bins.
                        
                           -w RU,80,60,30 sets a random multi-bin read-update workload with 80% reads and 20% writes.
                        
                              60% of reads will read all bins. 40% of reads will read a single bin.
                        
                              30% of writes will write all bins. 70% of writes will write a single bin.
                        
                           -w RR,20 sets a random read-replace workload with 20% reads and 80% replace all bin(s) writes.
                        
                              100% of reads will read all bins.
                        
                              100% of writes will replace all bins.
                        
                           -w RMU sets a random read all bins-update one bin workload with 50% reads.
                        
                           -w RMI sets a random read all bins-increment one integer bin workload with 50% reads.
                        
                           -w RMD sets a random read all bins-decrement one integer bin workload with 50% reads.
                        
                           -w TXN,r:1000,w:200,v:20%
                        
                              form business transactions with 1000 reads, 200 writes with a variation (+/-) of 20%
                        
                        """
			);
		options.addOption("e", "expirationTime", true,
                """
                        Set expiration time of each record in seconds.
                         -1: Never expire
                          0: Default to namespace expiration time
                         >0: Actual given expiration time"""
			);
		options.addOption("rt", "readTouchTtlPercent", true,
                """
                        Read touch TTL percent is expressed as a percentage of the TTL (or expiration) sent on the most
                        recent write such that a read within this interval of the record’s end of life will generate a touch.
                        Range: 0 - 100"""
		);
		options.addOption("g", "throughput", true,
			"Set a target transactions per second for the client. The client should not exceed this " +
			"average throughput."
			);
		options.addOption("t", "transactions", true,
			"Number of transactions to perform in read/write mode before shutting down. " +
			"The default is to run indefinitely."
			);
		options.addOption("ct", "connectTimeout", true,
			"Set socket connection timeout in milliseconds. Default: 0"
			);

		options.addOption("T", "timeout", true, "Set read and write socketTimeout and totalTimeout to the same timeout in milliseconds.");
		options.addOption("socketTimeout", true, "Set read and write socketTimeout in milliseconds.");
		options.addOption("readSocketTimeout", true, "Set read socketTimeout in milliseconds.");
		options.addOption("writeSocketTimeout", true, "Set write socketTimeout in milliseconds.");
		options.addOption("totalTimeout", true, "Set read and write totalTimeout in milliseconds.");
		options.addOption("readTotalTimeout", true, "Set read totalTimeout in milliseconds.");
		options.addOption("writeTotalTimeout", true, "Set write totalTimeout in milliseconds.");
		options.addOption("timeoutDelay", true, "Set read and write timeoutDelay in milliseconds.");

		options.addOption("rackId", true, "Set Rack where this benchmark instance resides.  Default: 0");
		options.addOption("maxRetries", true, "Maximum number of retries before aborting the current transaction.");
		options.addOption("sleepBetweenRetries", true,
			"Milliseconds to sleep between retries if a transaction fails and the timeout was not exceeded. " +
			"Enter zero to skip sleep."
			);
		options.addOption("r", "replica", true,
                """
                        Which replica to use for reads.
                        
                        Values:  master | any | sequence | preferRack.  Default: sequence
                        master: Always use node containing master partition.
                        any: Distribute reads across master and proles in round-robin fashion.
                        sequence: Always try master first. If master fails, try proles in sequence.
                        preferRack: Always try node on the same rack as the benchmark first. If no nodes on the same rack, use sequence.
                        Use 'rackId' option to set rack."""
			);
		options.addOption("readModeAP", true,
			"Read consistency level when in AP mode.\n" +
			"Values:  one | all.  Default: one"
			);
		options.addOption("readModeSC", true,
			"Read consistency level when in SC (strong consistency) mode.\n" +
			"Values:  session | linearize | allow_replica | allow_unavailable.  Default: session"
			);
		options.addOption("commitLevel", true,
			"Desired replica consistency guarantee when committing a transaction on the server.\n" +
			"Values:  all | master.  Default: all"
			);
		options.addOption("Y", "connPoolsPerNode", true,
			"Number of synchronous connection pools per node.  Default 1."
			);
		options.addOption("z", "threads", true,
			"Set the number of OS threads the client will use to generate load."
			);
		options.addOption("vt", "virtualThreads", true,
			"Set the number of virtual threads the client will use to generate load.\n" +
			"This option will override the OS threads setting (-z)."
		);
		options.addOption("latency", true,
                """
                        ycsb[,<warmup count>] | [alt,]<columns>,<range shift increment>[,us|ms]
                        ycsb: Show the timings in ycsb format.
                        alt: Show both count and pecentage in each elapsed time bucket.
                        default: Show pecentage in each elapsed time bucket.
                        <columns>: Number of elapsed time ranges.
                        <range shift increment>: Power of 2 multiple between each range starting at column 3.
                        (ms|us): display times in milliseconds (ms, default) or microseconds (us)
                        
                        A latency definition of '-latency 7,1' results in this layout:
                            <=1ms >1ms >2ms >4ms >8ms >16ms >32ms
                               x%   x%   x%   x%   x%    x%    x%
                        A latency definition of '-latency 4,3' results in this layout:
                            <=1ms >1ms >8ms >64ms
                               x%   x%   x%    x%
                        
                        Latency columns are cumulative. If a transaction takes 9ms, it will be included in both the >1ms and >8ms columns."""
			);

		options.addOption("N", "reportNotFound", false, "Report not found errors. Data should be fully initialized before using this option.");
		options.addOption("D", "debug", false, "Run benchmarks in debug mode.");
		options.addOption("u", "usage", false, "Print usage.");
		options.addOption("V", "version", false, "Print version.");

		options.addOption("B", "batchSize", true,
			"Enable batch mode with number of records to process in each batch get call. " +
			"Batch mode is valid only for RU (read update) workloads. Batch mode is disabled by default."
			);

		options.addOption("BSN", "batchShowNodes", false,
			"Print target nodes and count of keys directed at each node once on start of benchmarks."
			);

		options.addOption("prole", false, "Distribute reads across proles in round-robin fashion.");

		options.addOption("a", "async", false, "Benchmark asynchronous methods instead of synchronous methods.");
		options.addOption("C", "asyncMaxCommands", true, "Maximum number of concurrent asynchronous database commands.");
		options.addOption("W", "eventLoops", true, "Number of event loop threads when running in asynchronous mode.");
		options.addOption("F", "keyFile", true, "File path to read the keys for read operation.");
		options.addOption("KT", "keyType", true, "Type of the key(String/Integer) in the file, default is String");
		options.addOption("tls", "tlsEnable", false, "Use TLS/SSL sockets");
		options.addOption("tp", "tlsProtocols", true,
                """
                        Allow TLS protocols
                        Values:  TLSv1,TLSv1.1,TLSv1.2 separated by comma
                        Default: TLSv1.2"""
			);
		options.addOption("tlsCiphers", "tlsCipherSuite", true,
                """
                        Allow TLS cipher suites
                        Values:  cipher names defined by JVM separated by comma
                        Default: null (default cipher list provided by JVM)"""
			);
		options.addOption("tr", "tlsRevoke", true,
                """
                        Revoke certificates identified by their serial number
                        Values:  serial numbers separated by comma
                        Default: null (Do not revoke certificates)"""
			);
		options.addOption("tlsLoginOnly", false, "Use TLS/SSL sockets on node login only");
		options.addOption("auth", true, "Authentication mode. Values: " + Arrays.toString(AuthMode.values()));

		options.addOption("netty", false, "Use Netty NIO event loops for async benchmarks");
		options.addOption("nettyEpoll", false, "Use Netty epoll event loops for async benchmarks (Linux only)");
		options.addOption("elt", "eventLoopType", true,
			"Use specified event loop type for async examples\n" +
			"Value: DIRECT_NIO | NETTY_NIO | NETTY_EPOLL | NETTY_KQUEUE | NETTY_IOURING"
			);

		options.addOption("proxy", false, "Use proxy client.");

		options.addOption("upn", "udfPackageName", true, "Specify the package name where the udf function is located");
		options.addOption("ufn", "udfFunctionName", true, "Specify the udf function name that must be used in the udf benchmarks");
		options.addOption("ufv","udfFunctionValues",true, "The udf argument values comma separated");
		options.addOption("sendKey", false, "Send key to server");

		options.addOption("pids", "partitionIds", true, "Specify the list of comma seperated partition IDs the primary keys must belong to");

		options.addOption("pom", "prometheusPort", true, "Prometheus OpenTel End Point. If -1, OpenTel metrics are disabled. Default 19090");
		options.addOption("otel", "opentelEnable", false, "Open Telemetry Metrics will be enabled. Disabled by default.");

		return options;
	}

	private static void logUsage(Options options) {
		HelpFormatter formatter = new HelpFormatter();
		StringWriter sw = new StringWriter();
		PrintWriter pw = new PrintWriter(sw);
		String syntax = Main.class.getName() + " [<options>]";
		formatter.printHelp(pw, 100, syntax, "options:", options, 0, 2, null);

		System.out.println(sw);
	}

	private static String getLatencyUsage(String latencyString) {
		return "Latency usage: ycsb[,<warmup count>] | [alt,]<columns>,<range shift increment>[,us|ms]  Received: " + latencyString;
	}

	public void runBenchmarks() throws Exception {

		try (OpenTelemetry openTelemetry = OpenTelemetryHelper.Create(this.openTelemetryEndPointPort,
																		this.args,
																		this.hosts[0],
																		this.args.opentelEnabled,
																		this.clientPolicy.clusterName,
																		this.argsHdrGeneral,
																		this.argsHdrPolicies,
																		this.argsHdrOther,
																		this.nKeys,
																		this.nThreads,
																		this.mrtEnabled ? this.nMRTs : -1,
																		this.asyncEnabled,
																		this.counters)) {
			System.out.println(openTelemetry.printConfiguration());
			System.out.println();
			if(this.args.debug){
				System.out.println(openTelemetry);
				System.out.println();
			}

			IAerospikeClient client;
			String clusterName;

			if (this.asyncEnabled) {
				EventPolicy eventPolicy = new EventPolicy();

				if (args.readPolicy.socketTimeout > 0 && args.readPolicy.socketTimeout < eventPolicy.minTimeout) {
					eventPolicy.minTimeout = args.readPolicy.socketTimeout;
				}

				if (args.writePolicy.socketTimeout > 0 && args.writePolicy.socketTimeout < eventPolicy.minTimeout) {
					eventPolicy.minTimeout = args.writePolicy.socketTimeout;
				}

				if (this.useProxyClient && this.eventLoopType == EventLoopType.DIRECT_NIO) {
					// Proxy client requires netty event loops.
					if (Epoll.isAvailable()) {
						this.eventLoopType = EventLoopType.NETTY_EPOLL;
					} else {
						this.eventLoopType = EventLoopType.NETTY_NIO;
					}
				}

				switch (this.eventLoopType) {
					default:
					case DIRECT_NIO: {
						eventLoops = new NioEventLoops(eventPolicy, this.eventLoopSize);
						break;
					}

					case NETTY_NIO: {
						EventLoopGroup group = new NioEventLoopGroup(this.eventLoopSize);
						eventLoops = new NettyEventLoops(eventPolicy, group, this.eventLoopType);
						break;
					}

					case NETTY_EPOLL: {
						EventLoopGroup group = new EpollEventLoopGroup(this.eventLoopSize);
						eventLoops = new NettyEventLoops(eventPolicy, group, this.eventLoopType);
						break;
					}

					case NETTY_KQUEUE: {
						EventLoopGroup group = new KQueueEventLoopGroup(this.eventLoopSize);
						eventLoops = new NettyEventLoops(eventPolicy, group, this.eventLoopType);
						break;
					}

					case NETTY_IOURING: {
						EventLoopGroup group = new IOUringEventLoopGroup(this.eventLoopSize);
						eventLoops = new NettyEventLoops(eventPolicy, group, this.eventLoopType);
						break;
					}
				}

				try {
					clientPolicy.eventLoops = eventLoops;

					if (clientPolicy.asyncMaxConnsPerNode < this.asyncMaxCommands) {
						clientPolicy.asyncMaxConnsPerNode = this.asyncMaxCommands;
					}

					try {
						openTelemetry.setDBConnectionState("Opening");
						client = AerospikeClientFactory.getClient(clientPolicy, useProxyClient, hosts);
						clusterName = client.getCluster().getClusterName();

						if(clusterName == null && !useProxyClient){
							clusterName = Info.request(this.hosts[0].name, this.hosts[0].port, "cluster-name");
						}
					}
					catch (Exception e) {
						openTelemetry.setDBConnectionState(e.getMessage());
						throw e;
					}
					openTelemetry.setDBConnectionState("Opened");
					openTelemetry.setClusterName(clusterName);

					try {
						if (mrtEnabled) {
							if (initialize) {
								// TODO
							}
						} else {
							if (initialize) {
								doAsyncInserts(client);
							} else {
								showBatchNodes(client);
								doAsyncRWTest(client);
							}
						}
					} finally {
						openTelemetry.setDBConnectionState("Closed");
						client.close();
					}
				} finally {
					eventLoops.close();
				}
			} else {

				try {
					openTelemetry.setDBConnectionState("Opening");
					client = AerospikeClientFactory.getClient(clientPolicy, useProxyClient, hosts);

					clusterName = client.getCluster().getClusterName();

					if(clusterName == null && !useProxyClient){
						clusterName = Info.request(this.hosts[0].name, this.hosts[0].port, "cluster-name");
					}
				}
				catch (Exception e) {
					openTelemetry.setDBConnectionState(e.getMessage());
					throw e;
				}

				openTelemetry.setDBConnectionState("Opened");
				openTelemetry.setClusterName(clusterName);

				try {
					if (mrtEnabled) {
						if (initialize) {
							doMRTInserts(client);
						} else {
							doMRTRWTest(client);
						}
					} else {
						if (initialize) {
							doInserts(client);
						} else {
							showBatchNodes(client);
							doRWTest(client);
						}
					}
				} finally {
					openTelemetry.setDBConnectionState("Closed");
					client.close();
				}
			}

		}
	}

	private void doInserts(IAerospikeClient client) throws Exception {
		ExecutorService es = getExecutorService();

		// Create N insert tasks
		long ntasks = this.nThreads < this.nKeys ? this.nThreads : this.nKeys;
		long keysPerTask = this.nKeys / ntasks;
		long rem = this.nKeys - (keysPerTask * ntasks);
		long start = this.startKey;

		for (long i = 0; i < ntasks; i++) {
			long keyCount = (i < rem) ? keysPerTask + 1 : keysPerTask;
			InsertTaskSync it = new InsertTaskSync(client, args, counters, start, keyCount);
			es.execute(it);
			start += keyCount;
		}
		Thread.sleep(900);
		collectInsertStats();
		es.shutdownNow();
	}

	private void doMRTInserts(IAerospikeClient client) throws Exception {
		ExecutorService es = getExecutorService();

		// Create N insert tasks
		long ntasks = this.nThreads < this.nMRTs ? this.nThreads : this.nMRTs;
		long mrtsPerTask = this.nMRTs / ntasks;
		long rem = this.nMRTs - (mrtsPerTask * ntasks);
		long start = this.startKey;
		long keysPerMRT = this.keysPerMRT;

		for (long i = 0; i < ntasks; i++) {
			long nMrtsPerThread = (i < rem) ? mrtsPerTask + 1 : mrtsPerTask;
			MRTInsertTaskSync it = new MRTInsertTaskSync(client, args, counters, start, keysPerMRT, nMrtsPerThread);
			es.execute(it);
			start += keysPerMRT * nMrtsPerThread;
		}
		Thread.sleep(900);
		collectMRTStats();
		es.shutdownNow();
	}

	private void doAsyncInserts(IAerospikeClient client) throws Exception {
		// Generate asyncMaxCommand writes to seed the event loops.
		// Then start a new command in each command callback.
		// This effectively throttles new command generation, by only allowing
		// asyncMaxCommands at any point in time.
		long maxConcurrentCommands = this.asyncMaxCommands;

		if (maxConcurrentCommands > this.nKeys) {
			maxConcurrentCommands = this.nKeys;
		}

		long keysPerCommand = this.nKeys / maxConcurrentCommands;
		long keysRem = this.nKeys - (keysPerCommand * maxConcurrentCommands);
		long keyStart = this.startKey;

		for (int i = 0; i < maxConcurrentCommands; i++) {
			// Allocate separate tasks for each seed command and reuse them in callbacks.
			long keyCount = (i < keysRem) ? keysPerCommand + 1 : keysPerCommand;

			// Start seed commands on random event loops.
			EventLoop eventLoop = this.eventLoops.next();
			InsertTaskAsync task = new InsertTaskAsync(client, eventLoop, args, counters, keyStart, keyCount);
			task.runCommand();
			keyStart += keyCount;
		}
		Thread.sleep(900);
		collectInsertStats();
	}

	private void collectInsertStats() throws Exception {
		long total = 0;

		while (total < this.nKeys) {
			long time = System.currentTimeMillis();

			int numWrites = this.counters.write.count.getAndSet(0);
			int timeoutWrites = this.counters.write.timeouts.getAndSet(0);
			int errorWrites = this.counters.write.errors.getAndSet(0);
			total += numWrites;

			this.counters.periodBegin.set(time);

			LocalDateTime dt = Instant.ofEpochMilli(time).atZone(ZoneId.systemDefault()).toLocalDateTime();
			System.out.println(dt.format(TimeFormatter) + " write(count=" + total + " tps=" + numWrites +
				" timeouts=" + timeoutWrites + " errors=" + errorWrites + ")");

			if (this.counters.write.latency != null) {
				this.counters.write.latency.printHeader(System.out);
				this.counters.write.latency.printResults(System.out, "write");
			}

			Thread.sleep(1000);
		}

		if (this.counters.write.latency != null) {
			this.counters.write.latency.printSummaryHeader(System.out);
			this.counters.write.latency.printSummary(System.out, "write");
		}
	}

	private void collectMRTStats() throws Exception {
		long total = 0;

		while (total < this.nKeys) {
			long time = System.currentTimeMillis();

			int numWrites = this.counters.write.count.getAndSet(0);
			int timeoutWrites = this.counters.write.timeouts.getAndSet(0);
			int errorWrites = this.counters.write.errors.getAndSet(0);
			total += numWrites;

			this.counters.periodBegin.set(time);

			LocalDateTime dt = Instant.ofEpochMilli(time).atZone(ZoneId.systemDefault()).toLocalDateTime();
			System.out.println(dt.format(TimeFormatter) + " write(count=" + total + " tps=" + numWrites + " timeouts="
					+ timeoutWrites + " errors=" + errorWrites + ")");

			if (this.counters.write.latency != null) {
				this.counters.write.latency.printHeader(System.out);
				this.counters.write.latency.printResults(System.out, "write");
			}

			Thread.sleep(1000);
		}

		if (this.counters.write.latency != null) {
			this.counters.write.latency.printSummaryHeader(System.out);
			this.counters.write.latency.printSummary(System.out, "write");
		}
	}

	private void doRWTest(IAerospikeClient client) throws Exception {
		ExecutorService es = getExecutorService();
		RWTask[] tasks = new RWTask[this.nThreads];

		for (int i = 0; i < this.nThreads; i++) {
			RWTaskSync rt = new RWTaskSync(client, args, counters, this.startKey, this.nKeys);
			tasks[i] = rt;
			es.execute(rt);
		}
		Thread.sleep(900);
		collectRWStats(tasks);
		es.shutdown();
	}

	private void doMRTRWTest(IAerospikeClient client) throws Exception {
		ExecutorService es = getExecutorService();
		long ntasks = this.nThreads < this.nMRTs ? this.nThreads : this.nMRTs;
		long mrtsPerTask = this.nMRTs / ntasks;
		long rem = this.nMRTs - (mrtsPerTask * ntasks);
		MRTRWTask[] tasks = new MRTRWTask[this.nThreads];

		for (int i = 0; i < ntasks; i++) {
			long nMrtsPerThread = (i < rem) ? mrtsPerTask + 1 : mrtsPerTask;
			MRTRWTaskSync rt = new MRTRWTaskSync(client, args, counters, nMrtsPerThread, this.startKey,
					this.nKeys, this.keysPerMRT);
			tasks[i] = rt;
			es.execute(rt);
		}
		Thread.sleep(1000);
		collectMRTRWStats(tasks);
		es.shutdownNow();
	}

	private void doAsyncRWTest(IAerospikeClient client) throws Exception {
		// Generate asyncMaxCommand commands to seed the event loops.
		// Then start a new command in each command callback.
		// This effectively throttles new command generation, by only allowing
		// asyncMaxCommands at any point in time.
		int maxConcurrentCommands = this.asyncMaxCommands;

		if (maxConcurrentCommands > this.nKeys) {
			maxConcurrentCommands = (int) this.nKeys;
		}

		// Create seed commands distributed among event loops.
		RWTask[] tasks = new RWTask[maxConcurrentCommands];

		for (int i = 0; i < maxConcurrentCommands; i++) {
			EventLoop eventLoop = this.clientPolicy.eventLoops.next();
			tasks[i] = new RWTaskAsync(client, eventLoop, args, counters, this.startKey, this.nKeys);
		}

		// Start seed commands.
		for (RWTask task : tasks) {
			task.runNextCommand();
		}

		Thread.sleep(900);
		collectRWStats(tasks);
	}

	private void collectRWStats(RWTask[] tasks) throws Exception {
		long transactionTotal = 0;

		while (true) {
			long time = System.currentTimeMillis();

			int numWrites = this.counters.write.count.getAndSet(0);
			int timeoutWrites = this.counters.write.timeouts.getAndSet(0);
			int errorWrites = this.counters.write.errors.getAndSet(0);

			int numReads = this.counters.read.count.getAndSet(0);
			int timeoutReads = this.counters.read.timeouts.getAndSet(0);
			int errorReads = this.counters.read.errors.getAndSet(0);

			int numTxns = this.counters.transaction.count.getAndSet(0);
			int timeoutTxns = this.counters.transaction.timeouts.getAndSet(0);
			int errorTxns = this.counters.transaction.errors.getAndSet(0);

			int notFound = 0;

			if (args.reportNotFound) {
				notFound = this.counters.readNotFound.getAndSet(0);
			}
			this.counters.periodBegin.set(time);

			LocalDateTime dt = Instant.ofEpochMilli(time).atZone(ZoneId.systemDefault()).toLocalDateTime();
			System.out.print(dt.format(TimeFormatter));
			System.out.print(" write(tps=" + numWrites + " timeouts=" + timeoutWrites + " errors=" + errorWrites + ")");
			System.out.print(" read(tps=" + numReads + " timeouts=" + timeoutReads + " errors=" + errorReads);
			if (this.counters.transaction.latency != null) {
				System.out.print(" txns(tps=" + numTxns + " timeouts=" + timeoutTxns + " errors=" + errorTxns);
			}
			if (args.reportNotFound) {
				System.out.print(" nf=" + notFound);
			}
			System.out.print(")");

			System.out.print(" total(tps=" + (numWrites + numReads) + " timeouts=" + (timeoutWrites + timeoutReads) + " errors=" + (errorWrites + errorReads) + ")");
			//System.out.print(" buffused=" + used
			//System.out.print(" nodeused=" + ((AsyncNode)nodes[0]).openCount.get() + ',' + ((AsyncNode)nodes[1]).openCount.get() + ',' + ((AsyncNode)nodes[2]).openCount.get()
			System.out.println();

			if (this.counters.write.latency != null) {
				this.counters.write.latency.printHeader(System.out);
				this.counters.write.latency.printResults(System.out, "write");
				this.counters.read.latency.printResults(System.out, "read");
				if (this.counters.transaction != null && this.counters.transaction.latency != null) {
					this.counters.transaction.latency.printResults(System.out, "txn");
				}
			}

			if (args.transactionLimit > 0) {
				transactionTotal += numWrites + timeoutWrites + errorWrites + numReads + timeoutReads + errorReads;

				if (transactionTotal >= args.transactionLimit) {
					for (RWTask task : tasks) {
						task.stop();
					}

					if (this.counters.write.latency != null) {
						this.counters.write.latency.printSummaryHeader(System.out);
						this.counters.write.latency.printSummary(System.out, "write");
						this.counters.read.latency.printSummary(System.out, "read");
						if (this.counters.transaction != null && this.counters.transaction.latency != null) {
							this.counters.transaction.latency.printSummary(System.out, "txn");
						}
					}

					System.out.println("Transaction limit reached: " + args.transactionLimit + ". Exiting.");
					break;
				}
			}

			Thread.sleep(1000);
		}
	}

	private void collectMRTRWStats(MRTRWTask[] tasks) throws Exception {
		long transactionTotal = 0;

		while (true) {
			long time = System.currentTimeMillis();

			int numWrites = this.counters.write.count.getAndSet(0);
			int timeoutWrites = this.counters.write.timeouts.getAndSet(0);
			int errorWrites = this.counters.write.errors.getAndSet(0);

			int numReads = this.counters.read.count.getAndSet(0);
			int timeoutReads = this.counters.read.timeouts.getAndSet(0);
			int errorReads = this.counters.read.errors.getAndSet(0);

			int numTxns = this.counters.transaction.count.getAndSet(0);
			int timeoutTxns = this.counters.transaction.timeouts.getAndSet(0);
			int errorTxns = this.counters.transaction.errors.getAndSet(0);

			int notFound = 0;

			if (args.reportNotFound) {
				notFound = this.counters.readNotFound.getAndSet(0);
			}
			this.counters.periodBegin.set(time);

			LocalDateTime dt = Instant.ofEpochMilli(time).atZone(ZoneId.systemDefault()).toLocalDateTime();
			System.out.print(dt.format(TimeFormatter));
			System.out.print(" write(tps=" + numWrites + " timeouts=" + timeoutWrites + " errors=" + errorWrites + ")");
			System.out.print(" read(tps=" + numReads + " timeouts=" + timeoutReads + " errors=" + errorReads);
			if (this.counters.transaction.latency != null) {
				System.out.print(" txns(tps=" + numTxns + " timeouts=" + timeoutTxns + " errors=" + errorTxns);
			}
			if (args.reportNotFound) {
				System.out.print(" nf=" + notFound);
			}
			System.out.print(")");

			System.out.print(" total(tps=" + (numWrites + numReads) + " timeouts=" + (timeoutWrites + timeoutReads)
					+ " errors=" + (errorWrites + errorReads) + ")");
			// System.out.print(" buffused=" + used
			// System.out.print(" nodeused=" + ((AsyncNode)nodes[0]).openCount.get() + ',' +
			// ((AsyncNode)nodes[1]).openCount.get() + ',' +
			// ((AsyncNode)nodes[2]).openCount.get()
			System.out.println();

			if (this.counters.write.latency != null) {
				this.counters.write.latency.printHeader(System.out);
				this.counters.write.latency.printResults(System.out, "write");
				this.counters.read.latency.printResults(System.out, "read");
				if (this.counters.transaction != null && this.counters.transaction.latency != null) {
					this.counters.transaction.latency.printResults(System.out, "txn");
				}
			}

			if (args.transactionLimit > 0) {
				transactionTotal += numWrites + timeoutWrites + errorWrites + numReads + timeoutReads + errorReads;

				if (transactionTotal >= args.transactionLimit) {
					for (MRTRWTask task : tasks) {
						if (task != null) {
							task.stop();
						}
					}

					if (this.counters.write.latency != null) {
						this.counters.write.latency.printSummaryHeader(System.out);
						this.counters.write.latency.printSummary(System.out, "write");
						this.counters.read.latency.printSummary(System.out, "read");
						if (this.counters.transaction != null && this.counters.transaction.latency != null) {
							this.counters.transaction.latency.printSummary(System.out, "txn");
						}
					}

					System.out.println("Transaction limit reached: " + args.transactionLimit + ". Exiting.");
					break;
				}
			}

			Thread.sleep(1000);
		}
	}

	private ExecutorService getExecutorService() {
		return useVirtualThreads ?
				Executors.newVirtualThreadPerTaskExecutor() :
				Executors.newFixedThreadPool(this.nThreads);
	}

	private void showBatchNodes(IAerospikeClient client) {
		if (!batchShowNodes || args.batchSize <= 1) {
			return;
		}

		// Print target nodes for the first batchSize keys. The keys in each batch transaction
		// are randomly generated, so the actual target nodes may differ in each batch transaction.
		// This is useful to determine how increasing the cluster size also increases the number of
		// batch target nodes, which may result in a performance decrease for batch commands.
		Key[] keys = new Key[args.batchSize];

		for (int i = 0; i < keys.length; i++) {
			keys[i] = new Key(args.namespace, args.setName, i);
		}

		BatchStatus status = new BatchStatus(false);
		List<BatchNode> batchNodes = BatchNodeList.generate(client.getCluster(), args.batchPolicy, keys, null, false, status);

		System.out.println("Batch target nodes for first " + keys.length + " keys:");

		for (BatchNode bn : batchNodes) {
			System.out.println(bn.node.toString() + " keys: " + bn.offsetsSize);
		}
	}

	@Override
	public void log(Level level, String message) {
		Thread thread = Thread.currentThread();
		String name = thread.getName();

		if (name == null) {
			name = thread.getName();
		}

		System.out.println(LocalDateTime.now().format(TimeFormatter) + ' ' + level.toString() +
			" Thread " + name + ' ' + message);
	}

	private static class UsageException extends Exception {
		private static final long serialVersionUID = 1L;
	}

	private static void printVersion() {
		final Properties properties = new Properties();
		try {
			properties.load(Main.class.getClassLoader().getResourceAsStream("project.properties"));
		}
		catch (Exception e) {
			System.out.println("None");
		}
		finally {
			System.out.println(properties.getProperty("name"));
			System.out.println("Version " + properties.getProperty("version"));
		}
	}

	private void printOptions(){

		String threadType = useVirtualThreads ? "virtual" : "OS";

		argsHdrGeneral.append("Benchmark: ")
				.append(this.hosts[0])
				.append(", namespace: ")
				.append(args.namespace)
				.append(", set: ")
				.append(!args.setName.isEmpty() ? args.setName : "<empty>")
				.append(", ")
				.append(threadType)
				.append(" threads: ")
				.append(this.nThreads)
				.append(", workload: ")
				.append(args.workload)
				.append('\n');

		if (args.workload == Workload.READ_UPDATE || args.workload == Workload.READ_REPLACE) {
			argsHdrGeneral.append("read: ")
					.append(args.readPct)
					.append('%');
			argsHdrGeneral.append(" (all bins: ")
					.append(args.readMultiBinPct)
					.append('%');
			argsHdrGeneral.append(", single bin: ")
					.append(100 - args.readMultiBinPct)
					.append("%)");

			argsHdrGeneral.append(", write: ")
					.append(100 - args.readPct)
					.append('%');
			argsHdrGeneral.append(" (all bins: ")
					.append(args.writeMultiBinPct)
					.append('%');
			argsHdrGeneral.append(", single bin: ")
					.append(100 - args.writeMultiBinPct)
					.append("%)")
					.append('\n');
		}

		argsHdrGeneral.append("keys: ")
				.append(this.nKeys)
				.append(", start key: ")
				.append(this.startKey)
				.append(", transactions: ")
				.append(args.transactionLimit)
				.append(", bins: ")
				.append(args.nBins)
				.append(", random values: ")
				.append(args.fixedBins == null)
				.append(", throughput: ")
				.append(args.throughput == 0 ? "unlimited" : (args.throughput + " tps"))
				.append(", partitions: ")
				.append(args.partitionIds == null ? "all" : args.partitionIds.toString())
				.append('\n');

		System.out.println(argsHdrGeneral);

		argsHdrPolicies.append("client policy:")
				.append('\n');
		argsHdrPolicies.append("    loginTimeout: ")
				.append(clientPolicy.loginTimeout)
				.append(", tendTimeout: ")
				.append(clientPolicy.timeout)
				.append(", tendInterval: ")
				.append(clientPolicy.tendInterval)
				.append(", maxSocketIdle: ")
				.append(clientPolicy.maxSocketIdle)
				.append(", maxErrorRate: ")
				.append(clientPolicy.maxErrorRate)
				.append('\n');
		argsHdrPolicies.append("    errorRateWindow: ")
				.append(clientPolicy.errorRateWindow)
				.append(", minConnsPerNode: ")
				.append(clientPolicy.minConnsPerNode)
				.append(", maxConnsPerNode: ")
				.append(clientPolicy.maxConnsPerNode)
				.append(", asyncMinConnsPerNode: ")
				.append(clientPolicy.asyncMinConnsPerNode)
				.append(", asyncMaxConnsPerNode: ")
				.append(clientPolicy.asyncMaxConnsPerNode)
				.append('\n');

		if (args.workload != Workload.INITIALIZE) {
			argsHdrPolicies.append("read policy:")
					.append('\n');
			argsHdrPolicies.append("    connectTimeout: ")
					.append(args.readPolicy.connectTimeout)
					.append(", socketTimeout: ")
					.append(args.readPolicy.socketTimeout)
					.append(", totalTimeout: ")
					.append(args.readPolicy.totalTimeout)
					.append(", timeoutDelay: ")
					.append(args.readPolicy.timeoutDelay)
					.append(", maxRetries: ")
					.append(args.readPolicy.maxRetries)
					.append(", sleepBetweenRetries: ")
					.append(args.readPolicy.sleepBetweenRetries)
					.append('\n');

			argsHdrPolicies.append("    readModeAP: ")
					.append(args.readPolicy.readModeAP)
					.append(", readModeSC: ")
					.append(args.readPolicy.readModeSC)
					.append(", replica: ")
					.append(args.readPolicy.replica)
					.append(", readTouchTtlPercent: ")
					.append(args.readPolicy.readTouchTtlPercent)
					.append(", reportNotFound: ")
					.append(args.reportNotFound)
					.append('\n');
		}

		argsHdrPolicies.append("write policy:")
				.append('\n');
		argsHdrPolicies.append("    connectTimeout: ")
				.append(args.writePolicy.connectTimeout)
				.append(", socketTimeout: ")
				.append(args.writePolicy.socketTimeout)
				.append(", totalTimeout: ")
				.append(args.writePolicy.totalTimeout)
				.append(", timeoutDelay: ")
				.append(args.writePolicy.timeoutDelay)
				.append(", maxRetries: ")
				.append(args.writePolicy.maxRetries)
				.append(", sleepBetweenRetries: ")
				.append(args.writePolicy.sleepBetweenRetries)
				.append('\n');

		argsHdrPolicies.append("    commitLevel: ")
				.append(args.writePolicy.commitLevel)
				.append(", expiration: ")
				.append(args.writePolicy.expiration)
				.append('\n');

		System.out.println(argsHdrPolicies);

		if (args.batchSize > 1) {
			argsHdrOther.append("batch size: ")
					.append(args.batchSize)
					.append('\n');
		}

		if (this.asyncEnabled) {
			argsHdrOther.append("Async ").append(this.eventLoopType)
					.append(": MaxCommands ")
					.append(this.asyncMaxCommands)
					.append(", EventLoops: ")
					.append(this.eventLoopSize)
					.append('\n');
		}
		else {
			argsHdrOther.append("Sync: connPoolsPerNode: ")
					.append(clientPolicy.connPoolsPerNode)
					.append('\n');
		}

		int binCount = 0;

		for (DBObjectSpec spec : args.objectSpec) {
			argsHdrOther.append("bin[")
					.append(binCount)
					.append("]: ");

			switch (spec.type) {
				case INTEGER:
					argsHdrOther.append("integer")
							.append('\n');
					break;

				case STRING:
					argsHdrOther.append("string[")
							.append(spec.size)
							.append("]")
							.append('\n');
					break;

				case BYTES:
					argsHdrOther.append("byte[")
							.append(spec.size)
							.append("]")
							.append('\n');
					break;

				case RANDOM:
					argsHdrOther.append("random[")
							.append(spec.size * 8)
							.append("]")
							.append('\n');
					break;

				case TIMESTAMP:
					argsHdrOther.append("timestamp")
							.append('\n');
					break;
			}
			binCount++;
		}

		argsHdrOther.append("debug: ")
				.append(args.debug)
				.append('\n');

		System.out.println(argsHdrOther);

	}
}
