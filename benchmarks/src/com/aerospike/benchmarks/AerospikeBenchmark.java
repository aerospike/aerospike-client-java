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

import picocli.AutoComplete.GenerateCompletion;
import picocli.CommandLine.ArgGroup;
import picocli.CommandLine.Command;
import picocli.CommandLine.Model.CommandSpec;
import picocli.CommandLine.Spec;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.Host;
import com.aerospike.client.IAerospikeClient;
import com.aerospike.client.Key;
import com.aerospike.client.Log;
import com.aerospike.client.Log.Level;
import com.aerospike.client.Value;
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
import com.aerospike.client.util.Util;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.epoll.EpollEventLoopGroup;
import io.netty.channel.kqueue.KQueueEventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.incubator.channel.uring.IOUringEventLoopGroup;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Callable;
import java.util.concurrent.Executors;

@Command(
	name = Constants.ASBENCH_COMMAND,
	mixinStandardHelpOptions = false,
	versionProvider = Main.VersionProvider.class,
	description = "%n" + Constants.USAGE_MESSAGE + "%n",
	descriptionHeading = "%nDescription:%n",
	footerHeading = "%n %n",
	footer = "For more information, visit https://www.aerospike.com. Copyright (c) 2025",
	subcommands = {GenerateCompletion.class},
	usageHelpAutoWidth = true
)
public class AerospikeBenchmark implements Callable<Integer>, Log.Callback {
	@Spec
	public CommandSpec spec;

	@ArgGroup(exclusive = false, heading = "%n" + Constants.CONNECTION_OPTIONS_HEADING + "%n")
	public ConnectionOptions connectionOptions;

	@ArgGroup(exclusive = false, heading = "%n" + Constants.WORKLOAD_OPTIONS_HEADING + "%n")
	public WorkloadOptions workloadOptions;

	@ArgGroup(exclusive = false, heading = "%n" + Constants.BENCHMARK_OPTIONS_HEADING + "%n")
	public BenchmarkOptions benchmarkOptions;

	@ArgGroup(exclusive = false, heading = "%n" + Constants.HELP_OPTIONS_HEADING + "%n")
	public HelpOptions helpOptions;

	private static final DateTimeFormatter TimeFormatter =
		DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS");
	public static List<String> keyList = null;

	private final Arguments args = new Arguments();
	private Host[] hosts;
	private EventLoopType eventLoopType = EventLoopType.DIRECT_NIO;
	private int port = 3000;
	private long numberOfKeys;
	private long startKey;
	private int numberOfThreads;
	private int asyncMaxCommands = 100;
	private int eventLoopSize = 1;
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

	@Override
	public Integer call() throws Exception {
		// arg groups can be null, if user doesn't pass any arguments
		if (connectionOptions == null) {
			connectionOptions = new ConnectionOptions();
		}
		if (workloadOptions == null) {
			workloadOptions = new WorkloadOptions();
		}
		if (benchmarkOptions == null) {
			benchmarkOptions = new BenchmarkOptions();
		}
		initialize(connectionOptions, workloadOptions, benchmarkOptions);
		runBenchmarks();
		return 0;
	}

	public void initialize(ConnectionOptions connOpts, WorkloadOptions workloadOpts, BenchmarkOptions benchmarkOpts) throws Exception {
		boolean hasTxns = false;

		if (benchmarkOpts.isAsync()) {
			this.asyncEnabled = true;
		}

		args.readPolicy = clientPolicy.readPolicyDefault;
		args.writePolicy = clientPolicy.writePolicyDefault;
		args.batchPolicy = clientPolicy.batchPolicyDefault;

		args.writePolicy.expiration = workloadOpts.getExpirationTime();

		if (workloadOpts.getReadTouchTtlPercent() != null) {
			args.readPolicy.readTouchTtlPercent = workloadOpts.getReadTouchTtlPercent();
		}

		this.port = connOpts.getPort();

		this.hosts = Host.parseHosts(connOpts.getHosts(), this.port);

		if (connOpts.getLoginTimeout() != null) {
			clientPolicy.loginTimeout = connOpts.getLoginTimeout();
		}

		if (connOpts.getTendTimeout() != null) {
			clientPolicy.timeout = connOpts.getTendTimeout();
		}

		if (connOpts.getTendInterval() != null) {
			clientPolicy.tendInterval = connOpts.getTendInterval();
		}

		if (connOpts.isTlsEnable()) {
			clientPolicy.tlsPolicy = new TlsPolicy();

			if (connOpts.getTlsProtocols() != null) {
				clientPolicy.tlsPolicy.protocols = connOpts.getTlsProtocols().split(",");
			}

			if (connOpts.getTlsCipherSuite() != null) {
				clientPolicy.tlsPolicy.ciphers = connOpts.getTlsCipherSuite().split(",");
			}

			if (connOpts.getTlsRevoke() != null) {
				clientPolicy.tlsPolicy.revokeCertificates = Util.toBigIntegerArray(connOpts.getTlsRevoke());
			}

			if (connOpts.isTlsLoginOnly()) {
				clientPolicy.tlsPolicy.forLoginOnly = true;
			}
		}

		if (connOpts.getAuthMode() != null) {
			clientPolicy.authMode = AuthMode.valueOf(connOpts.getAuthMode().toUpperCase());
		}

		if (connOpts.getUser() != null) {
			clientPolicy.user = connOpts.getUser();
		}
		if (connOpts.getPassword() != null) {
			clientPolicy.password = connOpts.getPassword();
		}

		args.namespace = workloadOpts.getNamespace();

		if (workloadOpts.getBatchNamespaces() != null) {
			args.batchNamespaces = workloadOpts.getBatchNamespaces().split(",");
		}

		args.setName = workloadOpts.getSet();

		if (connOpts.getClusterName() != null) {
			clientPolicy.clusterName = connOpts.getClusterName();
		}

		if (connOpts.isServicesAlternate()) {
			clientPolicy.useServicesAlternate = true;
		}

		if (connOpts.getMaxSocketIdle() != null) {
			clientPolicy.maxSocketIdle = connOpts.getMaxSocketIdle();
		}

		if (connOpts.getMaxErrorRate() != null) {
			clientPolicy.maxErrorRate = connOpts.getMaxErrorRate();
		}

		if (connOpts.getErrorRateWindow() != null) {
			clientPolicy.errorRateWindow = connOpts.getErrorRateWindow();
		}

		if (connOpts.getMinConnsPerNode() != null) {
			clientPolicy.minConnsPerNode = connOpts.getMinConnsPerNode();
		}

		if (connOpts.getMaxConnsPerNode() != null) {
			clientPolicy.maxConnsPerNode = connOpts.getMaxConnsPerNode();
		}

		if (connOpts.getAsyncMinConnsPerNode() != null) {
			clientPolicy.asyncMinConnsPerNode = connOpts.getAsyncMinConnsPerNode();
		}

		clientPolicy.asyncMaxConnsPerNode = connOpts.getAsyncMaxConnsPerNode();

		this.numberOfKeys = workloadOpts.getKeys();

		if (workloadOpts.getMrtSize() != null) {
			this.mrtEnabled = true;
			this.keysPerMRT = workloadOpts.getMrtSize();
			if (this.keysPerMRT < 1 || this.keysPerMRT > this.numberOfKeys) {
				throw new Exception("Invalid mrtSize value");
			}
			long rem = this.numberOfKeys % this.keysPerMRT;
			if (rem != 0) {
				throw new Exception("Invalid key distribution per MRT");
			}
			this.nMRTs = this.numberOfKeys / this.keysPerMRT;
		}

		if (workloadOpts.getStartKey() != null) {
			this.startKey = workloadOpts.getStartKey();
		}

		// Variables setting in case of command arguments passed with keys in File
		if (workloadOpts.getKeyFile() != null) {
			if (startKey + numberOfKeys > Integer.MAX_VALUE) {
				throw new Exception(
					"Invalid arguments when keyFile specified. startKey "
						+ startKey
						+ " + keys "
						+ numberOfKeys
						+ " must be <= "
						+ Integer.MAX_VALUE);
			}

			this.filepath = workloadOpts.getKeyFile();
			// Load the file
			keyList = Utils.readKeyFromFile(filepath);
			if (keyList.isEmpty()) {
				throw new Exception("File : '" + filepath + "' is empty,this file can't be processed.");
			}
			this.numberOfKeys = keyList.size();
			this.startKey = 0;

			if (workloadOpts.getKeyType() != null) {
				String keyType = workloadOpts.getKeyType();

				if (keyType.equals("S")) {
					args.keyType = KeyType.STRING;
				} else if (keyType.equals("I")) {
					if (Utils.isNumeric(keyList.get(0))) {
						args.keyType = KeyType.INTEGER;
					} else {
						throw new Exception(
							"Invalid keyType '" + keyType + "' Key type doesn't match with file content type.");
					}
				} else {
					throw new Exception("Invalid keyType: " + keyType);
				}
			} else {
				args.keyType = KeyType.STRING;
			}
		}

		args.binNameBase = workloadOpts.getBinNameBase();
		args.nBins = workloadOpts.getBins();

		if (workloadOpts.getObjectSpec() != null) {
			String[] objectsArr = workloadOpts.getObjectSpec().split(",");
			args.objectSpec = new DBObjectSpec[objectsArr.length];

			for (int i = 0; i < objectsArr.length; i++) {
				try {
					DBObjectSpec spec = new DBObjectSpec(objectsArr[i]);
					args.objectSpec[i] = spec;
				} catch (Throwable t) {
					throw new Exception("Invalid object spec: " + objectsArr[i] + "\n" + t.getMessage());
				}
			}
		} else {
			// If the object is not specified, it has one bin of integer type.
			args.objectSpec = new DBObjectSpec[1];
			args.objectSpec[0] = new DBObjectSpec();
		}

		if (workloadOpts.getKeyFile() != null) {
			args.workload = Workload.READ_FROM_FILE;
		} else {
			args.workload = Workload.READ_UPDATE;
		}

		args.readPct = 50;
		args.readMultiBinPct = 100;
		args.writeMultiBinPct = 100;

		if (workloadOpts.getWorkload() != null) {
			String[] workloadOptions = workloadOpts.getWorkload().split(",");
			String workloadType = workloadOptions[0];

			if (workloadType.equals("I")) {
				args.workload = Workload.INITIALIZE;
				this.initialize = true;

				if (workloadOptions.length > 1) {
					throw new Exception(
						"Invalid workload number of arguments: " + workloadOptions.length + " Expected 1.");
				}
			} else if (workloadType.equals("RU") || workloadType.equals("RR")) {

				if (workloadType.equals("RR")) {
					args.writePolicy.recordExistsAction = RecordExistsAction.REPLACE;
					args.workload = Workload.READ_REPLACE;
				}

				if (workloadOptions.length < 2 || workloadOptions.length > 4) {
					throw new Exception(
						"Invalid workload number of arguments: "
							+ workloadOptions.length
							+ " Expected 2 to 4.");
				}

				if (workloadOptions.length >= 2) {
					args.readPct = Integer.parseInt(workloadOptions[1]);

					if (args.readPct < 0 || args.readPct > 100) {
						throw new Exception("Read-update workload read percentage must be between 0 and 100");
					}
				}

				if (workloadOptions.length >= 3) {
					args.readMultiBinPct = Integer.parseInt(workloadOptions[2]);
				}

				if (workloadOptions.length >= 4) {
					args.writeMultiBinPct = Integer.parseInt(workloadOptions[3]);
				}
			} else if (workloadType.equals("RMU")) {
				args.workload = Workload.READ_MODIFY_UPDATE;

				if (workloadOptions.length > 1) {
					throw new Exception(
						"Invalid workload number of arguments: " + workloadOptions.length + " Expected 1.");
				}
			} else if (workloadType.equals("RMI")) {
				args.workload = Workload.READ_MODIFY_INCREMENT;

				if (workloadOptions.length > 1) {
					throw new Exception(
						"Invalid workload number of arguments: " + workloadOptions.length + " Expected 1.");
				}
			} else if (workloadType.equals("RMD")) {
				args.workload = Workload.READ_MODIFY_DECREMENT;

				if (workloadOptions.length > 1) {
					throw new Exception(
						"Invalid workload number of arguments: " + workloadOptions.length + " Expected 1.");
				}
			} else if (workloadType.equals("TXN")) {
				args.workload = Workload.TRANSACTION;
				args.transactionalWorkload = new TransactionalWorkload(workloadOptions);
				hasTxns = true;
			} else {
				throw new Exception("Unknown workload: " + workloadType);
			}
		}

		if (workloadOpts.getThroughput() != null) {
			args.throughput = workloadOpts.getThroughput();
		}

		if (workloadOpts.getTransactions() != null) {
			args.transactionLimit = workloadOpts.getTransactions();
		}

		if (connOpts.getConnectTimeout() != null) {
			args.readPolicy.connectTimeout = connOpts.getConnectTimeout();
			args.writePolicy.connectTimeout = connOpts.getConnectTimeout();
			args.batchPolicy.connectTimeout = connOpts.getConnectTimeout();
		}

		if (connOpts.getTimeout() != null) {
			args.readPolicy.socketTimeout = connOpts.getTimeout();
			args.readPolicy.totalTimeout = connOpts.getTimeout();
			args.writePolicy.socketTimeout = connOpts.getTimeout();
			args.writePolicy.totalTimeout = connOpts.getTimeout();
			args.batchPolicy.socketTimeout = connOpts.getTimeout();
			args.batchPolicy.totalTimeout = connOpts.getTimeout();
		}

		if (connOpts.getSocketTimeout() != null) {
			args.readPolicy.socketTimeout = connOpts.getSocketTimeout();
			args.writePolicy.socketTimeout = connOpts.getSocketTimeout();
			args.batchPolicy.socketTimeout = connOpts.getSocketTimeout();
		}

		if (connOpts.getReadSocketTimeout() != null) {
			args.readPolicy.socketTimeout = connOpts.getReadSocketTimeout();
			args.batchPolicy.socketTimeout = connOpts.getReadSocketTimeout();
		}

		if (connOpts.getWriteSocketTimeout() != null) {
			args.writePolicy.socketTimeout = connOpts.getWriteSocketTimeout();
		}

		if (connOpts.getTotalTimeout() != null) {
			args.readPolicy.totalTimeout = connOpts.getTotalTimeout();
			args.writePolicy.totalTimeout = connOpts.getTotalTimeout();
			args.batchPolicy.totalTimeout = connOpts.getTotalTimeout();
		}

		if (connOpts.getReadTotalTimeout() != null) {
			args.readPolicy.totalTimeout = connOpts.getReadTotalTimeout();
			args.batchPolicy.totalTimeout = connOpts.getReadTotalTimeout();
		}

		if (connOpts.getWriteTotalTimeout() != null) {
			args.writePolicy.totalTimeout = connOpts.getWriteTotalTimeout();
		}

		if (connOpts.getTimeoutDelay() != null) {
			args.readPolicy.timeoutDelay = connOpts.getTimeoutDelay();
			args.writePolicy.timeoutDelay = connOpts.getTimeoutDelay();
			args.batchPolicy.timeoutDelay = connOpts.getTimeoutDelay();
		}

		if (workloadOpts.getMaxRetries() != null) {
			args.readPolicy.maxRetries = workloadOpts.getMaxRetries();
			args.writePolicy.maxRetries = workloadOpts.getMaxRetries();
			args.batchPolicy.maxRetries = workloadOpts.getMaxRetries();
		}

		if (workloadOpts.getSleepBetweenRetries() != null) {
			args.readPolicy.sleepBetweenRetries = workloadOpts.getSleepBetweenRetries();
			args.writePolicy.sleepBetweenRetries = workloadOpts.getSleepBetweenRetries();
			args.batchPolicy.sleepBetweenRetries = workloadOpts.getSleepBetweenRetries();
		}

		if (workloadOpts.getRackId() != null) {
			clientPolicy.rackId = workloadOpts.getRackId();
		}

		if (workloadOpts.getReplica() != null) {
			String replica = workloadOpts.getReplica();

			if (replica.equals("master")) {
				args.readPolicy.replica = Replica.MASTER;
				args.batchPolicy.replica = Replica.MASTER;
			} else if (replica.equals("any")) {
				args.readPolicy.replica = Replica.MASTER_PROLES;
				args.batchPolicy.replica = Replica.MASTER_PROLES;
			} else if (replica.equals("sequence")) {
				args.readPolicy.replica = Replica.SEQUENCE;
				args.batchPolicy.replica = Replica.SEQUENCE;
			} else if (replica.equals("preferRack")) {
				args.readPolicy.replica = Replica.PREFER_RACK;
				args.batchPolicy.replica = Replica.PREFER_RACK;
				clientPolicy.rackAware = true;
			} else {
				throw new Exception("Invalid replica: " + replica);
			}
		}

		// Leave this in for legacy reasons.
		if (benchmarkOpts.isProleDistribution()) {
			args.readPolicy.replica = Replica.MASTER_PROLES;
		}

		if (workloadOpts.getReadModeAp() != null) {
			String level = workloadOpts.getReadModeAp().toUpperCase();
			ReadModeAP mode = ReadModeAP.valueOf(level);
			args.readPolicy.readModeAP = mode;
			args.writePolicy.readModeAP = mode;
			args.batchPolicy.readModeAP = mode;
		}

		if (workloadOpts.getReadModeSc() != null) {
			String level = workloadOpts.getReadModeSc().toUpperCase();
			ReadModeSC mode = ReadModeSC.valueOf(level);
			args.readPolicy.readModeSC = mode;
			args.writePolicy.readModeSC = mode;
			args.batchPolicy.readModeSC = mode;
		}

		if (workloadOpts.getCommitLevel() != null) {
			String level = workloadOpts.getCommitLevel();
			if (level.equals("master")) {
				args.writePolicy.commitLevel = CommitLevel.COMMIT_MASTER;
			}
		}

		if (connOpts.getConnPoolsPerNode() != null) {
			clientPolicy.connPoolsPerNode = connOpts.getConnPoolsPerNode();
		}

		this.numberOfThreads = benchmarkOpts.getThreads();

		if (benchmarkOpts.getVirtualThreads() != null) {
			this.useVirtualThreads = true;
			this.numberOfThreads = benchmarkOpts.getVirtualThreads();
		}

		if (benchmarkOpts.isReportNotFound()) {
			args.reportNotFound = true;
		}

		if (benchmarkOpts.isDebug()) {
			args.debug = true;
		}

		if (benchmarkOpts.getBatchSize() != null) {
			args.batchSize = benchmarkOpts.getBatchSize();

			if(mrtEnabled) {
				throw new Exception("MRT does not support the batch size.");
			}
		}

		if (benchmarkOpts.isBatchShowNodes()) {
			this.batchShowNodes = true;

			if(mrtEnabled) {
				throw new Exception("MRT does not support the batch show nodes operation.");
			}
		}

		if (benchmarkOpts.getAsyncMaxCommands() != null) {
			this.asyncMaxCommands = benchmarkOpts.getAsyncMaxCommands();
		}

		if (benchmarkOpts.getEventLoops() != null) {
			this.eventLoopSize = benchmarkOpts.getEventLoops();
		}

		if (benchmarkOpts.getLatency() != null) {
			String latencyString = benchmarkOpts.getLatency();
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
				counters.read.latency = new LatencyManagerYcsb(" read", warmupCount);
				counters.write.latency = new LatencyManagerYcsb("write", warmupCount);
				if (hasTxns) {
					counters.transaction.latency = new LatencyManagerYcsb(" txns", warmupCount);
				}
			} else {
				boolean alt = false;
				int index = 0;

				if ("alt".equalsIgnoreCase(latencyOpts[index])) {
					if (latencyOpts.length > 4) {
						throw new Exception(getLatencyUsage(latencyString));
					}
					alt = true;
					index++;
				} else {
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

				if (alt) {
					counters.read.latency = new LatencyManagerAlternate(columns, bitShift, showMicroSeconds);
					counters.write.latency = new LatencyManagerAlternate(columns, bitShift, showMicroSeconds);
					if (hasTxns) {
						counters.transaction.latency =
							new LatencyManagerAlternate(columns, bitShift, showMicroSeconds);
					}
				} else {
					counters.read.latency = new LatencyManagerAerospike(columns, bitShift, showMicroSeconds);
					counters.write.latency = new LatencyManagerAerospike(columns, bitShift, showMicroSeconds);
					if (hasTxns) {
						counters.transaction.latency =
							new LatencyManagerAerospike(columns, bitShift, showMicroSeconds);
					}
				}
			}
		}

		if (!workloadOpts.isRandom()) {
			args.setFixedBins();
		}

		if (benchmarkOpts.isNetty()) {
			this.eventLoopType = EventLoopType.NETTY_NIO;
		}

		if (benchmarkOpts.isNettyEpoll()) {
			this.eventLoopType = EventLoopType.NETTY_EPOLL;
		}

		if (benchmarkOpts.getEventLoopType() != null) {
			this.eventLoopType = EventLoopType.valueOf(benchmarkOpts.getEventLoopType());
		}

		if (workloadOpts.getUdfPackageName() != null) {
			args.udfPackageName = workloadOpts.getUdfPackageName();
		}

		if (workloadOpts.getUdfFunctionName() != null) {
			if (args.udfPackageName == null) {
				throw new Exception("Udf Package name missing");
			}
			args.udfFunctionName = workloadOpts.getUdfFunctionName();
		}

		if (workloadOpts.getUdfFunctionValues() != null) {
			Object[] udfVals = workloadOpts.getUdfFunctionValues().split(",");
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

		if (workloadOpts.isSendKey()) {
			args.writePolicy.sendKey = true;
		}

		if (workloadOpts.getPartitionIds() != null) {
			String[] pids = workloadOpts.getPartitionIds().split(",");

			Set<Integer> partitionIds = new HashSet<>();

			for (String pid : pids) {
				int partitionId = -1;

				try {
					partitionId = Integer.parseInt(pid);
				} catch (NumberFormatException nfe) {
					throw new Exception("Partition ID has to be an integer");
				}

				if (partitionId < 0 || partitionId >= Node.PARTITIONS) {
					throw new Exception("Partition ID has to be a value between 0 and " + Node.PARTITIONS);
				}

				partitionIds.add(partitionId);
			}

			args.partitionIds = partitionIds;
		}

		String threadType = useVirtualThreads ? "virtual" : "OS";

		System.out.println(
			"Benchmark: "
				+ this.hosts[0]
				+ ", namespace: "
				+ args.namespace
				+ ", set: "
				+ (args.setName.length() > 0 ? args.setName : "<empty>")
				+ ", "
				+ threadType
				+ " threads: "
				+ this.numberOfThreads
				+ ", workload: "
				+ args.workload);

		if (args.workload == Workload.READ_UPDATE || args.workload == Workload.READ_REPLACE) {
			System.out.print("read: " + args.readPct + '%');
			System.out.print(" (all bins: " + args.readMultiBinPct + '%');
			System.out.print(", single bin: " + (100 - args.readMultiBinPct) + "%)");

			System.out.print(", write: " + (100 - args.readPct) + '%');
			System.out.print(" (all bins: " + args.writeMultiBinPct + '%');
			System.out.println(", single bin: " + (100 - args.writeMultiBinPct) + "%)");
		}

		System.out.println(
			"keys: "
				+ this.numberOfKeys
				+ ", start key: "
				+ this.startKey
				+ ", transactions: "
				+ args.transactionLimit
				+ ", bins: "
				+ args.nBins
				+ ", random values: "
				+ (args.fixedBins == null)
				+ ", throughput: "
				+ (args.throughput == 0 ? "unlimited" : (args.throughput + " tps"))
				+ ", partitions: "
				+ (args.partitionIds == null ? "all" : args.partitionIds.toString()));

		System.out.println("client policy:");
		System.out.println(
			"    loginTimeout: "
				+ clientPolicy.loginTimeout
				+ ", tendTimeout: "
				+ clientPolicy.timeout
				+ ", tendInterval: "
				+ clientPolicy.tendInterval
				+ ", maxSocketIdle: "
				+ clientPolicy.maxSocketIdle
				+ ", maxErrorRate: "
				+ clientPolicy.maxErrorRate);
		System.out.println(
			"    errorRateWindow: "
				+ clientPolicy.errorRateWindow
				+ ", minConnsPerNode: "
				+ clientPolicy.minConnsPerNode
				+ ", maxConnsPerNode: "
				+ clientPolicy.maxConnsPerNode
				+ ", asyncMinConnsPerNode: "
				+ clientPolicy.asyncMinConnsPerNode
				+ ", asyncMaxConnsPerNode: "
				+ clientPolicy.asyncMaxConnsPerNode);

		if (args.workload != Workload.INITIALIZE) {
			System.out.println("read policy:");
			System.out.println(
				"    connectTimeout: "
					+ args.readPolicy.connectTimeout
					+ ", socketTimeout: "
					+ args.readPolicy.socketTimeout
					+ ", totalTimeout: "
					+ args.readPolicy.totalTimeout
					+ ", timeoutDelay: "
					+ args.readPolicy.timeoutDelay
					+ ", maxRetries: "
					+ args.readPolicy.maxRetries
					+ ", sleepBetweenRetries: "
					+ args.readPolicy.sleepBetweenRetries);

			System.out.println(
				"    readModeAP: "
					+ args.readPolicy.readModeAP
					+ ", readModeSC: "
					+ args.readPolicy.readModeSC
					+ ", replica: "
					+ args.readPolicy.replica
					+ ", readTouchTtlPercent: "
					+ args.readPolicy.readTouchTtlPercent
					+ ", reportNotFound: "
					+ args.reportNotFound);
		}

		System.out.println("write policy:");
		System.out.println(
			"    connectTimeout: "
				+ args.writePolicy.connectTimeout
				+ ", socketTimeout: "
				+ args.writePolicy.socketTimeout
				+ ", totalTimeout: "
				+ args.writePolicy.totalTimeout
				+ ", timeoutDelay: "
				+ args.writePolicy.timeoutDelay
				+ ", maxRetries: "
				+ args.writePolicy.maxRetries
				+ ", sleepBetweenRetries: "
				+ args.writePolicy.sleepBetweenRetries);

		System.out.println(
			"    commitLevel: "
				+ args.writePolicy.commitLevel
				+ ", expiration: "
				+ args.writePolicy.expiration);

		if (args.batchSize > 1) {
			System.out.println("batch size: " + args.batchSize);
		}

		if (this.asyncEnabled) {
			System.out.println(
				"Async "
					+ this.eventLoopType
					+ ": MaxCommands "
					+ this.asyncMaxCommands
					+ ", EventLoops: "
					+ this.eventLoopSize);
		} else {
			System.out.println("Sync: connPoolsPerNode: " + clientPolicy.connPoolsPerNode);
		}

		int binCount = 0;

		for (DBObjectSpec spec : args.objectSpec) {
			System.out.print("bin[" + binCount + "]: ");

			switch (spec.type) {
				case INTEGER:
					System.out.println("integer");
					break;

				case STRING:
					System.out.println("string[" + spec.size + "]");
					break;

				case BYTES:
					System.out.println("byte[" + spec.size + "]");
					break;

				case RANDOM:
					System.out.println("random[" + (spec.size * 8) + "]");
					break;

				case TIMESTAMP:
					System.out.println("timestamp");
					break;

				default:
					System.out.println("unknown spec" + spec.type);
					break;
			}
			binCount++;
		}

		System.out.println("debug: " + args.debug);

		Log.Level level = (args.debug) ? Log.Level.DEBUG : Log.Level.INFO;
		Log.setLevel(level);
		Log.setCallback(this);
		args.updatePolicy = new WritePolicy(args.writePolicy);
		args.updatePolicy.recordExistsAction = RecordExistsAction.UPDATE;
		args.replacePolicy = new WritePolicy(args.writePolicy);
		args.replacePolicy.recordExistsAction = RecordExistsAction.REPLACE;

		clientPolicy.failIfNotConnected = true;
	}

	private String getLatencyUsage(String latencyString) {
		return "Latency usage: ycsb[,<warmup count>] | [alt,]<columns>,<range shift increment>[,us|ms] "
			+ " Received: "
			+ latencyString;
	}

	public void runBenchmarks() throws Exception {
		if (this.asyncEnabled) {
			EventPolicy eventPolicy = new EventPolicy();

			if (args.readPolicy.socketTimeout > 0
				&& args.readPolicy.socketTimeout < eventPolicy.minTimeout) {
				eventPolicy.minTimeout = args.readPolicy.socketTimeout;
			}

			if (args.writePolicy.socketTimeout > 0
				&& args.writePolicy.socketTimeout < eventPolicy.minTimeout) {
				eventPolicy.minTimeout = args.writePolicy.socketTimeout;
			}
			EventLoopGroup group = null;

			switch (this.eventLoopType) {
				default:
				case DIRECT_NIO:
					eventLoops = new NioEventLoops(eventPolicy, this.eventLoopSize);
					break;

				case NETTY_NIO:
					group = new NioEventLoopGroup(this.eventLoopSize);
					eventLoops = new NettyEventLoops(eventPolicy, group, this.eventLoopType);
					break;

				case NETTY_EPOLL:
					group = new EpollEventLoopGroup(this.eventLoopSize);
					eventLoops = new NettyEventLoops(eventPolicy, group, this.eventLoopType);
					break;

				case NETTY_KQUEUE:
					group = new KQueueEventLoopGroup(this.eventLoopSize);
					eventLoops = new NettyEventLoops(eventPolicy, group, this.eventLoopType);
					break;

				case NETTY_IOURING:
					group = new IOUringEventLoopGroup(this.eventLoopSize);
					eventLoops = new NettyEventLoops(eventPolicy, group, this.eventLoopType);
					break;
			}

			try {
				clientPolicy.eventLoops = eventLoops;

				if (clientPolicy.asyncMaxConnsPerNode < this.asyncMaxCommands) {
					clientPolicy.asyncMaxConnsPerNode = this.asyncMaxCommands;
				}

				IAerospikeClient client = new AerospikeClient(clientPolicy, hosts);

				try {
					if (mrtEnabled) {
						if (initialize) {
							throw new Exception("MRT does not support asynchronous insertion during initialization.");
						}
						else {
							throw new Exception("MRT does not support asynchronous batch and RU workload.");
						}
					} else {
						if (initialize) {
							doAsyncInserts(client);
						} else {
							showBatchNodes(client);
							doAsyncRwTest(client);
						}
					}
				} finally {
					client.close();
				}
			} finally {
				eventLoops.close();
			}
		} else {
			IAerospikeClient client = new AerospikeClient(clientPolicy, hosts);

			try {
				if (mrtEnabled) {
					if (initialize) {
						doMRTInserts(client);
					}
					else {
						doMRTRWTest(client);
					}
				} else {
					if (initialize) {
						doInserts(client);
					} else {
						showBatchNodes(client);
						doRwTest(client);
					}
				}
			} finally {
				client.close();
			}
		}
	}

	private void doInserts(IAerospikeClient client) throws Exception {
		ExecutorService es = getExecutorService();

		// Create N insert tasks
		long ntasks =
			this.numberOfThreads < this.numberOfKeys ? this.numberOfThreads : this.numberOfKeys;
		long keysPerTask = this.numberOfKeys / ntasks;
		long rem = this.numberOfKeys - (keysPerTask * ntasks);
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

	private void doAsyncInserts(IAerospikeClient client) throws Exception {
		// Generate asyncMaxCommand writes to seed the event loops.
		// Then start a new command in each command callback.
		// This effectively throttles new command generation, by only allowing
		// asyncMaxCommands at any point in time.
		long maxConcurrentCommands = this.asyncMaxCommands;

		if (maxConcurrentCommands > this.numberOfKeys) {
			maxConcurrentCommands = this.numberOfKeys;
		}

		long keysPerCommand = this.numberOfKeys / maxConcurrentCommands;
		long keysRem = this.numberOfKeys - (keysPerCommand * maxConcurrentCommands);
		long keyStart = this.startKey;

		for (int i = 0; i < maxConcurrentCommands; i++) {
			// Allocate separate tasks for each seed command and reuse them in callbacks.
			long keyCount = (i < keysRem) ? keysPerCommand + 1 : keysPerCommand;

			// Start seed commands on random event loops.
			EventLoop eventLoop = this.eventLoops.next();
			InsertTaskAsync task =
				new InsertTaskAsync(client, eventLoop, args, counters, keyStart, keyCount);
			task.runCommand();
			keyStart += keyCount;
		}
		Thread.sleep(900);
		collectInsertStats();
	}

	private void collectInsertStats() throws Exception {
		long total = 0;

		while (total < this.numberOfKeys) {
			long time = System.currentTimeMillis();

			int numWrites = this.counters.write.count.getAndSet(0);
			int timeoutWrites = this.counters.write.timeouts.getAndSet(0);
			int errorWrites = this.counters.write.errors.getAndSet(0);
			total += numWrites;

			this.counters.periodBegin.set(time);

			LocalDateTime dt =
				Instant.ofEpochMilli(time).atZone(ZoneId.systemDefault()).toLocalDateTime();
			System.out.println(
				dt.format(TimeFormatter)
					+ " write(count="
					+ total
					+ " tps="
					+ numWrites
					+ " timeouts="
					+ timeoutWrites
					+ " errors="
					+ errorWrites
					+ ")");

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

	private void doRwTest(IAerospikeClient client) throws Exception {
		ExecutorService es = getExecutorService();
		RWTask[] tasks = new RWTask[this.numberOfThreads];

		for (int i = 0; i < this.numberOfThreads; i++) {
			RWTaskSync rt = new RWTaskSync(client, args, counters, this.startKey, this.numberOfKeys);
			tasks[i] = rt;
			es.execute(rt);
		}
		Thread.sleep(900);
		collectRwStats(tasks);
		es.shutdown();
	}

	private void doAsyncRwTest(IAerospikeClient client) throws Exception {
		// Generate asyncMaxCommand commands to seed the event loops.
		// Then start a new command in each command callback.
		// This effectively throttles new command generation, by only allowing
		// asyncMaxCommands at any point in time.
		int maxConcurrentCommands = this.asyncMaxCommands;

		if (maxConcurrentCommands > this.numberOfKeys) {
			maxConcurrentCommands = (int) this.numberOfKeys;
		}

		// Create seed commands distributed among event loops.
		RWTask[] tasks = new RWTask[maxConcurrentCommands];

		for (int i = 0; i < maxConcurrentCommands; i++) {
			EventLoop eventLoop = this.clientPolicy.eventLoops.next();
			tasks[i] =
				new RWTaskAsync(client, eventLoop, args, counters, this.startKey, this.numberOfKeys);
		}

		// Start seed commands.
		for (RWTask task : tasks) {
			task.runNextCommand();
		}

		Thread.sleep(900);
		collectRwStats(tasks);
	}

	private void collectRwStats(RWTask[] tasks) throws Exception {
		long transactionTotal = 0;

		while (true) {
			long time = System.currentTimeMillis();
			int notFound = 0;

			if (args.reportNotFound) {
				notFound = this.counters.readNotFound.getAndSet(0);
			}
			this.counters.periodBegin.set(time);

			int numWrites = this.counters.write.count.getAndSet(0);
			int timeoutWrites = this.counters.write.timeouts.getAndSet(0);
			int errorWrites = this.counters.write.errors.getAndSet(0);
			LocalDateTime dt =
				Instant.ofEpochMilli(time).atZone(ZoneId.systemDefault()).toLocalDateTime();
			System.out.print(dt.format(TimeFormatter));
			System.out.print(
				" write(tps="
					+ numWrites
					+ " timeouts="
					+ timeoutWrites
					+ " errors="
					+ errorWrites
					+ ")");

			int numReads = this.counters.read.count.getAndSet(0);
			int timeoutReads = this.counters.read.timeouts.getAndSet(0);
			int errorReads = this.counters.read.errors.getAndSet(0);
			System.out.print(
				" read(tps=" + numReads + " timeouts=" + timeoutReads + " errors=" + errorReads);

			int numTxns = this.counters.transaction.count.getAndSet(0);
			int timeoutTxns = this.counters.transaction.timeouts.getAndSet(0);
			int errorTxns = this.counters.transaction.errors.getAndSet(0);

			if (this.counters.transaction.latency != null) {
				System.out.print(
					" txns(tps=" + numTxns + " timeouts=" + timeoutTxns + " errors=" + errorTxns);
			}
			if (args.reportNotFound) {
				System.out.print(" nf=" + notFound);
			}
			System.out.print(")");

			System.out.print(
				" total(tps="
					+ (numWrites + numReads)
					+ " timeouts="
					+ (timeoutWrites + timeoutReads)
					+ " errors="
					+ (errorWrites + errorReads)
					+ ")");
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
				transactionTotal +=
					numWrites + timeoutWrites + errorWrites + numReads + timeoutReads + errorReads;

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

	private void doMRTInserts(IAerospikeClient client) throws Exception {
		ExecutorService es = getExecutorService();

		// Create N insert tasks
		long ntasks = this.numberOfThreads < this.nMRTs ? this.numberOfThreads : this.nMRTs;
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

	private void collectMRTStats() throws Exception {
		long total = 0;

		while (total < this.numberOfKeys) {
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

	private void doMRTRWTest(IAerospikeClient client) throws Exception {
		ExecutorService es = getExecutorService();
		long ntasks = this.numberOfThreads < this.nMRTs ? this.numberOfThreads : this.nMRTs;
		long mrtsPerTask = this.nMRTs / ntasks;
		long rem = this.nMRTs - (mrtsPerTask * ntasks);
		MRTRWTask[] tasks = new MRTRWTask[this.numberOfThreads];

		for (int i = 0; i < ntasks; i++) {
			long nMrtsPerThread = (i < rem) ? mrtsPerTask + 1 : mrtsPerTask;
			MRTRWTaskSync rt = new MRTRWTaskSync(client, args, counters, nMrtsPerThread, this.startKey,
					this.numberOfKeys, this.keysPerMRT);
			tasks[i] = rt;
			es.execute(rt);
		}
		Thread.sleep(1000);
		collectMRTRWStats(tasks);
		es.shutdownNow();
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
		return useVirtualThreads
			? Executors.newVirtualThreadPerTaskExecutor()
			: Executors.newFixedThreadPool(this.numberOfThreads);
	}

	private void showBatchNodes(IAerospikeClient client) {
		if (!batchShowNodes || args.batchSize <= 1) {
			return;
		}

		// Print target nodes for the first batchSize keys. The keys in each batch
		// transaction
		// are randomly generated, so the actual target nodes may differ in each batch
		// transaction.
		// This is useful to determine how increasing the cluster size also increases
		// the number of
		// batch target nodes, which may result in a performance decrease for batch
		// commands.
		Key[] keys = new Key[args.batchSize];

		for (int i = 0; i < keys.length; i++) {
			keys[i] = new Key(args.namespace, args.setName, i);
		}

		BatchStatus status = new BatchStatus(false);
		List<BatchNode> batchNodes =
			BatchNodeList.generate(client.getCluster(), args.batchPolicy, keys, null, false, status);

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

		System.out.println(
			LocalDateTime.now().format(TimeFormatter)
				+ ' '
				+ level.toString()
				+ " Thread "
				+ name
				+ ' '
				+ message);
	}
}
