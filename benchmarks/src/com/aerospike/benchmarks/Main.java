/*
 * Copyright 2012-2020 Aerospike, Inc.
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
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.Host;
import com.aerospike.client.Log;
import com.aerospike.client.Log.Level;
import com.aerospike.client.Value;
import com.aerospike.client.async.EventLoop;
import com.aerospike.client.async.EventLoopType;
import com.aerospike.client.async.EventLoops;
import com.aerospike.client.async.EventPolicy;
import com.aerospike.client.async.NettyEventLoops;
import com.aerospike.client.async.NioEventLoops;
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
import io.netty.channel.nio.NioEventLoopGroup;

public class Main implements Log.Callback {

	private static final SimpleDateFormat SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
	public static List<String> keyList = null;

	public static void main(String[] args) {
		Main program = null;

		try {
			program = new Main(args);
			program.runBenchmarks();
		}
		catch (UsageException ue) {
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

	private Arguments args = new Arguments();
	private Host[] hosts;
	private EventLoopType eventLoopType = EventLoopType.DIRECT_NIO;
	private int port = 3000;
	private long nKeys;
	private long startKey;
	private int nThreads;
	private int asyncMaxCommands = 100;
	private int eventLoopSize = 1;
	private boolean asyncEnabled;
	private boolean initialize;
	private String filepath;

	private EventLoops eventLoops;
	private ClientPolicy clientPolicy = new ClientPolicy();
	private CounterStore counters = new CounterStore();

	public Main(String[] commandLineArgs) throws Exception {
		boolean hasTxns = false;

		Options options = new Options();
		options.addOption("h", "hosts", true,
			"List of seed hosts in format: " +
			"hostname1[:tlsname][:port1],...\n" +
			"The tlsname is only used when connecting with a secure TLS enabled server. " +
			"If the port is not specified, the default port is used. " +
			"IPv6 addresses must be enclosed in square brackets.\n" +
			"Default: localhost\n" +
			"Examples:\n" +
			"host1\n" +
			"host1:3000,host2:3000\n" +
			"192.168.1.10:cert1:3000,[2001::1111]:cert2:3000\n"
			);
		options.addOption("p", "port", true, "Set the default port on which to connect to Aerospike.");
		options.addOption("U", "user", true, "User name");
		options.addOption("P", "password", true, "Password");
		options.addOption("n", "namespace", true, "Set the Aerospike namespace. Default: test");
		options.addOption("bns","batchNamespaces", true, "Set batch namespaces. Default is regular namespace.");
		options.addOption("s", "set", true, "Set the Aerospike set name. Default: testset");
		options.addOption("c", "clusterName", true, "Set expected cluster name.");

		options.addOption("lt", "loginTimeout", true,
			"Set expected loginTimeout in milliseconds. The timeout is used when user " +
			"authentication is enabled and a node login is being performed. Default: 5000"
			);
		options.addOption("ct", "connectTimeout", true,
			"Set initial host connection timeout on node startup in milliseconds. Default: 1000"
			);
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
			"I | S:<size> | B:<size>\n" +
			"Set the type of object(s) to use in Aerospike transactions. Type can be 'I' " +
			"for integer, 'S' for string, or 'B' for Java blob. If type is 'I' (integer), " +
			"do not set a size (integers are always 8 bytes). If object_type is 'S' " +
			"(string), this value represents the length of the string."
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
			"I | RU,<percent>[,<percent2>][,<percent3>] | RR,<percent>[,<percent2>][,<percent3>], RMU | RMI | RMD\n" +
			"Set the desired workload.\n\n" +
			"   -w I sets a linear 'insert' workload.\n\n" +
			"   -w RU,80 sets a random read-update workload with 80% reads and 20% writes.\n\n" +
			"      100% of reads will read all bins.\n\n" +
			"      100% of writes will write all bins.\n\n" +
			"   -w RU,80,60,30 sets a random multi-bin read-update workload with 80% reads and 20% writes.\n\n" +
			"      60% of reads will read all bins. 40% of reads will read a single bin.\n\n" +
			"      30% of writes will write all bins. 70% of writes will write a single bin.\n\n" +
			"   -w RR,20 sets a random read-replace workload with 20% reads and 80% replace all bin(s) writes.\n\n" +
			"      100% of reads will read all bins.\n\n" +
			"      100% of writes will replace all bins.\n\n" +
			"   -w RMU sets a random read all bins-update one bin workload with 50% reads.\n\n" +
			"   -w RMI sets a random read all bins-increment one integer bin workload with 50% reads.\n\n" +
			"   -w RMD sets a random read all bins-decrement one integer bin workload with 50% reads.\n\n" +
			"   -w TXN,r:1000,w:200,v:20%\n\n" +
			"      form business transactions with 1000 reads, 200 writes with a variation (+/-) of 20%\n\n"
			);
		options.addOption("e", "expirationTime", true,
			"Set expiration time of each record in seconds.\n" +
			" -1: Never expire\n" +
			"  0: Default to namespace expiration time\n" +
			" >0: Actual given expiration time"
			);
		options.addOption("g", "throughput", true,
			"Set a target transactions per second for the client. The client should not exceed this " +
			"average throughput."
			);
		options.addOption("t", "transactions", true,
			"Number of transactions to perform in read/write mode before shutting down. " +
			"The default is to run indefinitely."
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
				"Which replica to use for reads.\n\n" +
				"Values:  master | any | sequence | preferRack.  Default: sequence\n" +
				"master: Always use node containing master partition.\n" +
				"any: Distribute reads across master and proles in round-robin fashion.\n" +
				"sequence: Always try master first. If master fails, try proles in sequence.\n" +
				"preferRack: Always try node on the same rack as the benchmark first. If no nodes on the same rack, use sequence.\n" +
				"Use 'rackId' option to set rack."
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
			"Set the number of threads the client will use to generate load. "
			);
		options.addOption("latency", true,
			"ycsb[,<warmup count>] | [alt,]<columns>,<range shift increment>[,us|ms]\n" +
			"ycsb: Show the timings in ycsb format.\n" +
			"alt: Show both count and pecentage in each elapsed time bucket.\n" +
			"default: Show pecentage in each elapsed time bucket.\n" +
			"<columns>: Number of elapsed time ranges.\n" +
			"<range shift increment>: Power of 2 multiple between each range starting at column 3.\n" +
			"(ms|us): display times in milliseconds (ms, default) or microseconds (us)\n\n" +
			"A latency definition of '-latency 7,1' results in this layout:\n" +
			"    <=1ms >1ms >2ms >4ms >8ms >16ms >32ms\n" +
			"       x%   x%   x%   x%   x%    x%    x%\n" +
			"A latency definition of '-latency 4,3' results in this layout:\n" +
			"    <=1ms >1ms >8ms >64ms\n" +
			"       x%   x%   x%    x%\n\n" +
			"Latency columns are cumulative. If a transaction takes 9ms, it will be included in both the >1ms and >8ms columns."
			);

		options.addOption("N", "reportNotFound", false, "Report not found errors. Data should be fully initialized before using this option.");
		options.addOption("D", "debug", false, "Run benchmarks in debug mode.");
		options.addOption("u", "usage", false, "Print usage.");
		options.addOption("V", "version", false, "Print version.");

		options.addOption("B", "batchSize", true,
			"Enable batch mode with number of records to process in each batch get call. " +
			"Batch mode is valid only for RU (read update) workloads. Batch mode is disabled by default."
			);

		options.addOption("BT", "batchThreads", true,
			"Maximum number of concurrent batch sub-threads for each batch command.\n" +
			"1   : Run each batch node command sequentially.\n" +
			"0   : Run all batch node commands in parallel.\n" +
			"> 1 : Run maximum batchThreads in parallel.  When a node command finshes, start a new one until all finished."
			);

		options.addOption("prole", false, "Distribute reads across proles in round-robin fashion.");

		options.addOption("a", "async", false, "Benchmark asynchronous methods instead of synchronous methods.");
		options.addOption("C", "asyncMaxCommands", true, "Maximum number of concurrent asynchronous database commands.");
		options.addOption("W", "eventLoops", true, "Number of event loop threads when running in asynchronous mode.");
		options.addOption("F", "keyFile", true, "File path to read the keys for read operation.");
		options.addOption("KT", "keyType", true, "Type of the key(String/Integer) in the file, default is String");
		options.addOption("tls", "tlsEnable", false, "Use TLS/SSL sockets");
		options.addOption("tp", "tlsProtocols", true,
				"Allow TLS protocols\n" +
				"Values:  TLSv1,TLSv1.1,TLSv1.2 separated by comma\n" +
				"Default: TLSv1.2"
				);
		options.addOption("tlsCiphers", "tlsCipherSuite", true,
				"Allow TLS cipher suites\n" +
				"Values:  cipher names defined by JVM separated by comma\n" +
				"Default: null (default cipher list provided by JVM)"
				);
		options.addOption("tr", "tlsRevoke", true,
				"Revoke certificates identified by their serial number\n" +
				"Values:  serial numbers separated by comma\n" +
				"Default: null (Do not revoke certificates)"
				);
		options.addOption("tlsLoginOnly", false, "Use TLS/SSL sockets on node login only");
		options.addOption("auth", true, "Authentication mode. Values: " + Arrays.toString(AuthMode.values()));
		options.addOption("netty", false, "Use Netty NIO event loops for async benchmarks");
		options.addOption("nettyEpoll", false, "Use Netty epoll event loops for async benchmarks (Linux only)");
		options.addOption("upn", "udfPackageName", true, "Specify the package name where the udf function is located");
		options.addOption("ufn", "udfFunctionName", true, "Specify the udf function name that must be used in the udf benchmarks");
		options.addOption("ufv","udfFunctionValues",true, "The udf argument values comma separated");
		options.addOption("sendKey", false, "Send key to server");

		// parse the command line arguments
		CommandLineParser parser = new PosixParser();
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

    	args.readPolicy = clientPolicy.readPolicyDefault;
    	args.writePolicy = clientPolicy.writePolicyDefault;
    	args.batchPolicy = clientPolicy.batchPolicyDefault;

    	if (line.hasOption("e")) {
			args.writePolicy.expiration =  Integer.parseInt(line.getOptionValue("e"));
			if (args.writePolicy.expiration < -1) {
				throw new Exception("Invalid expiration: "+ args.writePolicy.expiration + " It should be >= -1");
			}
		}

		if (line.hasOption("port")) {
			this.port = Integer.parseInt(line.getOptionValue("port"));
		}
		else {
			this.port = 3000;
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

		if (line.hasOption("connectTimeout")) {
			clientPolicy.timeout = Integer.parseInt(line.getOptionValue("connectTimeout"));
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

		if (line.hasOption("keys")) {
			this.nKeys = Long.parseLong(line.getOptionValue("keys"));
		} else {
			this.nKeys = 100000;
		}

		if (line.hasOption("startkey")) {
			this.startKey = Long.parseLong(line.getOptionValue("startkey"));
		}

		//Variables setting in case of command arguments passed with keys in File
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
					if (Utils.isNumeric(keyList.get(0))) {
						args.keyType = KeyType.INTEGER;
					} else {
						throw new Exception("Invalid keyType '"+keyType+"' Key type doesn't match with file content type.");
					}
				}
				else {
					throw new Exception("Invalid keyType: "+keyType);
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
			for (int i=0; i<objectsArr.length; i++) {
				String[] objarr = objectsArr[i].split(":");
				DBObjectSpec dbobj = new DBObjectSpec();
				dbobj.type = objarr[0].charAt(0);
				if (objarr.length > 1) {
					dbobj.size = Integer.parseInt(objarr[1]);
				}
				args.objectSpec[i] = dbobj;
			}
		}
		else {
			args.objectSpec = new DBObjectSpec[1];
			DBObjectSpec dbobj = new DBObjectSpec();
			dbobj.type = 'I';	// If the object is not specified, it has one bin of integer type
			args.objectSpec[0] = dbobj;
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

				if (workloadOpts.length >= 2) {
					args.readPct = Integer.parseInt(workloadOpts[1]);

					if (args.readPct < 0 || args.readPct > 100) {
						throw new Exception("Read-update workload read percentage must be between 0 and 100");
					}
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

			if (replica.equals("master")) {
				args.readPolicy.replica = Replica.MASTER;
				args.batchPolicy.replica = Replica.MASTER;
			}
			else if (replica.equals("any")) {
				args.readPolicy.replica = Replica.MASTER_PROLES;
				args.batchPolicy.replica = Replica.MASTER_PROLES;
			}
			else if (replica.equals("sequence")) {
				args.readPolicy.replica = Replica.SEQUENCE;
				args.batchPolicy.replica = Replica.SEQUENCE;
			}
			else if (replica.equals("preferRack")) {
				args.readPolicy.replica = Replica.PREFER_RACK;
				args.batchPolicy.replica = Replica.PREFER_RACK;
				clientPolicy.rackAware = true;
			}
			else {
				throw new Exception("Invalid replica: " + replica);
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

		if (line.hasOption("reportNotFound")) {
			args.reportNotFound = true;
		}

		if (line.hasOption("debug")) {
			args.debug = true;
		}

        if (line.hasOption("batchSize")) {
        	args.batchSize =  Integer.parseInt(line.getOptionValue("batchSize"));
        }

		if (line.hasOption("batchThreads")) {
			args.batchPolicy.maxConcurrentThreads = Integer.parseInt(line.getOptionValue("batchThreads"));
		}

		if (line.hasOption("asyncMaxCommands")) {
        	this.asyncMaxCommands =  Integer.parseInt(line.getOptionValue("asyncMaxCommands"));
        }

        if (line.hasOption("eventLoops")) {
        	this.eventLoopSize =  Integer.parseInt(line.getOptionValue("eventLoops"));
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
				counters.read.latency = new LatencyManagerYcsb(" read", warmupCount);
				counters.write.latency = new LatencyManagerYcsb("write", warmupCount);
				if (hasTxns) {
					counters.transaction.latency = new LatencyManagerYcsb(" txns", warmupCount);
				}
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

				if (alt) {
					counters.read.latency = new LatencyManagerAlternate(columns, bitShift, showMicroSeconds);
					counters.write.latency = new LatencyManagerAlternate(columns, bitShift, showMicroSeconds);
					if (hasTxns) {
						counters.transaction.latency = new LatencyManagerAlternate(columns, bitShift, showMicroSeconds);
					}
				}
				else {
					counters.read.latency = new LatencyManagerAerospike(columns, bitShift, showMicroSeconds);
					counters.write.latency = new LatencyManagerAerospike(columns, bitShift, showMicroSeconds);
					if (hasTxns) {
						counters.transaction.latency = new LatencyManagerAerospike(columns, bitShift, showMicroSeconds);
					}
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

		if(line.hasOption("udfPackageName")){
			args.udfPackageName = line.getOptionValue("udfPackageName");
		}

		if(line.hasOption("udfFunctionName")){
			if(args.udfPackageName == null){
				throw new Exception("Udf Package name missing");
			}
			args.udfFunctionName = line.getOptionValue("udfFunctionName");
		}

		if(line.hasOption("udfFunctionValues")){
			Object[] udfVals = line.getOptionValue("udfFunctionValues").split(",");
			if(args.udfPackageName == null){
				throw new Exception("Udf Package name missing");
			}

			if(args.udfFunctionName == null){
				throw new Exception("Udf Function name missing");
			}
			Value[] udfValues = new Value[udfVals.length];
			int index = 0;
			for(Object value : udfVals){
				udfValues[index++] = Value.get(value);
			}
			args.udfValues = udfValues;
		}

		if (line.hasOption("sendKey")) {
			args.writePolicy.sendKey = true;
		}

		System.out.println("Benchmark: " + this.hosts[0]
			+ ", namespace: " + args.namespace
			+ ", set: " + (args.setName.length() > 0? args.setName : "<empty>")
			+ ", threads: " + this.nThreads
			+ ", workload: " + args.workload);

		if (args.workload == Workload.READ_UPDATE || args.workload == Workload.READ_REPLACE) {
			System.out.print("read: " + args.readPct + '%');
			System.out.print(" (all bins: " + args.readMultiBinPct + '%');
			System.out.print(", single bin: " + (100 - args.readMultiBinPct) + "%)");

			System.out.print(", write: " + (100 - args.readPct) + '%');
			System.out.print(" (all bins: " + args.writeMultiBinPct + '%');
			System.out.println(", single bin: " + (100 - args.writeMultiBinPct) + "%)");
		}

		System.out.println("keys: " + this.nKeys
			+ ", start key: " + this.startKey
			+ ", transactions: " + args.transactionLimit
			+ ", bins: " + args.nBins
			+ ", random values: " + (args.fixedBins == null)
			+ ", throughput: " + (args.throughput == 0 ? "unlimited" : (args.throughput + " tps")));

		if (args.workload != Workload.INITIALIZE) {
			System.out.println("read policy:");
			System.out.println(
					"    socketTimeout: " + args.readPolicy.socketTimeout
					+ ", totalTimeout: " + args.readPolicy.totalTimeout
					+ ", timeoutDelay: " + args.readPolicy.timeoutDelay
					+ ", maxRetries: " + args.readPolicy.maxRetries
					+ ", sleepBetweenRetries: " + args.readPolicy.sleepBetweenRetries
					);

			System.out.println(
					"    readModeAP: " + args.readPolicy.readModeAP
					+ ", readModeSC: " + args.readPolicy.readModeSC
					+ ", replica: " + args.readPolicy.replica
					+ ", reportNotFound: " + args.reportNotFound);
		}

		System.out.println("write policy:");
		System.out.println(
			"    socketTimeout: " + args.writePolicy.socketTimeout
			+ ", totalTimeout: " + args.writePolicy.totalTimeout
			+ ", timeoutDelay: " + args.writePolicy.timeoutDelay
			+ ", maxRetries: " + args.writePolicy.maxRetries
			+ ", sleepBetweenRetries: " + args.writePolicy.sleepBetweenRetries
			);

		System.out.println("    commitLevel: " + args.writePolicy.commitLevel);

		if (args.batchSize > 1) {
			System.out.println("batch size: " + args.batchSize
				+ ", batch threads: " + args.batchPolicy.maxConcurrentThreads);
		}

		if (this.asyncEnabled) {
			System.out.println("Async " + this.eventLoopType + ": MaxCommands " +  this.asyncMaxCommands
				+ ", EventLoops: " + this.eventLoopSize
				);
		}
		else {
			System.out.println("Sync: connPoolsPerNode: " + clientPolicy.connPoolsPerNode);
		}

		int binCount = 0;

		for (DBObjectSpec spec : args.objectSpec) {
			System.out.print("bin[" + binCount + "]: ");

			switch (spec.type) {
			case 'I':
				System.out.println("integer");
				break;

			case 'S':
				System.out.println("string[" + spec.size + "]");
				break;

			case 'B':
				System.out.println("byte[" + spec.size + "]");
				break;
			}
			binCount++;
		}

		System.out.println("debug: " + args.debug);

		Log.Level level = (args.debug)? Log.Level.DEBUG : Log.Level.INFO;
		Log.setLevel(level);
		Log.setCallback(this);
		args.updatePolicy = new WritePolicy(args.writePolicy);
		args.updatePolicy.recordExistsAction = RecordExistsAction.UPDATE;
		args.replacePolicy = new WritePolicy(args.writePolicy);
		args.replacePolicy.recordExistsAction = RecordExistsAction.REPLACE;

		clientPolicy.failIfNotConnected = true;
	}

	private static void logUsage(Options options) {
		HelpFormatter formatter = new HelpFormatter();
		StringWriter sw = new StringWriter();
		PrintWriter pw = new PrintWriter(sw);
		String syntax = Main.class.getName() + " [<options>]";
		formatter.printHelp(pw, 100, syntax, "options:", options, 0, 2, null);

		System.out.println(sw.toString());
	}

	private static String getLatencyUsage(String latencyString) {
		return "Latency usage: ycsb[,<warmup count>] | [alt,]<columns>,<range shift increment>[,us|ms]  Received: " + latencyString;
	}

	public void runBenchmarks() throws Exception {
		if (this.asyncEnabled) {
			EventPolicy eventPolicy = new EventPolicy();

			if (args.readPolicy.socketTimeout > 0 && args.readPolicy.socketTimeout < eventPolicy.minTimeout) {
				eventPolicy.minTimeout = args.readPolicy.socketTimeout;
			}

			if (args.writePolicy.socketTimeout > 0 &&  args.writePolicy.socketTimeout < eventPolicy.minTimeout) {
				eventPolicy.minTimeout = args.writePolicy.socketTimeout;
			}

			switch (this.eventLoopType) {
				default:
				case DIRECT_NIO: {
					eventLoops = new NioEventLoops(eventPolicy, this.eventLoopSize);
					break;
				}

				case NETTY_NIO: {
					EventLoopGroup group = new NioEventLoopGroup(this.eventLoopSize);
					eventLoops = new NettyEventLoops(eventPolicy, group);
					break;
				}

				case NETTY_EPOLL: {
					EventLoopGroup group = new EpollEventLoopGroup(this.eventLoopSize);
					eventLoops = new NettyEventLoops(eventPolicy, group);
					break;
				}
			}

			try {
				clientPolicy.eventLoops = eventLoops;

				if (clientPolicy.maxConnsPerNode < this.asyncMaxCommands) {
					clientPolicy.maxConnsPerNode = this.asyncMaxCommands;
				}
				AerospikeClient client = new AerospikeClient(clientPolicy, hosts);

				try {
					if (initialize) {
						doAsyncInserts(client);
					}
					else {
						doAsyncRWTest(client);
					}
				}
				finally {
					client.close();
				}
			}
			finally {
				eventLoops.close();
			}
		}
		else {
			AerospikeClient client = new AerospikeClient(clientPolicy, hosts);

			try {
				if (initialize) {
					doInserts(client);
				}
				else {
					doRWTest(client);
				}
			}
			finally {
				client.close();
			}
		}
	}

	private void doInserts(AerospikeClient client) throws Exception {
		ExecutorService es = Executors.newFixedThreadPool(this.nThreads);

		// Create N insert tasks
		long ntasks = this.nThreads < this.nKeys ? this.nThreads : this.nKeys;
		long keysPerTask = this.nKeys / ntasks;
		long rem = this.nKeys - (keysPerTask * ntasks);
		long start = this.startKey;

		for (long i = 0 ; i < ntasks; i++) {
			long keyCount = (i < rem)? keysPerTask + 1 : keysPerTask;
			InsertTaskSync it = new InsertTaskSync(client, args, counters, start, keyCount);
			es.execute(it);
			start += keyCount;
		}
		Thread.sleep(900);
		collectInsertStats();
		es.shutdownNow();
	}

	private void doAsyncInserts(AerospikeClient client) throws Exception {
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
			long keyCount = (i < keysRem)? keysPerCommand + 1 : keysPerCommand;

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

			int	numWrites = this.counters.write.count.getAndSet(0);
			int timeoutWrites = this.counters.write.timeouts.getAndSet(0);
			int errorWrites = this.counters.write.errors.getAndSet(0);
			total += numWrites;

			this.counters.periodBegin.set(time);

			String date = SimpleDateFormat.format(new Date(time));
			System.out.println(date.toString() + " write(count=" + total + " tps=" + numWrites +
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

	private void doRWTest(AerospikeClient client) throws Exception {
		ExecutorService es = Executors.newFixedThreadPool(this.nThreads);
		RWTask[] tasks = new RWTask[this.nThreads];

		for (int i = 0 ; i < this.nThreads; i++) {
			RWTaskSync rt = new RWTaskSync(client, args, counters, this.startKey, this.nKeys);
			tasks[i] = rt;
			es.execute(rt);
		}
		Thread.sleep(900);
		collectRWStats(tasks);
		es.shutdown();
	}

	private void doAsyncRWTest(AerospikeClient client) throws Exception {
		// Generate asyncMaxCommand commands to seed the event loops.
		// Then start a new command in each command callback.
		// This effectively throttles new command generation, by only allowing
		// asyncMaxCommands at any point in time.
		int maxConcurrentCommands = this.asyncMaxCommands;

		if (maxConcurrentCommands > this.nKeys) {
			maxConcurrentCommands = (int)this.nKeys;
		}

		RWTask[] tasks = new RWTask[maxConcurrentCommands];

		for (int i = 0; i < maxConcurrentCommands; i++) {
			// Start seed commands on random event loops.
			EventLoop eventLoop = this.clientPolicy.eventLoops.next();
			RWTaskAsync task = new RWTaskAsync(client, eventLoop, args, counters, this.startKey, this.nKeys);
			task.runNextCommand();
		}
		Thread.sleep(900);
		collectRWStats(tasks);
	}

	private void collectRWStats(RWTask[] tasks) throws Exception {
		long transactionTotal = 0;

		while (true) {
			long time = System.currentTimeMillis();

			int	numWrites = this.counters.write.count.getAndSet(0);
			int timeoutWrites = this.counters.write.timeouts.getAndSet(0);
			int errorWrites = this.counters.write.errors.getAndSet(0);

			int	numReads = this.counters.read.count.getAndSet(0);
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

			String date = SimpleDateFormat.format(new Date(time));
			System.out.print(date.toString());
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

			if (args.transactionLimit > 0 ) {
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

	@Override
	public void log(Level level, String message) {
		String date = SimpleDateFormat.format(new Date());
		Thread thread = Thread.currentThread();
		String name = thread.getName();

		if (name == null) {
			name = Long.toString(thread.getId());
		}

		System.out.println(date.toString() + ' ' + level.toString() +
			" Thread " + name + ' ' + message);
	}

	private static class UsageException extends Exception {
		private static final long serialVersionUID = 1L;
	}

	private static void printVersion()
	{
		final Properties properties = new Properties();
		try {
			properties.load(Main.class.getClassLoader().getResourceAsStream("project.properties"));
		} catch (Exception e) {
			System.out.println("None");
		} finally {
			System.out.println(properties.getProperty("name"));
			System.out.println("Version " + properties.getProperty("version"));
		}
	}
}
