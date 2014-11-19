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
package com.aerospike.benchmarks;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.policy.RecordExistsAction;
import com.aerospike.client.Log;
import com.aerospike.client.Log.Level;
import com.aerospike.client.async.AsyncClient;
import com.aerospike.client.async.AsyncClientPolicy;

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
			
			if (program != null && program.args.debug) {
				e.printStackTrace();
			}
		}
	}

	private Arguments args = new Arguments();
	private String[] hosts;
	private int port = 3000;
	private int nKeys;
	private int startKey;
	private int nThreads;
	private int asyncTaskThreads;
	private boolean asyncEnabled;
	private boolean initialize;
	private String filepath;

	private AsyncClientPolicy clientPolicy = new AsyncClientPolicy();
	private CounterStore counters = new CounterStore();

	public Main(String[] commandLineArgs) throws Exception {
		Options options = new Options();
		options.addOption("h", "hosts", true, "Set the Aerospike host node.");
		options.addOption("p", "port", true, "Set the port on which to connect to Aerospike.");
		options.addOption("U", "user", true, "User name");
		options.addOption("P", "password", true, "Password");
		options.addOption("n", "namespace", true, "Set the Aerospike namespace.");
        options.addOption("s", "set", true, "Set the Aerospike set name.");
		options.addOption("k", "keys", true,
			"Set the number of keys the client is dealing with. " + 
			"If using an 'insert' workload (detailed below), the client will write this " + 
			"number of keys, starting from value = start_value. Otherwise, the client " + 
			"will read and update randomly across the values between start_value and " + 
			"start_value + num_keys."
			);
		
		// key type has been changed to integer, so this option is no longer relevant.
		// Leave in (and ignore) so existing benchmark scripts do not break.
		options.addOption("l", "keylength", true, "Not used anymore since key is an integer.");
		
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
			"I | RU,<percent>[,<percent2>][,<percent3>] | RMU | RMI | RMD\n" +
			"Set the desired workload.\n\n" +  
			"   -w I sets a linear 'insert' workload.\n\n" +
			"   -w RU,80 sets a random read-update workload with 80% reads and 20% writes.\n\n" + 
			"      100% of reads will read all bins.\n\n" + 
			"      100% of writes will write all bins.\n\n" + 
			"   -w RU,80,60,30 sets a random multi-bin read-update workload with 80% reads and 20% writes.\n\n" + 
			"      60% of reads will read all bins. 40% of reads will read a single bin.\n\n" + 
			"      30% of writes will write all bins. 70% of writes will write a single bin.\n\n" + 
			"    -w RMU sets a random read all bins-update one bin workload with 50% reads.\n\n" +      
			"    -w RMI sets a random read all bins-increment one integer bin workload with 50% reads.\n\n" + 	    
			"    -w RMD sets a random read all bins-decrement one integer bin workload with 50% reads."
			);
		options.addOption("g", "throughput", true, 
			"Set a target transactions per second for the client. The client should not exceed this " + 
			"average throughput."
			);
		
		options.addOption("T", "timeout", true, "Set read and write transaction timeout in milliseconds.");
		options.addOption("readTimeout", true, "Set read transaction timeout in milliseconds.");
		options.addOption("writeTimeout", true, "Set write transaction timeout in milliseconds.");
	
		options.addOption("maxRetries", true, "Maximum number of retries before aborting the current transaction.");
		options.addOption("sleepBetweenRetries", true, 
			"Milliseconds to sleep between retries if a transaction fails and the timeout was not exceeded. " +
			"Enter zero to skip sleep."	
			);
		options.addOption("z", "threads", true, 
			"Set the number of threads the client will use to generate load. " + 
			"It is not recommended to use a value greater than 125."
			);	
		options.addOption("latency", true, 
			"<number of latency columns>,<range shift increment>\n" +
			"Show transaction latency percentages using elapsed time ranges.\n" +
			"<number of latency columns>: Number of elapsed time ranges.\n" +
			"<range shift increment>: Power of 2 multiple between each range starting at column 3.\n\n" + 
			"A latency definition of '-latency 7,1' results in this layout:\n" +
			"    <=1ms >1ms >2ms >4ms >8ms >16ms >32ms\n" +
			"       x%   x%   x%   x%   x%    x%    x%\n" +
			"A latency definition of '-latency 4,3' results in this layout:\n" +
			"    <=1ms >1ms >8ms >64ms\n" +
			"       x%   x%   x%    x%\n\n" +
			"Latency columns are cumulative. If a transaction takes 9ms, it will be included in both the >1ms and >8ms columns."
			);
		
		options.addOption("N", "reportNotFound", false, "Report not found errors. Data should be fully initialized before using this option.");
		//options.addOption("v", "validate", false, "Validate data.");
		options.addOption("D", "debug", false, "Run benchmarks in debug mode.");
		options.addOption("u", "usage", false, "Print usage.");

		options.addOption("B", "batchSize", true, 
			"Enable batch mode with number of records to process in each batch get call. " + 
			"Batch mode is valid only for RU (read update) workloads. Batch mode is disabled by default."
			);

		options.addOption("t", "storeType", true, 
			"Defines data store type to run. Values:  KVS | LLIST | LSTACK" 
			);

		options.addOption("BT", "batchThreads", true,
			"Maximum number of concurrent batch sub-threads for each batch command.\n" + 
			"1   : Run each batch node command sequentially.\n" +
			"0   : Run all batch node commands in parallel.\n" +
			"> 1 : Run maximum batchThreads in parallel.  When a node command finshes, start a new one until all finished."
			);
		
		options.addOption("a", "async", false, "Benchmark asynchronous methods instead of synchronous methods.");
		options.addOption("C", "asyncMaxCommands", true, "Maximum number of concurrent asynchronous database commands.");
		options.addOption("E", "asyncSelectorTimeout", true, "Asynchronous select() timeout in milliseconds.");
		options.addOption("W", "asyncSelectorThreads", true, "Number of selector threads when running in asynchronous mode.");
		options.addOption("V", "asyncTaskThreads", true, "Number of asynchronous tasks. Use zero for unbounded thread pool.");
		options.addOption("F", "keyFile", true, "File path to read the keys for read operation.");
		options.addOption("KT", "keyType", true, "Type of the key(String/Integer) in the file, default is String");

		// parse the command line arguments
		CommandLineParser parser = new PosixParser();
		CommandLine line = parser.parse(options, commandLineArgs);		
		String[] extra = line.getArgs();

		if (line.hasOption("u")) {
			logUsage(options);
			throw new UsageException();
		}
		
		if (extra.length > 0) {
			throw new Exception("Unexpected arguments: " + Arrays.toString(extra));
		}
		
        if (line.hasOption("async")) {
        	this.asyncEnabled = true;
        	args.readPolicy = clientPolicy.asyncReadPolicyDefault;
        	args.writePolicy = clientPolicy.asyncWritePolicyDefault;
        	args.batchPolicy = clientPolicy.batchPolicyDefault;  // async does not need batch policy.
        }
        else {
        	args.readPolicy = clientPolicy.readPolicyDefault;
        	args.writePolicy = clientPolicy.writePolicyDefault;
        	args.batchPolicy = clientPolicy.batchPolicyDefault;
        }

        if (line.hasOption("hosts")) {
			this.hosts = line.getOptionValue("hosts").split(",");
		} 
		else {
			this.hosts = new String[1];
			this.hosts[0] = "127.0.0.1";
		}

		if (line.hasOption("port")) {
			this.port = Integer.parseInt(line.getOptionValue("port"));
		} 
		else {
			this.port = 3000;
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

		if (line.hasOption("set")) {
			args.setName = line.getOptionValue("set");
		}
		else {
			args.setName = "";
		}

		if (line.hasOption("keys")) {
			this.nKeys = Integer.parseInt(line.getOptionValue("keys"));
		} else {
			this.nKeys = 100000;
		}

		if (line.hasOption("startkey")) {
			this.startKey = Integer.parseInt(line.getOptionValue("startkey"));
		}
		
		//Variables setting in case of command arguments passed with keys in File
		if (line.hasOption("keyFile")) {
			this.filepath = line.getOptionValue("keyFile");
			// Load the file
			keyList = Utils.readKeyFromFile(filepath);
			if (keyList.isEmpty()) {
				throw new Exception("File : '" + filepath + "' is empty,this file can't be processed.");
			}
			this.nKeys = keyList.size();
			this.startKey = 0;
			args.validate = false;
			
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

				args.workload = Workload.READ_UPDATE;
				if (workloadType.equals("RR")) {
					args.writePolicy.recordExistsAction = RecordExistsAction.REPLACE;
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
			else {
				throw new Exception("Unknown workload: " + workloadType);
			}
		}

		if (line.hasOption("throughput")) {
			args.throughput = Integer.parseInt(line.getOptionValue("throughput"));
		}
		
		if (line.hasOption("timeout")) {
			int timeout = Integer.parseInt(line.getOptionValue("timeout"));
			args.readPolicy.timeout = timeout;
			args.writePolicy.timeout = timeout;
			args.batchPolicy.timeout = timeout;
		}			 

		if (line.hasOption("readTimeout")) {
			int timeout = Integer.parseInt(line.getOptionValue("readTimeout"));
			args.readPolicy.timeout = timeout;
			args.batchPolicy.timeout = timeout;
		}			 

		if (line.hasOption("writeTimeout")) {
			args.writePolicy.timeout = Integer.parseInt(line.getOptionValue("writeTimeout"));
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
		
		if (line.hasOption("threads")) {
			this.nThreads = Integer.parseInt(line.getOptionValue("threads"));
			
			if (this.nThreads < 1) {
				throw new Exception("number of client threads (-z) must be > 0");
			}
		}
		else {
			this.nThreads = 16;
		}

		if (line.hasOption("reportNotFound")) {
			args.reportNotFound = true;
		}

		if (line.hasOption("validate")) {
			args.validate = true;
		}

		if (line.hasOption("debug")) {
			args.debug = true;
		}

        if (line.hasOption("batchSize")) {
        	args.batchSize =  Integer.parseInt(line.getOptionValue("batchSize"));
        }

		args.storeType = StorageType.KVS;
        if (line.hasOption("storeType")) {
        	String storetype = line.getOptionValue("storeType");
			if (storetype.equals("LLIST")) {
				args.storeType = StorageType.LLIST;
			} else if (storetype.equals("LSTACK")) {
				args.storeType = StorageType.LSTACK;
			}
        }
        
		if (line.hasOption("batchThreads")) {
			args.batchPolicy.maxConcurrentThreads = Integer.parseInt(line.getOptionValue("batchThreads"));
		}			 

		if (line.hasOption("asyncMaxCommands")) {
        	this.clientPolicy.asyncMaxCommands =  Integer.parseInt(line.getOptionValue("asyncMaxCommands"));
        }
        
        if (line.hasOption("asyncSelectorTimeout")) {
        	this.clientPolicy.asyncSelectorTimeout =  Integer.parseInt(line.getOptionValue("asyncSelectorTimeout"));
        }

        if (line.hasOption("asyncSelectorThreads")) {
        	this.clientPolicy.asyncSelectorThreads =  Integer.parseInt(line.getOptionValue("asyncSelectorThreads"));
        }
        
        if (line.hasOption("asyncTaskThreads")) {
        	this.asyncTaskThreads = Integer.parseInt(line.getOptionValue("asyncTaskThreads"));
        	
        	if (asyncTaskThreads == 0) {
        		this.clientPolicy.asyncTaskThreadPool = Executors.newCachedThreadPool();
        	}
        	else {           		
        		this.clientPolicy.asyncTaskThreadPool = Executors.newFixedThreadPool(asyncTaskThreads);
        	}
        }
        
        if (line.hasOption("latency")) {
			String[] latencyOpts = line.getOptionValue("latency").split(",");
			
			if (latencyOpts.length != 2) {
				throw new Exception("Latency expects 2 arguments. Received: " + latencyOpts.length);
			}
			int columns = Integer.parseInt(latencyOpts[0]);
			int bitShift = Integer.parseInt(latencyOpts[1]);
			counters.read.latency = new LatencyManager(columns, bitShift);
			counters.write.latency = new LatencyManager(columns, bitShift);      	
        }

		if (! line.hasOption("random")) {
			args.setFixedBins();
		}

		System.out.println("Benchmark: " + this.hosts[0] + ":" + this.port 
			+ ", namespace: " + args.namespace 
			+ ", set: " + (args.setName.length() > 0? args.setName : "<empty>")
			+ ", threads: " + this.nThreads
			+ ", workload: " + args.workload);
		
		if (args.workload == Workload.READ_UPDATE) {
			System.out.print("read: " + args.readPct + '%');
			System.out.print(" (all bins: " + args.readMultiBinPct + '%');
			System.out.print(", single bin: " + (100 - args.readMultiBinPct) + "%)");
			
			System.out.print(", write: " + (100 - args.readPct) + '%');
			System.out.print(" (all bins: " + args.writeMultiBinPct + '%');
			System.out.println(", single bin: " + (100 - args.writeMultiBinPct) + "%)");
		}
		
		System.out.println("keys: " + this.nKeys
			+ ", start key: " + this.startKey
			+ ", bins: " + args.nBins
			+ ", random values: " + (args.fixedBins == null)
			+ ", throughput: " + (args.throughput == 0 ? "unlimited" : (args.throughput + " tps"))
			+ ", debug: " + args.debug);
	
		if (args.workload != Workload.INITIALIZE) {
			System.out.println("read policy: timeout: " + args.readPolicy.timeout
				+ ", maxRetries: " + args.readPolicy.maxRetries 
				+ ", sleepBetweenRetries: " + args.readPolicy.sleepBetweenRetries
				+ ", reportNotFound: " + args.reportNotFound);
		}

		System.out.println("write policy: timeout: " + args.writePolicy.timeout
			+ ", maxRetries: " + args.writePolicy.maxRetries
			+ ", sleepBetweenRetries: " + args.writePolicy.sleepBetweenRetries);
		
		if (args.batchSize > 1) {		
			System.out.println("batch size: " + args.batchSize
				+ ", batch threads: " + args.batchPolicy.maxConcurrentThreads);
		}
		
		if (this.asyncEnabled) {
			String threadPoolName = (clientPolicy.asyncTaskThreadPool == null)? "none" : clientPolicy.asyncTaskThreadPool.getClass().getName();
			System.out.println("Async: MaxConnTotal " +  clientPolicy.asyncMaxCommands
				+ ", MaxConnAction: " + clientPolicy.asyncMaxCommandAction
				+ ", SelectorTimeout: " + clientPolicy.asyncSelectorTimeout
				+ ", SelectorThreads: " + clientPolicy.asyncSelectorThreads
				+ ", TaskThreadPool: " + threadPoolName);
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

		Log.Level level = (args.debug)? Log.Level.DEBUG : Log.Level.INFO;
		Log.setLevel(level);
		Log.setCallback(this);		

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

	public void runBenchmarks() throws Exception {
		if (this.asyncEnabled) {
			AsyncClient client = new AsyncClient(clientPolicy, hosts[0], port);		

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
		else {			
			AerospikeClient client = new AerospikeClient(clientPolicy, hosts[0], port);		

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
		int ntasks = this.nThreads < this.nKeys ? this.nThreads : this.nKeys;
		int start = this.startKey;
		int keysPerTask = this.nKeys / ntasks + 1;

		for (int i = 0 ; i < ntasks; i++) {
			InsertTask it = new InsertTaskSync(client, args, counters, start, keysPerTask); 			
			es.execute(it);
			start += keysPerTask;
		}	
		collectInsertStats();
		es.shutdownNow();
	}

	private void doAsyncInserts(AsyncClient client) throws Exception {	
		ExecutorService es = Executors.newFixedThreadPool(this.nThreads);

		// Create N insert tasks
		int ntasks = this.nThreads < this.nKeys ? this.nThreads : this.nKeys;
		int start = this.startKey;
		int keysPerTask = this.nKeys / ntasks + 1;

		for (int i=0 ; i<ntasks; i++) {
			InsertTask it = new InsertTaskAsync(client, args, counters, start, keysPerTask); 			
			es.execute(it);
			start += keysPerTask;
		}
		collectInsertStats();
		es.shutdownNow();
	}

	private void collectInsertStats() throws Exception {	
		int total = 0;
		
		while (total < this.nKeys) {
			long time = System.currentTimeMillis();
			
			int	numWrites = this.counters.write.count.getAndSet(0);
			int timeoutWrites = this.counters.write.timeouts.getAndSet(0);
			int errorWrites = this.counters.write.errors.getAndSet(0);		
			total += numWrites;

			String date = SimpleDateFormat.format(new Date(time));
			System.out.println(date.toString() + " write(count=" + total + " tps=" + numWrites + 
				" timeouts=" + timeoutWrites + " errors=" + errorWrites + ")");

			if (this.counters.write.latency != null) {
				this.counters.write.latency.printHeader(System.out);
				this.counters.write.latency.printResults(System.out, "write");
			}

			Thread.sleep(1000);
		}
	}

	private void doRWTest(AerospikeClient client) throws Exception {		
		ExecutorService es = Executors.newFixedThreadPool(this.nThreads);
		
		for (int i = 0 ; i < this.nThreads; i++) {
			RWTask rt;
			if (args.validate) {
				int tstart = this.startKey + ((int) (this.nKeys*(((float) i)/this.nThreads)));			
				int tkeys = (int) (this.nKeys*(((float) (i+1))/this.nThreads)) - (int) (this.nKeys*(((float) i)/this.nThreads));
				rt = new RWTaskSync(client, args, counters, tstart, tkeys);
			} else {
				rt = new RWTaskSync(client, args, counters, this.startKey, this.nKeys);
			}
			es.execute(rt);
		}
		collectRWStats(null);
	}

	private void doAsyncRWTest(AsyncClient client) throws Exception {
		ExecutorService es = Executors.newFixedThreadPool(this.nThreads);
		
		for (int i = 0 ; i < this.nThreads; i++) {
			RWTask rt;
			if (args.validate) {
				int tstart = this.startKey + ((int) (this.nKeys*(((float) i)/this.nThreads)));			
				int tkeys = (int) (this.nKeys*(((float) (i+1))/this.nThreads)) - (int) (this.nKeys*(((float) i)/this.nThreads));
				rt = new RWTaskAsync(client, args, counters, tstart, tkeys);
			} else {
				rt = new RWTaskAsync(client, args, counters, this.startKey, this.nKeys);
			}
			es.execute(rt);
		}
		collectRWStats(client);
	}
	
	private void collectRWStats(AsyncClient client) throws Exception {		
		// wait for all the tasks to finish setting up for validation
		if (args.validate) {
			while(counters.loadValuesFinishedTasks.get() < this.nThreads) {
				Thread.sleep(1000);
				//System.out.println("tasks done = "+counters.loadValuesFinishedTasks.get()+ ", g_ntasks = "+g_ntasks);
			}
			// set flag that everyone is ready - this will allow the individual tasks to go
			counters.loadValuesFinished.set(true);
		}

		while (true) {
			long time = System.currentTimeMillis();
			
			int	numWrites = this.counters.write.count.getAndSet(0);
			int timeoutWrites = this.counters.write.timeouts.getAndSet(0);
			int errorWrites = this.counters.write.errors.getAndSet(0);
			
			int	numReads = this.counters.read.count.getAndSet(0);
			int timeoutReads = this.counters.read.timeouts.getAndSet(0);
			int errorReads = this.counters.read.errors.getAndSet(0);
			
			int notFound = 0;
			
			if (args.reportNotFound) {
				notFound = this.counters.readNotFound.getAndSet(0);
			}
			
			this.counters.periodBegin.set(time);

			//int used = (client != null)? client.getAsyncConnUsed() : 0;
			//Node[] nodes = client.getNodes();
			
			String date = SimpleDateFormat.format(new Date(time));
			System.out.print(date.toString());
			System.out.print(" write(tps=" + numWrites + " timeouts=" + timeoutWrites + " errors=" + errorWrites + ")");
			System.out.print(" read(tps=" + numReads + " timeouts=" + timeoutReads + " errors=" + errorReads);
			
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
			}

			Thread.sleep(1000);
		}
	}

	@Override
	public void log(Level level, String message) {
		String date = SimpleDateFormat.format(new Date());
		System.out.println(date.toString() + ' ' + level.toString() + 
			" Thread " + Thread.currentThread().getId() + ' ' + message);		
	}
	
	private static class UsageException extends Exception {
		private static final long serialVersionUID = 1L;
	}
}
