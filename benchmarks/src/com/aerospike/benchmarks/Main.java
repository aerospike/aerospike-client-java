/*
 * Aerospike Client - Java Library
 *
 * Copyright 2013 by Aerospike, Inc. All rights reserved.
 *
 * Availability of this source code to partners and customers includes
 * redistribution rights covered by individual contract. Please check your
 * contract for exact rights and responsibilities.
 */
package com.aerospike.benchmarks;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Scanner;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicIntegerArray;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.PosixParser;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.Log;
import com.aerospike.client.Log.Level;
import com.aerospike.client.async.AsyncClient;
import com.aerospike.client.async.AsyncClientPolicy;
import com.aerospike.client.policy.Policy;
import com.aerospike.client.policy.WritePolicy;

public class Main implements Log.Callback {
	
	private static final SimpleDateFormat SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");

	public static void main(String[] args) {
		Main program = null;
		
		try {
			program = new Main(args);
			program.runBenchmarks();
		}
		catch (UsageException ue) {
		}
		catch (Exception e) {		
			System.out.println("Error: " + e.getMessage());
			
			if (program != null && program.debug) {
				e.printStackTrace();
			}
		}
	}

	private String[]        hosts;
	private int             port = 3000;
	private String          namespace = "";
    private String          set = "";
	private DBObjectSpec[]  objectSpec;
	private int             nBins = 1;
	private int             nKeys = 0;
	private int             keySize = 0;
	private int             startKey = 0;
	private String          cycleType = "";
	private int             readPct = -1;
	private int             singleBinReadPct = 0;
	private int             singleBinUpdatePct = 0;
	private boolean         writeKeys = false;
	private boolean         validate = false;
	private int             nThreads = 16;
	private int             nTasks = 1;
	private int             throughput = 0;
	private String          throughput_file = "";
	private int             runTime = 0;
	private int             asyncTaskThreads;
	private boolean         debug = false;
	private boolean         asyncEnabled;

	private AsyncClientPolicy clientPolicy = new AsyncClientPolicy();
	private Policy readPolicy = new Policy();
	private WritePolicy writePolicy = new WritePolicy();
	private CounterStore counters = new CounterStore();

	public Main(String[] args) throws Exception {
		Options options = new Options();
		options.addOption("h", "hosts", true, "Set the Aerospike host node.");
		options.addOption("p", "port", true, "Set the port on which to connect to Aerospike.");
		options.addOption("n", "namespace", true, "Set the Aerospike namespace.");
        options.addOption("s", "set", true, "Set the Aerospike set name.");
		options.addOption("k", "keys", true,
			"Set the number of keys the client is dealing with. " + 
			"If using an 'insert' workload (detailed below), the client will write this " + 
			"number of keys, starting from value = start_value. Otherwise, the client " + 
			"will read and update randomly across the values between start_value and " + 
			"start_value + num_keys."
			);
		options.addOption("l", "keylength", true, "Set the length of the string to use as a key.");
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
		options.addOption("S", "startkey", true, 
			"Set the starting value of the working set of keys. " + 
			"If using an 'insert' workload, the start_value indicates the first value to write. " + 
			"Otherwise, the start_value indicates the smallest value in the working set of keys."
			);
		options.addOption("w", "workload", true, 
			"I | RU,<percent>[,<percent2>][,<percent3>] | RMU <percent> | RMI <percent> | RMD <percent>\n" +
			"Set the desired workload.\n\n" +  
			"   -w I sets a linear 'insert' workload.\n\n" +
			"   -w RU,80 sets a random read-update workload with 80% reads and 20% writes.\n\n" + 
			"   -w RU,80,60,40 sets a random multi-bin read-update workload with 80% reads.\n\n" + 
			"      60% will read a single bin.\n\n" + 
			"       40% will read all bins.\n\n" + 
			"    -w RMU 30 sets a random read-modify-update workload with 30% reads.\n\n" +      
			"    -w RMI 20 sets a random read-modify-increment workload with 20% reads.\n\n" + 	    
			"    -w RMD 70 sets a read-modify-decrement workload workload with 70% reads."
			);
		options.addOption("g", "throughput", true, 
			"Set a target transactions per second for the client. The client should not exceed this " + 
			"average throughput, though it will try to catch-up if it falls behind."
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
		
		//options.addOption("v", "validate", false, "Validate data.");
		options.addOption("D", "debug", false, "Run benchmarks in debug mode.");
		options.addOption("u", "usage", false, "Print usage.");
		
		options.addOption("a", "async", false, "Benchmark asynchronous methods instead of synchronous methods.");
		options.addOption("C", "asyncMaxCommands", true, "Maximum number of concurrent asynchronous database commands.");
		options.addOption("E", "asyncSelectorTimeout", true, "Asynchronous select() timeout in milliseconds.");
		options.addOption("R", "asyncSelectorThreads", true, "Number of selector threads when running in asynchronous mode.");
		options.addOption("U", "asyncTaskThreads", true, "Number of asynchronous tasks. Use zero for unbounded thread pool.");

		// parse the command line arguments
		CommandLineParser parser = new PosixParser();
		CommandLine line = parser.parse(options, args);
		
		if (args.length == 0 || line.hasOption("u")) {
			logUsage(options);
			throw new UsageException();
		}
		
		if (line.hasOption("hosts")) {
			this.hosts = line.getOptionValue("hosts").split(",");
		} else {
			this.hosts = new String[1];
			this.hosts[0] = "127.0.0.1";
		}

		if (line.hasOption("port")) {
			this.port = Integer.parseInt(line.getOptionValue("port"));
		} else {
			this.port = 3000;
		}

		if (line.hasOption("namespace")) {
			this.namespace = line.getOptionValue("namespace");
		} else {
			this.namespace = "test";
		}

		if (line.hasOption("set")) {
			this.set = line.getOptionValue("set");
		}
                   
		if (line.hasOption("keys")) {
			this.nKeys = Integer.parseInt(line.getOptionValue("keys"));
		} else {
			this.nKeys = 100000;
		}

		if (line.hasOption("keylength")) {
			this.keySize = Integer.parseInt(line.getOptionValue("keylength"));
		}

		if (line.hasOption("bins")) {
			this.nBins = Integer.parseInt(line.getOptionValue("bins"));
		}

		if (line.hasOption("objectSpec")) {
			String[] objectsArr = line.getOptionValue("objectSpec").split(",");
			this.objectSpec = new DBObjectSpec[objectsArr.length];
			for (int i=0; i<objectsArr.length; i++) {
				String[] objarr = objectsArr[i].split(":");
				DBObjectSpec dbobj = new DBObjectSpec();
				dbobj.type = objarr[0].charAt(0);
				if (objarr.length > 1) {
					dbobj.size = Integer.parseInt(objarr[1]);
				}
				this.objectSpec[i] = dbobj;
			}
		} else {
			this.objectSpec = new DBObjectSpec[1];
			DBObjectSpec dbobj = new DBObjectSpec(); 
			dbobj.type = 'I';	// If the object is not specified, it has one bin of integer type
			this.objectSpec[0] = dbobj;
		}

		if (line.hasOption("startkey")) {
			this.startKey = Integer.parseInt(line.getOptionValue("startkey"));
		}

		if (line.hasOption("workloadfile")) {
			this.throughput_file = line.getOptionValue("workloadfile");
		}

		if (line.hasOption("workload")) {
			String[] workloadOpts = line.getOptionValue("workload").split(",");
			this.cycleType = workloadOpts[0];
			if(this.cycleType.equals("I")) {
				this.writeKeys = true;
			}
			if (workloadOpts.length > 1) {
				if (this.cycleType.equals("I")) {
					System.out.println("insert workload expects only one parameter");
					System.exit(-1);
				} else if (this.cycleType.equals("RMU")) {
					System.out.println("read-modify-update workload expects only one parameter");
					System.exit(-1);
				} else if (this.cycleType.equals("RMI")) {
					System.out.println("read-modify-increment workload expects only one parameter");
					System.exit(-1);
				} else if (this.cycleType.equals("RMD")) {
					System.out.println("read-modify-decrement workload expects only one parameter");
					System.exit(-1);
				}

				this.readPct = Integer.parseInt(workloadOpts[1]);
			} else if (this.cycleType.equals("RU")) {
				System.out.println("read-update workload expects only one parameter");
				System.exit(-1);
			}
			if (workloadOpts.length > 2) {
				this.singleBinReadPct = Integer.parseInt(workloadOpts[2]);
				this.singleBinUpdatePct = Integer.parseInt(workloadOpts[3]);
			}
		} else {
			this.cycleType = "RU";
			this.readPct = 90;
		}

		if (line.hasOption("throughput")) {
			this.throughput = Integer.parseInt(line.getOptionValue("throughput"));
		}
		
		if (line.hasOption("timeout")) {
			int timeout = Integer.parseInt(line.getOptionValue("timeout"));
			this.readPolicy.timeout = timeout;
			this.writePolicy.timeout = timeout;
		}			 

		if (line.hasOption("readTimeout")) {
			this.readPolicy.timeout = Integer.parseInt(line.getOptionValue("readTimeout"));
		}			 

		if (line.hasOption("writeTimeout")) {
			this.writePolicy.timeout = Integer.parseInt(line.getOptionValue("writeTimeout"));
		}			 

		if (line.hasOption("maxRetries")) {
			int maxRetries = Integer.parseInt(line.getOptionValue("maxRetries"));
			this.readPolicy.maxRetries = maxRetries;
			this.writePolicy.maxRetries = maxRetries;
		}
		
		if (line.hasOption("sleepBetweenRetries")) {
			int sleepBetweenRetries = Integer.parseInt(line.getOptionValue("sleepBetweenRetries"));
			this.readPolicy.sleepBetweenRetries = sleepBetweenRetries;
			this.writePolicy.sleepBetweenRetries = sleepBetweenRetries;
		}
		
		if (line.hasOption("threads")) {
			this.nThreads = Integer.parseInt(line.getOptionValue("threads"));
		}  

		if (line.hasOption("validate")) {
			this.validate = true;
		}

		if (line.hasOption("runtime")) {
			this.runTime = Integer.parseInt(line.getOptionValue("runtime"));
		}

		if (line.hasOption("debug")) {
			this.debug = true;
		}

        if (line.hasOption("async")) {
        	this.asyncEnabled = true;
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

        if (this.nThreads > 1) {
			this.nTasks = nThreads;
		}
        
        if (line.hasOption("latency")) {
			String[] latencyOpts = line.getOptionValue("latency").split(",");
			int columns = Integer.parseInt(latencyOpts[0]);
			int bitShift = Integer.parseInt(latencyOpts[1]);
			counters.read.latency = new LatencyManager(columns, bitShift);
			counters.write.latency = new LatencyManager(columns, bitShift);      	
        }
        
		if (this.keySize == 0) {
			this.keySize = (Integer.toString(this.nKeys+this.startKey)).length();
		}
		
		int rp = (this.readPct >= 0)? this.readPct : 0; 

		System.out.println("Benchmark: " + this.hosts[0] + ":" + this.port 
			+ ", namespace: " + this.namespace 
			+ ", set: " + (this.set.length() > 0? this.set : "<empty>")
			+ ", threads: " + this.nThreads
			+ ", read-write ratio: " + rp + "/" + (100-rp));
		
		System.out.println("keys: " + this.nKeys
			+ ", key length: " + this.keySize
			+ ", start key: " + this.startKey
			+ ", bins: " + this.nBins
			+ ", debug: " + this.debug);
	
		System.out.println("read policy: timeout: " + this.readPolicy.timeout
			+ ", maxRetries: " + this.readPolicy.maxRetries 
			+ ", sleepBetweenRetries: " + this.readPolicy.sleepBetweenRetries);

		System.out.println("write policy: timeout: " + this.writePolicy.timeout
			+ ", maxRetries: " + this.writePolicy.maxRetries
			+ ", sleepBetweenRetries: " + this.writePolicy.sleepBetweenRetries);
		
		if (this.asyncEnabled) {
			String threadPoolName = (clientPolicy.asyncTaskThreadPool == null)? "none" : clientPolicy.asyncTaskThreadPool.getClass().getName();
			System.out.println("Async: MaxConnTotal " +  clientPolicy.asyncMaxCommands
				+ ", MaxConnAction: " + clientPolicy.asyncMaxCommandAction
				+ ", SelectorTimeout: " + clientPolicy.asyncSelectorTimeout
				+ ", SelectorThreads: " + clientPolicy.asyncSelectorThreads
				+ ", TaskThreadPool: " + threadPoolName);
		}

		if (this.keySize < (Integer.toString(this.nKeys+this.startKey)).length()) {
			String errStr = "keylength (-l) must be at least "+(Integer.toString(this.nKeys+this.startKey)).length()+" for "+this.nKeys+" keys";
			if(this.startKey > 0) {
				errStr = errStr + ", starting at "+this.startKey;
			}
			System.out.println(errStr);
			System.exit(-1);
		}

		if (!this.cycleType.equals("I") && !this.cycleType.equals("RU") && !this.cycleType.equals("RMU") && !this.cycleType.equals("RMI") && !this.cycleType.equals("RMD")) {
			System.out.println("workload type must be I (insert), RU (read/update), RMU (read-modify-update), RMI (read-modify-increment), or RMD (read-modify-decrement)");
			System.exit(-1);
		}

		if (this.cycleType.equals("RU") && (this.readPct < 0 || this.readPct > 100)) {
			System.out.println("read-update workload read percentage must be between 0 and 100");
			System.exit(-1);
		}

		if (this.nThreads < 1) {
			System.out.println("number of client threads (-z) must be > 0");
			System.exit(-1);
		}

		if (this.runTime < 0) {
			System.out.println("runtime (-t) must be >= 0");
			System.exit(-1);
		}
		
		Log.Level level = (debug)? Log.Level.DEBUG : Log.Level.INFO;
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
				if (writeKeys) {
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
				if (writeKeys) {
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
		int ntasks = this.nTasks < this.nKeys ? this.nTasks : this.nKeys;
		int start = this.startKey;
		int keysPerTask = this.nKeys / ntasks + 1;

		for (int i=0 ; i<ntasks; i++) {
			InsertTask it = new InsertTaskSync(client, this.namespace, this.set, start, keysPerTask, 
				this.keySize, this.nBins, this.writePolicy, this.objectSpec, this.counters, debug);
			
			es.execute(it);
			start += keysPerTask;
		}	
		collectInsertStats();
		es.shutdownNow();
	}

	private void doAsyncInserts(AsyncClient client) throws Exception {	
		ExecutorService es = Executors.newFixedThreadPool(this.nThreads);

		// Create N insert tasks
		int ntasks = this.nTasks < this.nKeys ? this.nTasks : this.nKeys;
		int start = this.startKey;
		int keysPerTask = this.nKeys / ntasks + 1;

		for (int i=0 ; i<ntasks; i++) {
			InsertTask it = new InsertTaskAsync(client, this.namespace, this.set, start, keysPerTask, 
					this.keySize, this.nBins, this.writePolicy, this.objectSpec, this.counters, debug);
			
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
		AtomicIntegerArray settingsArr = initSettings();
		
		// Start RW tasks...
		ExecutorService es = Executors.newFixedThreadPool(this.nThreads);
		
		for (int i=0 ; i<this.nTasks; i++) {
			RWTask rt;
			if (this.validate) {
				int tstart = this.startKey + ((int) (this.nKeys*(((float) i)/this.nTasks)));
				int tkeys = (int) (this.nKeys*(((float) (i+1))/this.nTasks)) - (int) (this.nKeys*(((float) i)/this.nTasks));
				
				rt = new RWTaskSync(client, this.namespace, this.set, tkeys, tstart, this.keySize, this.objectSpec, this.nBins, 
					this.cycleType, this.readPolicy, this.writePolicy, settingsArr, this.validate, this.runTime, this.counters, this.debug);
			} else {
				rt = new RWTaskSync(client, this.namespace, this.set, this.nKeys, this.startKey, this.keySize, this.objectSpec, this.nBins, 
					this.cycleType, this.readPolicy, this.writePolicy, settingsArr, this.validate, this.runTime, this.counters, this.debug);
			}
			es.execute(rt);
		}
		collectRWStats(null, settingsArr);
	}

	private void doAsyncRWTest(AsyncClient client) throws Exception {
		AtomicIntegerArray settingsArr = initSettings();
		
		// Start RW tasks...
		ExecutorService es = Executors.newFixedThreadPool(this.nThreads);
		
		for (int i=0 ; i<this.nTasks; i++) {
			RWTask rt;
			if (this.validate) {
				int tstart = this.startKey + ((int) (this.nKeys*(((float) i)/this.nTasks)));
				int tkeys = (int) (this.nKeys*(((float) (i+1))/this.nTasks)) - (int) (this.nKeys*(((float) i)/this.nTasks));
				
				rt = new RWTaskAsync(client, this.namespace, this.set, tkeys, tstart, this.keySize, this.objectSpec, this.nBins,
					this.cycleType, this.readPolicy, this.writePolicy, settingsArr, this.validate, this.runTime, this.counters, this.debug);					
			} else {
				rt = new RWTaskAsync(client, this.namespace, this.set, this.nKeys, this.startKey, this.keySize, this.objectSpec, this.nBins, 
					this.cycleType, this.readPolicy, this.writePolicy, settingsArr, this.validate, this.runTime, this.counters, this.debug);
			}
			es.execute(rt);
		}
		collectRWStats(client, settingsArr);
	}

	private AtomicIntegerArray initSettings() {
		// Certain setting can change dynamically based on the throughput file. Because 
		// of this, we pass these variable settings as AtomicIntegers, and via reference. 
		AtomicIntegerArray settingsArr = new AtomicIntegerArray(4);
		settingsArr.set(0, this.throughput);
		settingsArr.set(1, this.readPct);
		settingsArr.set(2, this.singleBinReadPct);
		settingsArr.set(3, this.singleBinUpdatePct);
		return settingsArr;
	}
	
	private void collectRWStats(AsyncClient client, AtomicIntegerArray settingsArr) throws Exception {		
		// wait for all the tasks to finish setting up for validation
		if (this.validate) {
			while(counters.loadValuesFinishedTasks.get() < this.nTasks) {
				Thread.sleep(1000);
				//System.out.println("tasks done = "+counters.loadValuesFinishedTasks.get()+ ", g_ntasks = "+g_ntasks);
			}
			// set flag that everyone is ready - this will allow the individual tasks to go
			counters.loadValuesFinished.set(true);
		}

		// Set start time 
		this.counters.start_time = System.currentTimeMillis();

		// Wait for completion
		for (; this.runTime == 0 || this.counters.timeElapsed.get() < this.runTime; this.counters.timeElapsed.incrementAndGet()) {

			long time = System.currentTimeMillis();
			
			int	numWrites = this.counters.write.count.getAndSet(0);
			int timeoutWrites = this.counters.write.timeouts.getAndSet(0);
			int errorWrites = this.counters.write.errors.getAndSet(0);
			
			int	numReads = this.counters.read.count.getAndSet(0);
			int timeoutReads = this.counters.read.timeouts.getAndSet(0);
			int errorReads = this.counters.read.errors.getAndSet(0);
			
			//int used = (client != null)? client.getAsyncConnUsed() : 0;
			//Node[] nodes = client.getNodes();
			
			String date = SimpleDateFormat.format(new Date(time));
			System.out.println(date.toString() + " write(tps=" + numWrites + " timeouts=" + timeoutWrites + " errors=" + errorWrites + ")" +
				" read(tps=" + numReads + " timeouts=" + timeoutReads + " errors=" + errorReads + ")" +
				" total(tps=" + (numWrites + numReads) + " timeouts=" + (timeoutWrites + timeoutReads) + " errors=" + (errorWrites + errorReads) + ")"
				//+ " buffused=" + used
				//+ " nodeused=" + ((AsyncNode)nodes[0]).openCount.get() + ',' + ((AsyncNode)nodes[1]).openCount.get() + ',' + ((AsyncNode)nodes[2]).openCount.get()
				);

			if (this.counters.write.latency != null) {
				this.counters.write.latency.printHeader(System.out);
				this.counters.write.latency.printResults(System.out, "write");
				this.counters.read.latency.printResults(System.out, "read");
			}

			if (throughput_file.length() > 0) {
				// set target throughput and read/update percentage based on throughput_file
				try {
					String[] settings = readSettings(throughput_file);
					settingsArr.set(0, Integer.parseInt(settings[0]));
					settingsArr.set(1, Integer.parseInt(settings[1]));
				} catch (Exception e) {
					System.out.println("can't read throughput file!");
				}
			}		
			Thread.sleep(1000);
		}
	}

	private String[] readSettings(String throughput_file) throws IOException {
		Scanner scanner = new Scanner(new FileInputStream(throughput_file), "UTF-8");
		String[] settings = scanner.nextLine().split(",");
		scanner.close();
		return settings;
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
