package com.aerospike.benchmarks;

import java.util.concurrent.atomic.AtomicLong;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.async.EventLoop;
import com.aerospike.client.policy.ClientPolicy;

class RWAsyncManager implements Runnable, Stoppable {
	private boolean valid = true;

	private CounterStore counters;

	private int throughput;
	private int writeThroughput = 0;
	public RWTaskAsync[] tasks;

	public RWAsyncManager(Arguments args, CounterStore counters, long keyStart, long keyCount, int maxCommands,
			boolean splitAsyncCommands, AerospikeClient client, ClientPolicy clientPolicy) {
		this.throughput = args.throughput;
		this.counters = counters;

		this.tasks = new RWTaskAsync[maxCommands];

		if (!splitAsyncCommands || args.readPct == 0 || args.readPct == 100) {
			this.createTasks(args, client, clientPolicy, args.readPct, keyStart, keyCount, counters.asyncQuota, 0,
					maxCommands);
			return;
		}

		double readRatio = args.readPct / 100.0;
		int maxReadCommands = (int) Math.round(readRatio * maxCommands);
		int maxWriteCommands = maxCommands - maxReadCommands;

		if (maxReadCommands == 0) {
			maxReadCommands++;
			maxWriteCommands--;
		}
		else if (maxWriteCommands == 0) {
			maxReadCommands--;
			maxWriteCommands++;
		}

		if (args.throughput != 0) {
			int readThroughput = (int) Math.round(readRatio * args.throughput);
			int writeThroughput = args.throughput - readThroughput;

			if (readThroughput == 0) {
				readThroughput++;
				writeThroughput--;
			} else if (writeThroughput == 0) {
				readThroughput--;
				writeThroughput++;
			}

			this.throughput = readThroughput;
			this.writeThroughput = writeThroughput;
		}

		this.createTasks(args, client, clientPolicy, 100, keyStart, keyCount, counters.asyncQuota, 0, maxReadCommands);
		this.createTasks(args, client, clientPolicy, 0, keyStart, keyCount, counters.asyncWriteQuota, maxReadCommands,
				maxWriteCommands);
	}

	private void createTasks(Arguments args, AerospikeClient client, ClientPolicy clientPolicy, int readPct,
			long keyStart, long keyCount, AtomicLong quota, int starti, int count) {
		int end = starti + count;

		for (int i = starti; i < end; i++) {
			EventLoop eventLoop = clientPolicy.eventLoops.next();

			this.tasks[i] = new RWTaskAsync(client, eventLoop, args, counters, readPct, keyStart, keyCount, quota);
		}
	}

	@Override
	public void run() {
		if (this.throughput == 0) {
			// Set quotas to unlimited.
			this.counters.asyncQuota.set(Long.MAX_VALUE);
			this.counters.asyncWriteQuota.set(Long.MAX_VALUE);
			this.runNextCommand();

			return;
		}

		int cyclesPerSecond = 1000;
		long wait = 1000 / cyclesPerSecond;

		Throttle throttle = new Throttle(this.counters.asyncQuota, this.throughput, cyclesPerSecond);
		Throttle writeThrottle = new Throttle(this.counters.asyncWriteQuota, this.writeThroughput, cyclesPerSecond);

		long start = System.currentTimeMillis();

		while (valid) {
			this.runNextCommand();

			try {
				Thread.sleep(wait);
			} catch (InterruptedException e) {
				e.printStackTrace();
				return;
			}

			long end = System.currentTimeMillis();
			long delta = end - start;

			start = end;

			throttle.tick(delta);

			if (this.writeThroughput != 0) {
				writeThrottle.tick(delta);
			}
		}
	}

	protected void runNextCommand() {
		for (RWTaskAsync task : this.tasks) {
			if (!task.isRunning) {
				task.runNextCommand();
			}
		}
	}

	@Override
	public void stop() {
		this.valid = false;

		for (RWTask task : tasks) {
			task.stop();
		}
	}
}

class Throttle {
	private double partial = 0.0;

	private AtomicLong quota;

	private double perMilli;

	public Throttle(AtomicLong quota, int eventsPerSecond, int cyclesPerSecond) {
		this.quota = quota;

		this.perMilli = eventsPerSecond / 1000.0;

		// Prime the quota with one cycle of work.
		int eventsPerCycle = (eventsPerSecond + cyclesPerSecond - 1) / cyclesPerSecond;
		this.quota.addAndGet(eventsPerCycle);
	}

	public void tick(long delta) {
		double newEventsReal = delta * this.perMilli + partial;
		int newEvents = (int) Math.floor(newEventsReal);

		partial = newEventsReal - newEvents;
		this.quota.addAndGet(newEvents);
	}
}