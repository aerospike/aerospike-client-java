/*
 * Copyright 2012 The Netty Project
 *
 * The Netty Project licenses this file to you under the Apache License,
 * version 2.0 (the "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at:
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */
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
package com.aerospike.client.async;

import java.util.concurrent.TimeUnit;

import com.aerospike.client.Log;

/**
 * This HashedWheelTimer is a simplified version of netty's
 * <a href="https://github.com/netty/netty/blob/ed37cf20ef1ce2c792d57b02ef6494880b24f72d/common/src/main/java/io/netty/util/HashedWheelTimer.java">HashedWheelTimer</a>.
 * <p>
 * {@link HashedWheelTimer} is based on
 * <a href="http://cseweb.ucsd.edu/users/varghese/">George Varghese</a> and
 * Tony Lauck's paper,
 * <a href="http://cseweb.ucsd.edu/users/varghese/PAPERS/twheel.ps.Z">'Hashed
 * and Hierarchical Timing Wheels: data structures to efficiently implement a
 * timer facility'</a>.  More comprehensive slides are located
 * <a href="http://www.cse.wustl.edu/~cdgill/courses/cs6874/TimingWheels.ppt">here</a>.
 * <p>
 * This HashedWheelTimer runs directly in each event loop thread.  All HashedWheelTimer method calls
 * must also occur in its defined event loop thread.
 */
public final class HashedWheelTimer implements Runnable {
	private final EventLoop eventLoop;
	private final HashedWheelBucket[] wheel;
	private final ScheduleTask schedule;
	private final long tickDuration;
	private long startTime;
	private long tick;
	private final int mask;
	private boolean scheduled;

	public HashedWheelTimer(EventLoop eventLoop, long tickDuration, TimeUnit unit, int ticksPerWheel) {
		this.eventLoop = eventLoop;
		this.tickDuration = unit.toNanos(tickDuration);

        int normalizedTicksPerWheel = 1;

		while (normalizedTicksPerWheel < ticksPerWheel) {
			normalizedTicksPerWheel <<= 1;
		}

        wheel = new HashedWheelBucket[normalizedTicksPerWheel];

		for (int i = 0; i < wheel.length; i++) {
			wheel[i] = new HashedWheelBucket();
		}
		mask = wheel.length - 1;

		// Prevent overflow.
		if (this.tickDuration >= Long.MAX_VALUE / wheel.length) {
			throw new IllegalArgumentException(String.format(
				"tickDuration: %d (expected: 0 < tickDuration in nanos < %d",
				tickDuration, Long.MAX_VALUE / wheel.length));
		}

		schedule = new ScheduleTask(this);
	}

	public HashedWheelTimeout addTimeout(TimerTask task, long deadline) {
		if (! scheduled) {
			scheduled = true;
			startTime = System.nanoTime();
			HashedWheelTimeout timeout = addTimeout(task, deadline);  // recursive call
			eventLoop.schedule(schedule, tickDuration, TimeUnit.NANOSECONDS);
			return timeout;
		}

		HashedWheelTimeout timeout = new HashedWheelTimeout(task, deadline - startTime);

		long calculated = timeout.deadline / tickDuration;
		timeout.remainingRounds = (calculated - tick) / wheel.length;

		final long ticks = Math.max(calculated, tick);
		int stopIndex = (int) (ticks & mask);

		HashedWheelBucket bucket = wheel[stopIndex];
		bucket.addTimeout(timeout);
		return timeout;
	}

	public void restoreTimeout(HashedWheelTimeout timeout, long deadline) {
		timeout.deadline = deadline - startTime;
		timeout.next = null;
		timeout.prev = null;

		long calculated = timeout.deadline / tickDuration;
		timeout.remainingRounds = (calculated - tick) / wheel.length;

		final long ticks = Math.max(calculated, tick);
		int stopIndex = (int) (ticks & mask);

		HashedWheelBucket bucket = wheel[stopIndex];
		bucket.addTimeout(timeout);
	}

	public void run() {
		long currentTime = System.nanoTime() - startTime;
		long expectTime = tickDuration * (tick + 1);

		while (expectTime <= currentTime) {
			int idx = (int) (tick & mask);
			wheel[idx].expireTimeouts(currentTime);
			tick++;
			expectTime += tickDuration;
		}
		eventLoop.schedule(schedule, expectTime - currentTime, TimeUnit.NANOSECONDS);
	}

    public static final class HashedWheelTimeout {
		private final TimerTask task;
		private long deadline;
		private long remainingRounds;
		private HashedWheelTimeout next;
		private HashedWheelTimeout prev;
		private HashedWheelBucket bucket;

		private HashedWheelTimeout(TimerTask task, long deadline) {
			this.task = task;
			this.deadline = deadline;
		}

		public void cancel() {
			if (bucket != null) {
				bucket.remove(this);
			}
		}

		private void expire() {
			try {
			    task.timeout();
			} catch (Throwable t) {
				if (Log.warnEnabled()) {
					Log.warn("task.timeout() failed: " + t.getMessage());
				}
			}
		}
    }

    private static final class HashedWheelBucket {
		private HashedWheelTimeout head;
		private HashedWheelTimeout tail;

		public void addTimeout(HashedWheelTimeout timeout) {
			timeout.bucket = this;
			if (head == null) {
				head = tail = timeout;
			} else {
				tail.next = timeout;
				timeout.prev = tail;
				tail = timeout;
			}
		}

		public void expireTimeouts(long deadline) {
			HashedWheelTimeout timeout = head;

			// process all timeouts
			while (timeout != null) {
				HashedWheelTimeout next = timeout.next;
				if (timeout.remainingRounds <= 0) {
					next = remove(timeout);

					if (timeout.deadline > deadline) {
						// Should not happen.  Do not throw exception because that would break all
						// other timeouts in this iteration.
						if (Log.warnEnabled()) {
							Log.warn("timeout.deadline (" + timeout.deadline + ") > deadline (" + deadline + ")");
						}
					}
					timeout.expire();
				} else {
					timeout.remainingRounds--;
				}
				timeout = next;
			}
		}

		public HashedWheelTimeout remove(HashedWheelTimeout timeout) {
			HashedWheelTimeout next = timeout.next;
			// remove timeout that was either processed or cancelled by updating the linked-list
			if (timeout.prev != null) {
				timeout.prev.next = next;
			}
			if (timeout.next != null) {
				timeout.next.prev = timeout.prev;
			}

			if (timeout == head) {
				// if timeout is also the tail we need to adjust the entry too
				if (timeout == tail) {
					tail = null;
					head = null;
				} else {
					head = next;
				}
			} else if (timeout == tail) {
				// if the timeout is the tail modify the tail to be the prev node.
				tail = timeout.prev;
			}
			// null out prev, next and bucket to allow for GC.
			timeout.prev = null;
			timeout.next = null;
			timeout.bucket = null;
			return next;
		}
	}

    /*
    public void printRemaining() {
		System.out.println("Search remaining buckets");

    	for (int i = 0; i < wheel.length; i++) {
    		HashedWheelBucket bucket = wheel[i];

    		if (bucket.head != null || bucket.tail != null) {
    			System.out.println("Bucket " + i + " exists");
    		}
    	}
    }
    */
}
