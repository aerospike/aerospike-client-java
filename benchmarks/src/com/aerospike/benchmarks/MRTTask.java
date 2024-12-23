package com.aerospike.benchmarks;

import com.aerospike.client.*;
import com.aerospike.client.policy.BatchPolicy;
import com.aerospike.client.policy.Policy;
import com.aerospike.client.policy.WritePolicy;
import com.aerospike.client.Record;

public abstract class MRTTask {

    final Arguments args;
    final CounterStore counters;
    final int blockRetries;
    final int blockRetriesSleepMillis;
    final int txnTimeoutSeconds;

    public MRTTask(Arguments args, CounterStore counters) {
        this.args = args;
        this.counters = counters;
        this.blockRetries = args.mrtBlockRetries;
        this.blockRetriesSleepMillis = args.mrtBlockSleepMS;
        this.txnTimeoutSeconds = args.mrtTimeoutSec;
    }

    public static final class MRTHandleResult {
        public final boolean successful;
        public  final long totalElapseTime;
        public  final boolean commitProcessed;

        public MRTHandleResult(boolean successful, boolean commit, long totalElapseTime) {
            this.successful = successful;
            this.commitProcessed = commit;
            this.totalElapseTime = totalElapseTime;
        }

        public MRTHandleResult(long totalElapseTime) {
            this.successful = true;
            this.commitProcessed = false;
            this.totalElapseTime = totalElapseTime;
        }
    }

    protected void putUoW(IAerospikeClient client, WritePolicy writePolicy, Key key, Bin[] bins)
            throws AerospikeException {
        putUoW(client, writePolicy, key, bins, 0);
    }

    protected void putUoW(IAerospikeClient client, WritePolicy writePolicy, Key key, Bin[] bins, int retry)
            throws AerospikeException {

        try{
            if (counters.write.latency != null) {
                long begin = System.nanoTime();
                client.put(writePolicy, key, bins);
                long elapsed = System.nanoTime() - begin;
                counters.write.count.getAndIncrement();
                counters.write.latency.add(elapsed);
            } else {
                client.put(writePolicy, key, bins);
                counters.write.count.getAndIncrement();
                counters.write.incrTransCountOTel(LatencyTypes.WRITE);
            }
        }
        catch (AerospikeException ae) {
            if(ae.getResultCode() == 120 && (args.mrtBlockRetries < 0 || retry < args.mrtBlockRetries)) {
                counters.write.errors.getAndIncrement();
                counters.write.addExceptionOTel(ae, LatencyTypes.WRITE);
                counters.write.blocked.getAndIncrement();
                putUoW(client, writePolicy, key, bins, retry + 1);
                return;
            }
            throw ae;
        }
    }

    protected void addUoW(IAerospikeClient client, WritePolicy writePolicy, Key key, Bin[] bins)
            throws AerospikeException {
        addUoW(client, writePolicy, key, bins, 0);
    }

    protected void addUoW(IAerospikeClient client, WritePolicy writePolicy, Key key, Bin[] bins, int retry)
            throws AerospikeException {

        try{
            if (counters.write.latency != null) {
                long begin = System.nanoTime();
                client.add(writePolicy, key, bins);
                long elapsed = System.nanoTime() - begin;
                counters.write.count.getAndIncrement();
                counters.write.latency.add(elapsed);
            } else {
                client.add(writePolicy, key, bins);
                counters.write.count.getAndIncrement();
                counters.write.incrTransCountOTel(LatencyTypes.WRITE);
            }
        }
        catch (AerospikeException ae) {
            if(ae.getResultCode() == 120 && (args.mrtBlockRetries < 0 || retry < args.mrtBlockRetries)) {
                counters.write.errors.getAndIncrement();
                counters.write.addExceptionOTel(ae, LatencyTypes.WRITE);
                counters.write.blocked.getAndIncrement();
                addUoW(client, writePolicy, key, bins, retry + 1);
                return;
            }
            throw ae;
        }
    }

    protected Record getUoW(IAerospikeClient client, Policy readPolicy, Key key, String binName)
            throws AerospikeException {
        return getUoW(client, readPolicy, key, binName, 0);
    }

    protected Record getUoW(IAerospikeClient client, Policy readPolicy, Key key, String binName, int retry)
            throws AerospikeException {

        Record record;
        try{
            if (counters.read.latency != null) {
                long begin = System.nanoTime();
                record = client.get(readPolicy, key, binName);
                long elapsed = System.nanoTime() - begin;
                counters.read.count.getAndIncrement();
                counters.read.latency.add(elapsed);
            } else {
                record = client.get(readPolicy, key, binName);
                counters.read.count.getAndIncrement();
                counters.read.incrTransCountOTel(LatencyTypes.READ);
            }
        }
        catch (AerospikeException ae) {
            if(ae.getResultCode() == 120 && (args.mrtBlockRetries < 0 || retry < args.mrtBlockRetries)) {
                counters.read.errors.getAndIncrement();
                counters.read.addExceptionOTel(ae, LatencyTypes.READ);
                counters.read.blocked.getAndIncrement();
                return getUoW(client, readPolicy, key, binName, retry + 1);
            }
            throw ae;
        }
        return record;
    }

    protected Record getUoW(IAerospikeClient client, Policy readPolicy, Key key)
            throws AerospikeException {
        return getUoW(client, readPolicy, key, 0);
    }

    protected Record getUoW(IAerospikeClient client, Policy readPolicy, Key key, int retry)
            throws AerospikeException {

        Record record;
        try{
            if (counters.read.latency != null) {
                long begin = System.nanoTime();
                record = client.get(readPolicy, key);
                long elapsed = System.nanoTime() - begin;
                counters.read.count.getAndIncrement();
                counters.read.latency.add(elapsed);
            } else {
                record = client.get(readPolicy, key);
                counters.read.count.getAndIncrement();
                counters.read.incrTransCountOTel(LatencyTypes.READ);
            }
        }
        catch (AerospikeException ae) {
            if(ae.getResultCode() == 120 && (args.mrtBlockRetries < 0 || retry < args.mrtBlockRetries)) {
                counters.read.errors.getAndIncrement();
                counters.read.addExceptionOTel(ae, LatencyTypes.READ);
                counters.read.blocked.getAndIncrement();
                return getUoW(client, readPolicy, key, retry + 1);
            }
            throw ae;
        }
        return record;
    }

    protected Record[] getUoW(IAerospikeClient client, BatchPolicy batchPolicy, Key[] keys, String binName)
            throws AerospikeException {
        return getUoW(client, batchPolicy, keys, binName, 0);
    }

    protected Record[] getUoW(IAerospikeClient client, BatchPolicy batchPolicy, Key[] keys, String binName, int retry)
            throws AerospikeException {

        Record[] records;
        try{
            if (counters.read.latency != null) {
                long begin = System.nanoTime();
                records = client.get(batchPolicy, keys, binName);
                long elapsed = System.nanoTime() - begin;
                counters.read.count.getAndIncrement();
                counters.read.latency.add(elapsed);
            } else {
                records = client.get(batchPolicy, keys, binName);
                counters.read.count.getAndIncrement();
                counters.read.incrTransCountOTel(LatencyTypes.READ);
            }
        }
        catch (AerospikeException ae) {
            if(ae.getResultCode() == 120 && (args.mrtBlockRetries < 0 || retry < args.mrtBlockRetries)) {
                counters.read.errors.getAndIncrement();
                counters.read.addExceptionOTel(ae, LatencyTypes.READ);
                counters.read.blocked.getAndIncrement();
                return getUoW(client, batchPolicy, keys, binName, retry + 1);
            }
            throw ae;
        }
        return records;
    }

    protected Record[] getUoW(IAerospikeClient client, BatchPolicy batchPolicy, Key[] keys)
            throws AerospikeException {
        return getUoW(client, batchPolicy, keys, 0);
    }

    protected Record[] getUoW(IAerospikeClient client, BatchPolicy batchPolicy, Key[] keys, int retry)
            throws AerospikeException {

        Record[] records;
        try{
            if (counters.read.latency != null) {
                long begin = System.nanoTime();
                records = client.get(batchPolicy, keys);
                long elapsed = System.nanoTime() - begin;
                counters.read.count.getAndIncrement();
                counters.read.latency.add(elapsed);
            } else {
                records = client.get(batchPolicy, keys);
                counters.read.count.getAndIncrement();
                counters.read.incrTransCountOTel(LatencyTypes.READ);
            }
        }
        catch (AerospikeException ae) {
            if(ae.getResultCode() == 120 && (args.mrtBlockRetries < 0 || retry < args.mrtBlockRetries)) {
                counters.read.errors.getAndIncrement();
                counters.read.addExceptionOTel(ae, LatencyTypes.READ);
                counters.read.blocked.getAndIncrement();
                return getUoW(client, batchPolicy, keys, retry + 1);
            }
            throw ae;
        }
        return records;
    }


    public MRTHandleResult CompleteUoW(IAerospikeClient client,
                                       Txn txn,
                                       long beginTime,
                                       boolean performCommit) throws Exception {
        long uowElapse = 0;
        boolean successful = false;

        //Finish latency/counters for the UoW actions (puts,gets)...
        if(counters.mrtUnitOfWork.latency != null) {
            uowElapse = System.nanoTime() - beginTime;
            counters.mrtUnitOfWork.count.getAndIncrement();
            counters.mrtUnitOfWork.latency.add(uowElapse);
        }
        else {
            counters.mrtUnitOfWork.count.getAndIncrement();
            counters.mrtUnitOfWork.incrTransCountOTel(LatencyTypes.MRTUOW);
        }

        if (performCommit) {
            MRTHandleResult result = PerformMRTCommit(client,txn);
            successful = result.successful;
            performCommit = result.commitProcessed;
            uowElapse += result.totalElapseTime;
        } else {
            uowElapse += PerformMRTAbort(client,txn);
            successful = true;
            performCommit = false;
        }

        return new MRTHandleResult(successful, performCommit, uowElapse);
    }

    public MRTHandleResult PerformMRTCommit(IAerospikeClient client, Txn txn) {
        return PerformMRTCommit(client, txn, 0, null);
    }

    public MRTHandleResult PerformMRTCommit(IAerospikeClient client,
                                            Txn txn,
                                            int tryNum,
                                            AerospikeException ae) {

        if(ae != null) {
            if (!ae.getInDoubt()) {
                counters.mrtCommit.addExceptionOTel(ae, LatencyTypes.MRTCOMMIT);
                counters.mrtCommit.errors.getAndIncrement();
                return new MRTHandleResult(PerformMRTAbort(client, txn));
            }

            if (args.mrtInDoubtRetries == 0 || (args.mrtInDoubtRetries >= 1 && tryNum >= args.mrtInDoubtRetries)) {
                counters.mrtCommit.addExceptionOTel(ae, LatencyTypes.MRTCOMMIT);
                counters.mrtCommit.errors.getAndIncrement();
                return new MRTHandleResult(PerformMRTAbort(client, txn));
            }

            if (args.mrtRetrySleepMS > 0) {
                try {
                    Thread.sleep(args.mrtRetrySleepMS);
                } catch (InterruptedException ignored) { }
            }
        }

        long elapsed;
        boolean successful;
        boolean commitProcessed = true;
        final long begin = System.nanoTime();

        try {
            CommitStatus status = client.commit(txn);
            elapsed = System.nanoTime() - begin;
            counters.mrtCommit.count.getAndIncrement();
            if(counters.mrtCommit.latency != null) {
                counters.mrtCommit.latency.add(elapsed);
            }
            else {
                counters.mrtCommit.incrTransCountOTel(LatencyTypes.MRTCOMMIT);
            }
            if(status == CommitStatus.ALREADY_ABORTED || status == CommitStatus.CLOSE_ABANDONED) {
                successful = false;
                counters.mrtCommit.addExceptionOTel("CommitStatus", status.toString(), "UoW treated as Not Completed", LatencyTypes.MRTCOMMIT);
            }
            else {
                successful = true;
                if(status != CommitStatus.OK) {
                    counters.mrtCommit.addExceptionOTel("CommitStatus", status.toString(), "UoW treated as Committed/Completed", LatencyTypes.MRTCOMMIT);
                }
            }
        }
        catch(AerospikeException e) {
            elapsed = System.nanoTime() - begin;
            MRTHandleResult result = PerformMRTCommit(client, txn, tryNum + 1, e);
            elapsed += result.totalElapseTime;
            successful = result.successful;
            commitProcessed = result.commitProcessed;
        }
        catch (Exception e) {
            elapsed = System.nanoTime() - begin;
            elapsed += PerformMRTAbort(client, txn);
            successful = true;
            commitProcessed = false;
        }

        return new MRTHandleResult(successful, commitProcessed, elapsed);
    }

    public long PerformMRTAbort(IAerospikeClient client,
                                     Txn txn) {
        long abortElapseTime = 0;
        long begin = System.nanoTime();
        client.abort(txn);
        if (counters.mrtAbort.latency != null) {
            abortElapseTime = System.nanoTime() - begin;
            counters.mrtAbort.count.getAndIncrement();
            counters.mrtAbort.latency.add(abortElapseTime);
        } else {
            counters.mrtAbort.count.getAndIncrement();
            counters.mrtAbort.incrTransCountOTel(LatencyTypes.MRTABORT);
        }
        return abortElapseTime;
    }

}
