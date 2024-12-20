package com.aerospike.benchmarks;

import com.aerospike.client.*;

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
        return PerformMRTCommit(client, txn, 1, null);
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
