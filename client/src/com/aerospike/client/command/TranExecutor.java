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
package com.aerospike.client.command;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.BatchRecord;
import com.aerospike.client.Key;
import com.aerospike.client.Tran;
import com.aerospike.client.cluster.Cluster;
import com.aerospike.client.policy.BatchPolicy;
import java.util.List;
import java.util.Map;
import java.util.Set;

public final class TranExecutor {
    public static void commit(Cluster cluster, Tran tran, BatchPolicy verifyPolicy, BatchPolicy rollPolicy) {
        // Validate record versions in a batch.
        Set<Map.Entry<Key,Long>> reads = tran.getReads();
        int max = reads.size();

        if (max > 0) {
            BatchRecord[] records = new BatchRecord[max];
            Key[] keys = new Key[max];
            Long[] versions = new Long[max];
            int count = 0;

            for (Map.Entry<Key,Long> entry : reads) {
                Key key = entry.getKey();
                keys[count] = key;
                records[count] = new BatchRecord(key, false);
                versions[count] = entry.getValue();
                count++;
            }

            try {
                BatchStatus status = new BatchStatus(true);
                List<BatchNode> bns = BatchNodeList.generate(cluster, verifyPolicy, keys, records, false, status);
                IBatchCommand[] commands = new IBatchCommand[bns.size()];

                count = 0;

                for (BatchNode bn : bns) {
                    if (bn.offsetsSize == 1) {
                        int i = bn.offsets[0];
                        commands[count++] = new BatchSingle.TranVerify(
                            cluster, verifyPolicy, versions[i], records[i], status, bn.node);
                    }
                    else {
                        commands[count++] = new Batch.TranVerify(
                            cluster, bn, verifyPolicy, keys, versions, records, status);
                    }
                }

                BatchExecutor.execute(cluster, verifyPolicy, commands, status);

                if (!status.getStatus()) {
                    throw new Exception("Failed to verify one or more record versions");
                }
            }
            catch (Throwable e) {
                // Read verification failed. Abort transaction.
                try {
                    roll(cluster, tran, rollPolicy, Command.INFO4_MRT_ROLL_BACK);
                }
                catch (Throwable e2) {
                    // Throw combination of tranAbort and original exception.
                    e.addSuppressed(e2);
                    throw new AerospikeException.BatchRecordArray(records,
                        "Batch record verification and tran abort failed: " + tran.getId(), e);
                }

                // Throw original exception when abort succeeds.
                throw new AerospikeException.BatchRecordArray(records,
                    "Batch record verification failed. Tran aborted: " + tran.getId(), e);
            }
        }

        // System.out.println("Tran version matched: " + tran.id);
        roll(cluster, tran, rollPolicy, Command.INFO4_MRT_ROLL_FORWARD);
    }

    public static void roll(Cluster cluster, Tran tran, BatchPolicy rollPolicy, int tranAttr) {
        Set<Key> keySet = tran.getWrites();

        if (keySet.isEmpty()) {
            return;
        }

        Key[] keys = keySet.toArray(new Key[keySet.size()]);
        BatchRecord[] records = new BatchRecord[keys.length];

        for (int i = 0; i < keys.length; i++) {
            records[i] = new BatchRecord(keys[i], true);
        }

        try {
            // Copy tran roll policy because it needs to be modified.
            BatchPolicy batchPolicy = new BatchPolicy(rollPolicy);

            BatchAttr attr = new BatchAttr();
            attr.setTran(tranAttr);

            BatchStatus status = new BatchStatus(true);

            // generate() requires a null tran instance.
            List<BatchNode> bns = BatchNodeList.generate(cluster, batchPolicy, keys, records, true, status);
            IBatchCommand[] commands = new IBatchCommand[bns.size()];

            // Batch roll forward requires the tran instance.
            batchPolicy.tran = tran;

            int count = 0;

            for (BatchNode bn : bns) {
                if (bn.offsetsSize == 1) {
                    int i = bn.offsets[0];
                    commands[count++] = new BatchSingle.TranRoll(
                        cluster, batchPolicy, records[i], status, bn.node, tranAttr);
                }
                else {
                    commands[count++] = new Batch.TranRoll(
                        cluster, bn, batchPolicy, keys, records, attr, status);
                }
            }
            BatchExecutor.execute(cluster, batchPolicy, commands, status);

            if (!status.getStatus()) {
                String rollString = tranAttr == Command.INFO4_MRT_ROLL_FORWARD? "commit" : "abort";
                throw new Exception("Failed to " + rollString + " one or more records");
            }
        }
        catch (Throwable e) {
            String rollString = tranAttr == Command.INFO4_MRT_ROLL_FORWARD? "commit" : "abort";
            throw new AerospikeException.BatchRecordArray(records,
                "Tran " + rollString + " failed: " + tran.getId(), e);
        }
    }
}
