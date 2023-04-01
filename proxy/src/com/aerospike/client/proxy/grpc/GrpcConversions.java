package com.aerospike.client.proxy.grpc;

import com.aerospike.client.Operation;
import com.aerospike.client.Value;
import com.aerospike.client.policy.Policy;
import com.aerospike.client.policy.QueryPolicy;
import com.aerospike.client.policy.ScanPolicy;
import com.aerospike.client.policy.WritePolicy;
import com.aerospike.client.query.Filter;
import com.aerospike.client.query.PartitionFilter;
import com.aerospike.client.query.Statement;
import com.aerospike.client.util.Packer;
import com.aerospike.proxy.client.Kvs;
import com.google.protobuf.ByteString;

/**
 * Conversions from native client objects to Grpc objects.
 */
public class GrpcConversions {
    public static void setRequestPolicy(Policy policy,
                                        Kvs.AerospikeRequestPayload.Builder requestBuilder) {
        if (policy instanceof WritePolicy) {
            Kvs.WritePolicy.Builder writePolicyBuilder =
                Kvs.WritePolicy.newBuilder();
            Kvs.ReadModeAP readModeAP =
                Kvs.ReadModeAP.valueOf(policy.readModeAP.name());
            writePolicyBuilder.setReadModeAP(readModeAP);
            Kvs.ReadModeSC readModeSC =
                Kvs.ReadModeSC.valueOf(policy.readModeSC.name());
            writePolicyBuilder.setReadModeSC(readModeSC);
            Kvs.Replica replica =
                Kvs.Replica.valueOf(policy.replica.name());
            writePolicyBuilder.setReplica(replica);
            requestBuilder.setWritePolicy(writePolicyBuilder.build());
        }
        else {
            Kvs.ReadPolicy.Builder readPolicyBuilder =
                Kvs.ReadPolicy.newBuilder();
            Kvs.ReadModeAP readModeAP =
                Kvs.ReadModeAP.valueOf(policy.readModeAP.name());
            readPolicyBuilder.setReadModeAP(readModeAP);
            Kvs.ReadModeSC readModeSC =
                Kvs.ReadModeSC.valueOf(policy.readModeSC.name());
            readPolicyBuilder.setReadModeSC(readModeSC);
            Kvs.Replica replica =
                Kvs.Replica.valueOf(policy.replica.name());
            readPolicyBuilder.setReplica(replica);
            requestBuilder.setReadPolicy(readPolicyBuilder.build());
        }
    }

    public static Kvs.ScanPolicy toGrpc(ScanPolicy scanPolicy) {
        // Base policy fields.
        Kvs.ScanPolicy.Builder scanPolicyBuilder =
            Kvs.ScanPolicy.newBuilder();
        Kvs.ReadModeAP readModeAP =
            Kvs.ReadModeAP.valueOf(scanPolicy.readModeAP.name());
        scanPolicyBuilder.setReadModeAP(readModeAP);
        Kvs.ReadModeSC readModeSC =
            Kvs.ReadModeSC.valueOf(scanPolicy.readModeSC.name());
        scanPolicyBuilder.setReadModeSC(readModeSC);
        Kvs.Replica replica =
            Kvs.Replica.valueOf(scanPolicy.replica.name());
        scanPolicyBuilder.setReplica(replica);
        if (scanPolicy.filterExp != null) {
            scanPolicyBuilder.setExpression(ByteString.copyFrom(scanPolicy.filterExp.getBytes()));
        }

        scanPolicyBuilder.setTotalTimeout(scanPolicy.totalTimeout);
        scanPolicyBuilder.setCompress(scanPolicy.compress);


        // Scan policy specific fields
        scanPolicyBuilder.setMaxRecords(scanPolicy.maxRecords);
        scanPolicyBuilder.setRecordsPerSecond(scanPolicy.recordsPerSecond);
        scanPolicyBuilder.setMaxConcurrentNodes(scanPolicy.maxConcurrentNodes);
        scanPolicyBuilder.setConcurrentNodes(scanPolicy.concurrentNodes);
        scanPolicyBuilder.setIncludeBinData(scanPolicy.includeBinData);

        return scanPolicyBuilder.build();
    }

    public static Kvs.QueryPolicy toGrpc(QueryPolicy queryPolicy) {
        // Base policy fields.
        Kvs.QueryPolicy.Builder queryPolicyBuilder =
            Kvs.QueryPolicy.newBuilder();
        Kvs.ReadModeAP readModeAP =
            Kvs.ReadModeAP.valueOf(queryPolicy.readModeAP.name());
        queryPolicyBuilder.setReadModeAP(readModeAP);
        Kvs.ReadModeSC readModeSC =
            Kvs.ReadModeSC.valueOf(queryPolicy.readModeSC.name());
        queryPolicyBuilder.setReadModeSC(readModeSC);
        Kvs.Replica replica =
            Kvs.Replica.valueOf(queryPolicy.replica.name());
        queryPolicyBuilder.setReplica(replica);
        if (queryPolicy.filterExp != null) {
            queryPolicyBuilder.setExpression(ByteString.copyFrom(queryPolicy.filterExp.getBytes()));
        }

        queryPolicyBuilder.setTotalTimeout(queryPolicy.totalTimeout);
        queryPolicyBuilder.setCompress(queryPolicy.compress);
        queryPolicyBuilder.setSendKey(queryPolicy.sendKey);

        // Query policy specific fields
        queryPolicyBuilder.setMaxConcurrentNodes(queryPolicy.maxConcurrentNodes);
        queryPolicyBuilder.setRecordQueueSize(queryPolicy.recordQueueSize);
        queryPolicyBuilder.setIncludeBinData(queryPolicy.includeBinData);
        queryPolicyBuilder.setFailOnClusterChange(queryPolicy.failOnClusterChange);
        queryPolicyBuilder.setShortQuery(queryPolicy.shortQuery);

        return queryPolicyBuilder.build();
    }


    /**
     * Convert a value to packed bytes.
     *
     * @param value the value to pack
     * @return the packed bytes.
     */
    public static ByteString valueToByteString(Value value) {
        // TODO: @Brian is there a better way to convert value to bytes?
        // This involves two copies. One when returning bytes Packer
        // and one for the byte string.
        Packer packer = new Packer();
        value.pack(packer);
        return ByteString.copyFrom(packer.toByteArray());
    }

    public static Kvs.Filter toGrpc(Filter filter) {
        Kvs.Filter.Builder builder = Kvs.Filter.newBuilder();

        builder.setName(filter.getName());
        builder.setValType(filter.getValType());

        if (filter.getBegin() != null) {
            // TODO: @Brian is there a better way to convert value to bytes?
            // This involves two copies. One when returning bytes Packer
            // and one for the byte string.
            Packer packer = new Packer();
            filter.getBegin().pack(packer);
            builder.setBegin(ByteString.copyFrom(packer.toByteArray()));
        }

        if (filter.getBegin() != null) {
            builder.setBegin(valueToByteString(filter.getBegin()));
        }

        if (filter.getEnd() != null) {
            builder.setEnd(valueToByteString(filter.getEnd()));
        }

        if (filter.getPackedCtx() != null) {
            builder.setPackedCtx(ByteString.copyFrom(filter.getPackedCtx()));
        }

        builder.setColType(Kvs.IndexCollectionType.valueOf(filter.getColType().name()));
        return builder.build();
    }

    public static Kvs.Operation toGrpc(Operation operation) {
        Kvs.Operation.Builder builder = Kvs.Operation.newBuilder();
        builder.setType(Kvs.OperationType.valueOf(operation.type.name()));

        if (operation.binName != null) {
            builder.setBinName(operation.binName);
        }

        if (operation.value != null) {
            builder.setValue(valueToByteString(operation.value));
        }

        return builder.build();
    }


    /**
     * @param statement Aerospike client statement
     * @return equivalent gRPC {@link com.aerospike.proxy.client.Kvs.Statement}
     */
    public static Kvs.Statement toGrpc(Statement statement) {
        Kvs.Statement.Builder statementBuilder = Kvs.Statement.newBuilder();
        statementBuilder.setNamespace(statement.getNamespace());
        if (statement.getSetName() != null) {
            statementBuilder.setSetName(statement.getSetName());
        }
        if (statement.getIndexName() != null) {
            statementBuilder.setIndexName(statement.getIndexName());
        }
        if (statement.getBinNames() != null) {
            for (String binName : statement.getBinNames()) {
                statementBuilder.addBinNames(binName);
            }
        }

        if (statement.getFilter() != null) {
            statementBuilder.setFilter(toGrpc(statement.getFilter()));
        }


        if (statement.getPackageName() != null) {
            statementBuilder.setPackageName(statement.getPackageName());
        }

        if (statement.getFunctionName() != null) {
            statementBuilder.setFunctionName(statement.getFunctionName());
        }

        if (statement.getFunctionArgs() != null) {
            for (Value arg : statement.getFunctionArgs()) {
                statementBuilder.addFunctionArgs(valueToByteString(arg));
            }
        }

        if (statement.getOperations() != null) {
            for (Operation operation : statement.getOperations()) {
                statementBuilder.addOperations(toGrpc(operation));
            }
        }

        if (statement.getTaskId() != 0) {
            statementBuilder.setTaskId(statement.getTaskId());
        }

        statementBuilder.setMaxRecords(statement.getMaxRecords());
        statementBuilder.setRecordsPerSecond(statement.getRecordsPerSecond());

        return statementBuilder.build();
    }

    public static Kvs.PartitionFilter toGrpc(PartitionFilter partitionFilter) {
        Kvs.PartitionFilter.Builder builder = Kvs.PartitionFilter.newBuilder();
        builder.setBegin(partitionFilter.getBegin());
        builder.setCount(partitionFilter.getCount());

        byte[] digest = partitionFilter.getDigest();
        if (digest != null && digest.length > 0) {
            builder.setDigest(ByteString.copyFrom(digest));
        }
        return builder.build();
    }

    public static Kvs.BackgroundExecutePolicy toGrpc(WritePolicy writePolicy) {
        // Base policy fields.
        Kvs.BackgroundExecutePolicy.Builder queryPolicyBuilder = Kvs.BackgroundExecutePolicy.newBuilder();
        Kvs.ReadModeAP readModeAP =
            Kvs.ReadModeAP.valueOf(writePolicy.readModeAP.name());
        queryPolicyBuilder.setReadModeAP(readModeAP);
        Kvs.ReadModeSC readModeSC =
            Kvs.ReadModeSC.valueOf(writePolicy.readModeSC.name());
        queryPolicyBuilder.setReadModeSC(readModeSC);
        Kvs.Replica replica =
            Kvs.Replica.valueOf(writePolicy.replica.name());
        queryPolicyBuilder.setReplica(replica);
        if (writePolicy.filterExp != null) {
            queryPolicyBuilder.setExpression(ByteString.copyFrom(writePolicy.filterExp.getBytes()));
        }

        queryPolicyBuilder.setTotalTimeout(writePolicy.totalTimeout);
        queryPolicyBuilder.setCompress(writePolicy.compress);
        queryPolicyBuilder.setSendKey(writePolicy.sendKey);

        // Query policy specific fields
        queryPolicyBuilder.setRecordExistsAction(Kvs.RecordExistsAction.valueOf(writePolicy.recordExistsAction.name()));
        queryPolicyBuilder.setGenerationPolicy(Kvs.GenerationPolicy.valueOf(writePolicy.generationPolicy.name()));
        queryPolicyBuilder.setCommitLevel(Kvs.CommitLevel.valueOf(writePolicy.commitLevel.name()));
        queryPolicyBuilder.setGeneration(writePolicy.generation);
        queryPolicyBuilder.setExpiration(writePolicy.expiration);
        queryPolicyBuilder.setRespondAllOps(writePolicy.respondAllOps);
        queryPolicyBuilder.setDurableDelete(writePolicy.durableDelete);
        queryPolicyBuilder.setXdr(writePolicy.xdr);
        return queryPolicyBuilder.build();
    }
}
