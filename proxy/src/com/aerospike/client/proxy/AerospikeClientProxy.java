/*
 * Copyright 2012-2023 Aerospike, Inc.
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
package com.aerospike.client.proxy;

import java.io.Closeable;
import java.util.Calendar;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.AerospikeException;
import com.aerospike.client.BatchRead;
import com.aerospike.client.BatchRecord;
import com.aerospike.client.BatchResults;
import com.aerospike.client.Bin;
import com.aerospike.client.Host;
import com.aerospike.client.IAerospikeClient;
import com.aerospike.client.Key;
import com.aerospike.client.Language;
import com.aerospike.client.Operation;
import com.aerospike.client.Record;
import com.aerospike.client.ResultCode;
import com.aerospike.client.ScanCallback;
import com.aerospike.client.Value;
import com.aerospike.client.admin.Privilege;
import com.aerospike.client.admin.Role;
import com.aerospike.client.admin.User;
import com.aerospike.client.async.EventLoop;
import com.aerospike.client.cdt.CTX;
import com.aerospike.client.cluster.Cluster;
import com.aerospike.client.cluster.ClusterStats;
import com.aerospike.client.cluster.Node;
import com.aerospike.client.exp.Expression;
import com.aerospike.client.listener.BatchListListener;
import com.aerospike.client.listener.BatchOperateListListener;
import com.aerospike.client.listener.BatchRecordArrayListener;
import com.aerospike.client.listener.BatchRecordSequenceListener;
import com.aerospike.client.listener.BatchSequenceListener;
import com.aerospike.client.listener.DeleteListener;
import com.aerospike.client.listener.ExecuteListener;
import com.aerospike.client.listener.ExistsArrayListener;
import com.aerospike.client.listener.ExistsListener;
import com.aerospike.client.listener.ExistsSequenceListener;
import com.aerospike.client.listener.IndexListener;
import com.aerospike.client.listener.InfoListener;
import com.aerospike.client.listener.RecordArrayListener;
import com.aerospike.client.listener.RecordListener;
import com.aerospike.client.listener.RecordSequenceListener;
import com.aerospike.client.listener.WriteListener;
import com.aerospike.client.policy.AdminPolicy;
import com.aerospike.client.policy.BatchDeletePolicy;
import com.aerospike.client.policy.BatchPolicy;
import com.aerospike.client.policy.BatchUDFPolicy;
import com.aerospike.client.policy.BatchWritePolicy;
import com.aerospike.client.policy.ClientPolicy;
import com.aerospike.client.policy.InfoPolicy;
import com.aerospike.client.policy.Policy;
import com.aerospike.client.policy.QueryPolicy;
import com.aerospike.client.policy.ScanPolicy;
import com.aerospike.client.policy.WritePolicy;
import com.aerospike.client.proxy.auth.AuthTokenManager;
import com.aerospike.client.proxy.grpc.GrpcCallExecutor;
import com.aerospike.client.proxy.grpc.GrpcChannelProvider;
import com.aerospike.client.query.IndexCollectionType;
import com.aerospike.client.query.IndexType;
import com.aerospike.client.query.PartitionFilter;
import com.aerospike.client.query.QueryListener;
import com.aerospike.client.query.RecordSet;
import com.aerospike.client.query.ResultSet;
import com.aerospike.client.query.Statement;
import com.aerospike.client.task.ExecuteTask;
import com.aerospike.client.task.IndexTask;
import com.aerospike.client.task.RegisterTask;
import com.aerospike.proxy.client.AboutGrpc;
import com.aerospike.proxy.client.Kvs;

import io.grpc.Status;
import io.grpc.StatusRuntimeException;

/**
 * Aerospike Proxy based implementation of {@link AerospikeClient}.
 */
public class AerospikeClientProxy implements IAerospikeClient, Closeable {
    /**
     * Proxy client version
     */
    public static String VERSION = getVersion();
    /**
     * Default read policy that is used when read command policy is null.
     */
    public final Policy readPolicyDefault;
    /**
     * Default write policy that is used when write command policy is null.
     */
    public final WritePolicy writePolicyDefault;
    /**
     * Default scan policy that is used when scan command policy is null.
     */
    public final ScanPolicy scanPolicyDefault;
    /**
     * Default query policy that is used when query command policy is null.
     */
    public final QueryPolicy queryPolicyDefault;
    /**
     * Default parent policy used in batch read commands. Parent policy fields
     * include socketTimeout, totalTimeout, maxRetries, etc...
     */
    public final BatchPolicy batchPolicyDefault;
    /**
     * Default parent policy used in batch write commands. Parent policy fields
     * include socketTimeout, totalTimeout, maxRetries, etc...
     */
    public final BatchPolicy batchParentPolicyWriteDefault;
    /**
     * Default write policy used in batch operate commands.
     * Write policy fields include generation, expiration, durableDelete, etc...
     */
    public final BatchWritePolicy batchWritePolicyDefault;
    /**
     * Default delete policy used in batch delete commands.
     */
    public final BatchDeletePolicy batchDeletePolicyDefault;
    /**
     * Default user defined function policy used in batch UDF excecute commands.
     */
    public final BatchUDFPolicy batchUDFPolicyDefault;
    /**
     * Default info policy that is used when info command policy is null.
     */
    public final InfoPolicy infoPolicyDefault;

    private final AuthTokenManager authTokenManager;

    /**
     * Call executor.
     */
    private final GrpcCallExecutor grpcCallExecutor;

    public AerospikeClientProxy(ClientPolicy policy, Host... hosts)
            throws AerospikeException {
        if (policy == null) {
            policy = new ClientPolicy();
            // Proxy defaults. Small number of connections work well.
            policy.minConnsPerNode = 1;
            policy.maxConnsPerNode = 8;
        }
        ClientPolicy clientPolicy = policy;
        try {
            GrpcChannelProvider channelProvider = new GrpcChannelProvider();
            this.authTokenManager = new AuthTokenManager(clientPolicy, channelProvider);
            this.grpcCallExecutor =
                    new GrpcCallExecutor(policy.maxConnsPerNode, 128,
                            policy.timeout, authTokenManager, policy.tlsPolicy, hosts);
            channelProvider.setCallExecutor(grpcCallExecutor);
            this.readPolicyDefault = policy.readPolicyDefault;
            this.writePolicyDefault = policy.writePolicyDefault;
            this.scanPolicyDefault = policy.scanPolicyDefault;
            this.queryPolicyDefault = policy.queryPolicyDefault;
            this.batchPolicyDefault = policy.batchPolicyDefault;
            this.batchParentPolicyWriteDefault = policy.batchParentPolicyWriteDefault;
            this.batchWritePolicyDefault = policy.batchWritePolicyDefault;
            this.batchDeletePolicyDefault = policy.batchDeletePolicyDefault;
            this.batchUDFPolicyDefault = policy.batchUDFPolicyDefault;
            this.infoPolicyDefault = policy.infoPolicyDefault;
        } catch (Exception e) {
        	e.printStackTrace();
            close();

            if (e instanceof AerospikeException) {
                throw e;
            }

            // TODO convert to correct result code.
            throw new AerospikeException(ResultCode.CLIENT_ERROR, e);
        }
    }

    /**
     * Tests whether the remote endpoint is a proxy server.
     * <p>
     * WARN: this method is invoked by reflection in AerospikeClient. Any
     * signature changes should be made at both places.
     *
     * @param clientPolicy aerospike client policy
     * @param hosts        remote end point host to connect to
     * @return true if the remote endpoint is a proxy server
     */
    @SuppressWarnings("unused")
    public static boolean isRemoteProxy(ClientPolicy clientPolicy,
                                        Host... hosts) {
        try (GrpcCallExecutor channelPool = new GrpcCallExecutor(1, 1,
                clientPolicy.timeout, null,
                clientPolicy.tlsPolicy, hosts)) {
            Kvs.AboutRequest aboutRequest = Kvs.AboutRequest.newBuilder().build();
            //noinspection ResultOfMethodCallIgnored
            AboutGrpc.newBlockingStub(channelPool.getChannel()).get(aboutRequest);
            return true;
        } catch (StatusRuntimeException e) {
            // TODO: figure out more failure conditions.
            if (e.getStatus() == Status.UNAVAILABLE && e.getMessage().equals(
                    "Network closed for unknown reason")) {
                // Aerospike receives a gRPC payload, which it recognises
                // as invalid and closes the connection.
                return false;
            }
            throw new AerospikeException(e);
        } catch (Exception e) {
            throw new AerospikeException(e);
        }
    }

    private static String getVersion() {
        final Properties properties = new Properties();
        String version = null;
        try {
            properties.load(AerospikeClientProxy.class.getClassLoader()
                    .getResourceAsStream("project.properties"));
            version = properties.getProperty("version");
        } catch (Exception ignored) {
        }
        if (version == null) {
            version = "N/A";
        }
        return version;
    }


    public final Policy getReadPolicyDefault() {
        return readPolicyDefault;
    }

    public final WritePolicy getWritePolicyDefault() {
        return writePolicyDefault;
    }

    public final ScanPolicy getScanPolicyDefault() {
        return scanPolicyDefault;
    }

    public final QueryPolicy getQueryPolicyDefault() {
        return queryPolicyDefault;
    }

    public final BatchPolicy getBatchPolicyDefault() {
        return batchPolicyDefault;
    }

    public final BatchPolicy getBatchParentPolicyWriteDefault() {
        return batchParentPolicyWriteDefault;
    }

    public final BatchWritePolicy getBatchWritePolicyDefault() {
        return batchWritePolicyDefault;
    }

    public final BatchDeletePolicy getBatchDeletePolicyDefault() {
        return batchDeletePolicyDefault;
    }

    public final BatchUDFPolicy getBatchUDFPolicyDefault() {
        return batchUDFPolicyDefault;
    }

    public final InfoPolicy getInfoPolicyDefault() {
        return infoPolicyDefault;
    }


    @Override
    public void close() {
        if (grpcCallExecutor != null) {
            grpcCallExecutor.close();
        }

        if (authTokenManager != null) {
            authTokenManager.close();
        }
    }

    @Override
    public boolean isConnected() {
        return false;
    }

    @Override
    public Node[] getNodes() {
        return new Node[0];
    }

    @Override
    public List<String> getNodeNames() {
        return null;
    }

    @Override
    public Node getNode(String nodeName) throws AerospikeException.InvalidNode {
        return null;
    }

    @Override
    public ClusterStats getClusterStats() {
        return null;
    }

    @Override
    public Cluster getCluster() {
        return null;
    }

    @Override
    public void put(WritePolicy policy, Key key, Bin... bins)
            throws AerospikeException {
        // TODO: abstract the common usage.
        CompletableFuture<Void> future = new CompletableFuture<>();
        WriteListener listener = new WriteListener() {
            @Override
            public void onSuccess(Key key) {
                future.complete(null);
            }

            @Override
            public void onFailure(AerospikeException exception) {
            	System.out.println("ERROR");
            	System.out.println(exception);
                future.completeExceptionally(exception);
            }
        };
        put(null, listener, policy, key, bins);

        try {
            future.get();
        } catch (ExecutionException e) {
            throw new AerospikeException(e);
        } catch (InterruptedException e) {
            // Restore interrupt.
            Thread.currentThread().interrupt();
            throw new AerospikeException(e);
        }
    }

    @Override
    public void put(EventLoop eventLoop, WriteListener listener,
                    WritePolicy policy, Key key, Bin... bins) throws AerospikeException {
        policy = policy != null ? policy : writePolicyDefault;
        WriteCommandProxy writeCommandProxy =
                new WriteCommandProxy(grpcCallExecutor,
                        policy, key, listener, bins);
        writeCommandProxy.execute();
    }

    @Override
    public void append(WritePolicy policy, Key key, Bin... bins)
            throws AerospikeException {

    }

    @Override
    public void append(EventLoop eventLoop, WriteListener listener,
                       WritePolicy policy, Key key, Bin... bins) throws AerospikeException {

    }

    @Override
    public void prepend(WritePolicy policy, Key key, Bin... bins)
            throws AerospikeException {

    }

    @Override
    public void prepend(EventLoop eventLoop, WriteListener listener,
                        WritePolicy policy, Key key, Bin... bins) throws AerospikeException {

    }

    @Override
    public void add(WritePolicy policy, Key key, Bin... bins)
            throws AerospikeException {

    }

    @Override
    public void add(EventLoop eventLoop, WriteListener listener,
                    WritePolicy policy, Key key, Bin... bins) throws AerospikeException {

    }

    @Override
    public boolean delete(WritePolicy policy, Key key)
            throws AerospikeException {
        return false;
    }

    @Override
    public void delete(EventLoop eventLoop, DeleteListener listener,
                       WritePolicy policy, Key key) throws AerospikeException {

    }

    @Override
    public BatchResults delete(BatchPolicy batchPolicy,
                               BatchDeletePolicy deletePolicy, Key[] keys) throws AerospikeException {
        return null;
    }

    @Override
    public void delete(EventLoop eventLoop, BatchRecordArrayListener listener,
                       BatchPolicy batchPolicy, BatchDeletePolicy deletePolicy, Key[] keys)
            throws AerospikeException {

    }

    @Override
    public void delete(EventLoop eventLoop,
                       BatchRecordSequenceListener listener,
                       BatchPolicy batchPolicy, BatchDeletePolicy deletePolicy, Key[] keys)
            throws AerospikeException {

    }

    @Override
    public void truncate(InfoPolicy policy, String ns, String set,
                         Calendar beforeLastUpdate) throws AerospikeException {

    }

    @Override
    public void touch(WritePolicy policy, Key key) throws AerospikeException {

    }

    @Override
    public void touch(EventLoop eventLoop, WriteListener listener,
                      WritePolicy policy, Key key) throws AerospikeException {

    }

    @Override
    public boolean exists(Policy policy, Key key) throws AerospikeException {
        return false;
    }

    @Override
    public void exists(EventLoop eventLoop, ExistsListener listener,
                       Policy policy, Key key) throws AerospikeException {

    }

    @Override
    public boolean[] exists(BatchPolicy policy, Key[] keys)
            throws AerospikeException {
        return new boolean[0];
    }

    @Override
    public void exists(EventLoop eventLoop, ExistsArrayListener listener,
                       BatchPolicy policy, Key[] keys) throws AerospikeException {

    }

    @Override
    public void exists(EventLoop eventLoop, ExistsSequenceListener listener,
                       BatchPolicy policy, Key[] keys) throws AerospikeException {

    }

    @Override
    public Record get(Policy policy, Key key) throws AerospikeException {
        return get(policy, key, (String[]) null);
    }

    @Override
    public void get(EventLoop eventLoop, RecordListener listener, Policy policy,
                    Key key) throws AerospikeException {
        get(eventLoop, listener, policy, key, (String[]) null);
    }

    @Override
    public Record get(Policy policy, Key key, String... binNames) throws
            AerospikeException {
        // TODO: abstract the common usage.
        CompletableFuture<Record> future = new CompletableFuture<>();
        RecordListener listener = new RecordListener() {
            @Override
            public void onSuccess(Key key, Record record) {
                future.complete(record);
            }

            @Override
            public void onFailure(AerospikeException exception) {
                future.completeExceptionally(exception);
            }
        };

        get(null, listener, policy, key, binNames);

        try {
            return future.get();
        } catch (ExecutionException e) {
            throw new AerospikeException(e);
        } catch (InterruptedException e) {
            // Restore interrupt.
            Thread.currentThread().interrupt();
            throw new AerospikeException(e);
        }
    }

    @Override
    public void get(EventLoop eventLoop, RecordListener listener, Policy policy,
                    Key key, String... binNames) throws AerospikeException {
        policy = policy != null ? policy : readPolicyDefault;
        ReadCommandProxy readCommand = new ReadCommandProxy(grpcCallExecutor,
                policy, key, binNames, listener);
        readCommand.execute();
    }

    @Override
    public Record getHeader(Policy policy, Key key) throws AerospikeException {
        return null;
    }

    @Override
    public void getHeader(EventLoop eventLoop, RecordListener listener,
                          Policy policy, Key key) throws AerospikeException {

    }

    @Override
    public boolean get(BatchPolicy policy, List<BatchRead> records)
            throws AerospikeException {
        return false;
    }

    @Override
    public void get(EventLoop eventLoop, BatchListListener listener,
                    BatchPolicy policy, List<BatchRead> records) throws AerospikeException {

    }

    @Override
    public void get(EventLoop eventLoop, BatchSequenceListener listener,
                    BatchPolicy policy, List<BatchRead> records) throws AerospikeException {

    }

    @Override
    public Record[] get(BatchPolicy policy, Key[] keys)
            throws AerospikeException {
        return new Record[0];
    }

    @Override
    public void get(EventLoop eventLoop, RecordArrayListener listener,
                    BatchPolicy policy, Key[] keys) throws AerospikeException {

    }

    @Override
    public void get(EventLoop eventLoop, RecordSequenceListener listener,
                    BatchPolicy policy, Key[] keys) throws AerospikeException {

    }

    @Override
    public Record[] get(BatchPolicy policy, Key[] keys, String... binNames)
            throws AerospikeException {
        return new Record[0];
    }

    @Override
    public void get(EventLoop eventLoop, RecordArrayListener listener,
                    BatchPolicy policy, Key[] keys, String... binNames)
            throws AerospikeException {

    }

    @Override
    public void get(EventLoop eventLoop, RecordSequenceListener listener,
                    BatchPolicy policy, Key[] keys, String... binNames)
            throws AerospikeException {

    }

    @Override
    public Record[] get(BatchPolicy policy, Key[] keys, Operation... ops)
            throws AerospikeException {
        return new Record[0];
    }

    @Override
    public void get(EventLoop eventLoop, RecordArrayListener listener,
                    BatchPolicy policy, Key[] keys, Operation... ops)
            throws AerospikeException {

    }

    @Override
    public void get(EventLoop eventLoop, RecordSequenceListener listener,
                    BatchPolicy policy, Key[] keys, Operation... ops)
            throws AerospikeException {

    }

    @Override
    public Record[] getHeader(BatchPolicy policy, Key[] keys)
            throws AerospikeException {
        return new Record[0];
    }

    @Override
    public void getHeader(EventLoop eventLoop, RecordArrayListener listener,
                          BatchPolicy policy, Key[] keys) throws AerospikeException {

    }

    @Override
    public void getHeader(EventLoop eventLoop, RecordSequenceListener listener,
                          BatchPolicy policy, Key[] keys) throws AerospikeException {

    }

    @Override
    public Record operate(WritePolicy policy, Key key, Operation... operations)
            throws AerospikeException {
        return null;
    }

    @Override
    public void operate(EventLoop eventLoop, RecordListener listener,
                        WritePolicy policy, Key key, Operation... operations)
            throws AerospikeException {

    }

    @Override
    public boolean operate(BatchPolicy policy, List<BatchRecord> records)
            throws AerospikeException {
        return false;
    }

    @Override
    public void operate(EventLoop eventLoop, BatchOperateListListener listener,
                        BatchPolicy policy, List<BatchRecord> records)
            throws AerospikeException {

    }

    @Override
    public void operate(EventLoop eventLoop,
                        BatchRecordSequenceListener listener,
                        BatchPolicy policy, List<BatchRecord> records)
            throws AerospikeException {

    }

    @Override
    public BatchResults operate(BatchPolicy batchPolicy,
                                BatchWritePolicy writePolicy, Key[] keys, Operation... ops)
            throws AerospikeException {
        return null;
    }

    @Override
    public void operate(EventLoop eventLoop, BatchRecordArrayListener listener,
                        BatchPolicy batchPolicy, BatchWritePolicy writePolicy, Key[] keys,
                        Operation... ops) throws AerospikeException {

    }

    @Override
    public void operate(EventLoop eventLoop,
                        BatchRecordSequenceListener listener,
                        BatchPolicy batchPolicy, BatchWritePolicy writePolicy, Key[] keys,
                        Operation... ops) throws AerospikeException {

    }

    @Override
    public void scanAll(ScanPolicy policy, String namespace, String setName,
                        ScanCallback callback, String... binNames) throws AerospikeException {
    }

    @Override
    public void scanAll(EventLoop eventLoop, RecordSequenceListener listener,
                        ScanPolicy policy, String namespace, String setName, String... binNames)
            throws AerospikeException {

    }

    @Override
    public void scanNode(ScanPolicy policy, String nodeName, String namespace,
                         String setName, ScanCallback callback, String... binNames)
            throws AerospikeException {

    }

    @Override
    public void scanNode(ScanPolicy policy, Node node, String namespace,
                         String setName, ScanCallback callback, String... binNames)
            throws AerospikeException {

    }

    @Override
    public void scanPartitions(ScanPolicy policy,
                               PartitionFilter partitionFilter,
                               String namespace, String setName, ScanCallback callback,
                               String... binNames) throws AerospikeException {

    }

    @Override
    public void scanPartitions(EventLoop eventLoop,
                               RecordSequenceListener listener, ScanPolicy policy,
                               PartitionFilter partitionFilter, String namespace, String setName,
                               String... binNames) throws AerospikeException {

    }

    @Override
    public RegisterTask register(Policy policy, String clientPath,
                                 String serverPath, Language language) throws AerospikeException {
        return null;
    }

    @Override
    public RegisterTask register(Policy policy, ClassLoader resourceLoader,
                                 String resourcePath, String serverPath, Language language)
            throws AerospikeException {
        return null;
    }

    @Override
    public RegisterTask registerUdfString(Policy policy, String code,
                                          String serverPath, Language language) throws AerospikeException {
        return null;
    }

    @Override
    public void removeUdf(InfoPolicy policy, String serverPath)
            throws AerospikeException {

    }

    @Override
    public Object execute(WritePolicy policy, Key key, String packageName,
                          String functionName, Value... args) throws AerospikeException {
        return null;
    }

    @Override
    public void execute(EventLoop eventLoop, ExecuteListener listener,
                        WritePolicy policy, Key key, String packageName, String functionName,
                        Value... functionArgs) throws AerospikeException {

    }

    @Override
    public BatchResults execute(BatchPolicy batchPolicy,
                                BatchUDFPolicy udfPolicy,
                                Key[] keys, String packageName, String functionName,
                                Value... functionArgs) throws AerospikeException {
        return null;
    }

    @Override
    public void execute(EventLoop eventLoop, BatchRecordArrayListener listener,
                        BatchPolicy batchPolicy, BatchUDFPolicy udfPolicy, Key[] keys,
                        String packageName, String functionName, Value... functionArgs)
            throws AerospikeException {

    }

    @Override
    public void execute(EventLoop eventLoop,
                        BatchRecordSequenceListener listener,
                        BatchPolicy batchPolicy, BatchUDFPolicy udfPolicy, Key[] keys,
                        String packageName, String functionName, Value... functionArgs)
            throws AerospikeException {

    }

    @Override
    public ExecuteTask execute(WritePolicy policy, Statement statement,
                               String packageName, String functionName, Value... functionArgs)
            throws AerospikeException {
        return null;
    }

    @Override
    public ExecuteTask execute(WritePolicy policy, Statement statement,
                               Operation... operations) throws AerospikeException {
        return null;
    }

    @Override
    public RecordSet query(QueryPolicy policy, Statement statement)
            throws AerospikeException {
        return null;
    }

    @Override
    public void query(EventLoop eventLoop, RecordSequenceListener listener,
                      QueryPolicy policy, Statement statement) throws AerospikeException {

    }

    @Override
    public void query(QueryPolicy policy, Statement statement,
                      QueryListener listener) throws AerospikeException {

    }

    @Override
    public void query(QueryPolicy policy, Statement statement,
                      PartitionFilter partitionFilter, QueryListener listener)
            throws AerospikeException {

    }

    @Override
    public RecordSet queryNode(QueryPolicy policy, Statement statement,
                               Node node)
            throws AerospikeException {
        return null;
    }

    @Override
    public RecordSet queryPartitions(QueryPolicy policy, Statement statement,
                                     PartitionFilter partitionFilter) throws AerospikeException {
        return null;
    }

    @Override
    public void queryPartitions(EventLoop eventLoop,
                                RecordSequenceListener listener, QueryPolicy policy,
                                Statement statement,
                                PartitionFilter partitionFilter) throws AerospikeException {

    }

    @Override
    public ResultSet queryAggregate(QueryPolicy policy, Statement statement,
                                    String packageName, String functionName, Value... functionArgs)
            throws AerospikeException {
        return null;
    }

    @Override
    public ResultSet queryAggregate(QueryPolicy policy, Statement statement)
            throws AerospikeException {
        return null;
    }

    @Override
    public ResultSet queryAggregateNode(QueryPolicy policy, Statement statement,
                                        Node node) throws AerospikeException {
        return null;
    }

    @Override
    public IndexTask createIndex(Policy policy, String namespace,
                                 String setName,
                                 String indexName, String binName, IndexType indexType)
            throws AerospikeException {
        return null;
    }

    @Override
    public IndexTask createIndex(Policy policy, String namespace,
                                 String setName,
                                 String indexName, String binName, IndexType indexType,
                                 IndexCollectionType indexCollectionType, CTX... ctx)
            throws AerospikeException {
        return null;
    }

    @Override
    public void createIndex(EventLoop eventLoop, IndexListener listener,
                            Policy policy, String namespace, String setName, String indexName,
                            String binName, IndexType indexType,
                            IndexCollectionType indexCollectionType, CTX... ctx)
            throws AerospikeException {

    }

    @Override
    public IndexTask dropIndex(Policy policy, String namespace, String setName,
                               String indexName) throws AerospikeException {
        return null;
    }

    @Override
    public void dropIndex(EventLoop eventLoop, IndexListener listener,
                          Policy policy, String namespace, String setName, String indexName)
            throws AerospikeException {

    }

    @Override
    public void info(EventLoop eventLoop, InfoListener listener,
                     InfoPolicy policy, Node node, String... commands)
            throws AerospikeException {

    }

    @Override
    public void setXDRFilter(InfoPolicy policy, String datacenter,
                             String namespace, Expression filter) throws AerospikeException {

    }

    @Override
    public void createUser(AdminPolicy policy, String user, String password,
                           List<String> roles) throws AerospikeException {

    }

    @Override
    public void dropUser(AdminPolicy policy, String user)
            throws AerospikeException {

    }

    @Override
    public void changePassword(AdminPolicy policy, String user, String password)
            throws AerospikeException {

    }

    @Override
    public void grantRoles(AdminPolicy policy, String user, List<String> roles)
            throws AerospikeException {

    }

    @Override
    public void revokeRoles(AdminPolicy policy, String user, List<String> roles)
            throws AerospikeException {

    }

    @Override
    public void createRole(AdminPolicy policy, String roleName,
                           List<Privilege> privileges) throws AerospikeException {

    }

    @Override
    public void createRole(AdminPolicy policy, String roleName,
                           List<Privilege> privileges, List<String> whitelist)
            throws AerospikeException {

    }

    @Override
    public void createRole(AdminPolicy policy, String roleName,
                           List<Privilege> privileges, List<String> whitelist, int readQuota,
                           int writeQuota) throws AerospikeException {

    }

    @Override
    public void dropRole(AdminPolicy policy, String roleName)
            throws AerospikeException {

    }

    @Override
    public void grantPrivileges(AdminPolicy policy, String roleName,
                                List<Privilege> privileges) throws AerospikeException {

    }

    @Override
    public void revokePrivileges(AdminPolicy policy, String roleName,
                                 List<Privilege> privileges) throws AerospikeException {

    }

    @Override
    public void setWhitelist(AdminPolicy policy, String roleName,
                             List<String> whitelist) throws AerospikeException {

    }

    @Override
    public void setQuotas(AdminPolicy policy, String roleName, int readQuota,
                          int writeQuota) throws AerospikeException {

    }

    @Override
    public User queryUser(AdminPolicy policy, String user)
            throws AerospikeException {
        return null;
    }

    @Override
    public List<User> queryUsers(AdminPolicy policy) throws AerospikeException {
        return null;
    }

    @Override
    public Role queryRole(AdminPolicy policy, String roleName)
            throws AerospikeException {
        return null;
    }

    @Override
    public List<Role> queryRoles(AdminPolicy policy) throws AerospikeException {
        return null;
    }

}
