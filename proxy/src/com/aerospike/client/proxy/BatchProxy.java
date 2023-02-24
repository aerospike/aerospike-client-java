package com.aerospike.client.proxy;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.BatchRecord;
import com.aerospike.client.Key;
import com.aerospike.client.Log;
import com.aerospike.client.Operation;
import com.aerospike.client.ResultCode;
import com.aerospike.client.command.BatchAttr;
import com.aerospike.client.command.BatchNode;
import com.aerospike.client.command.Command;
import com.aerospike.client.listener.BatchRecordSequenceListener;
import com.aerospike.client.policy.BatchPolicy;
import com.aerospike.client.proxy.grpc.GrpcCallExecutor;
import com.aerospike.client.util.Util;
import com.aerospike.proxy.client.KVSGrpc;
import com.aerospike.proxy.client.Kvs;
import io.grpc.MethodDescriptor;

/**
 * All batch executors in one class mimicking
 * {@link com.aerospike.client.async.AsyncBatch}.
 */
public class BatchProxy {
    //-------------------------------------------------------
    // OperateRecordSequence
    //-------------------------------------------------------

    // TODO @BrianNichols handle retries.
    public static final class OperateRecordSequenceCommandProxy extends CommandProxy {
        private final BatchRecordSequenceListener listener;
        private final boolean[] sent;
        private final BatchPolicy batchPolicy;
        private final Key[] keys;
        private final Operation[] ops;
        private final BatchAttr attr;
        private final boolean isOperation;

        public OperateRecordSequenceCommandProxy(
                GrpcCallExecutor grpcCallExecutor,
                BatchPolicy batchPolicy,
                Key[] keys,
                Operation[] ops,
                BatchRecordSequenceListener listener,
                BatchAttr attr
        ) {
            super(grpcCallExecutor, batchPolicy);
            this.batchPolicy = batchPolicy;
            this.keys = keys;
            this.ops = ops;
            this.sent = new boolean[keys.length];
            this.listener = listener;
            this.attr = attr;
            this.isOperation = ops != null;
        }

        @Override
        protected MethodDescriptor<Kvs.AerospikeRequestPayload, Kvs.AerospikeResponsePayload> getGrpcMethod() {
            return KVSGrpc.getBatchOperateStreamingMethod();
        }

        @Override
        protected boolean isUnaryCall() {
            return false;
        }

        @Override
        void writeCommand(Command command) {
            // The destination node is a single Aerospike proxy instance,
            // where all keys are sent to the same node. Keys are not
            // distributed across nodes.
            // TODO @BrianNichols can a interface be implemented by both the
            //  native and proxy client, to be passed to setBatchOperate as an
            //  argument? node is passed in as "null" in BatchNode constructor.
            BatchNode batchNode = new BatchNode(null, keys.length, 0);
            for (int i = 1; i < keys.length; i++) {
                batchNode.addKey(i);
            }

            command.setBatchOperate(batchPolicy, keys, batchNode, null, ops, attr);
        }

        @Override
        void onResponse(Kvs.AerospikeResponsePayload response) {
            byte[] bytes = response.getPayload().toByteArray();
            Parser parser = new Parser(bytes, response.getStatus());

            if (response.getHasNext()) {
                parseResult(parser, response.getInDoubt());
                return;
            }

            int resultCode = parser.parseHeader(5);
            if (resultCode == ResultCode.OK) {
                onSuccess();
            } else {
                onFailure(new AerospikeException(resultCode));
            }
        }

        @Override
        void parseResult(Parser parser, Boolean inDoubt) {
            int resultCode = parser.parseHeader(5);
            parser.skipKey();

            Key keyOrig = keys[parser.batchIndex];
            BatchRecord record;

            if (resultCode == 0) {
                record = new BatchRecord(keyOrig, parser.parseRecord(isOperation), attr.hasWrite);
            } else {
                // TODO @BrianNichols commandSentCounter?
                int commandSentCounter = 0;
                record = new BatchRecord(keyOrig, null, resultCode,
                        inDoubt || Command.batchInDoubt(attr.hasWrite,
                                commandSentCounter),
                        attr.hasWrite);
            }
            sent[parser.batchIndex] = true;

            try {
                listener.onRecord(record, parser.batchIndex);
            } catch (Throwable e) {
                Log.error("Unexpected exception from onRecord(): " + Util.getErrorMessage(e));
            }
        }

        @Override
        void onFailure(AerospikeException ae) {
            listener.onFailure(ae);
        }

        @Override
        protected void onSuccess() {
            listener.onSuccess();
        }
    }
}
