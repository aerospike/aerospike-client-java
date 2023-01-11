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

import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.Map;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.Bin;
import com.aerospike.client.Key;
import com.aerospike.client.Operation;
import com.aerospike.client.Value;
import com.aerospike.client.command.Buffer;
import com.aerospike.client.command.Command;
import com.aerospike.client.command.FieldType;
import com.aerospike.client.policy.Policy;
import com.aerospike.client.policy.WritePolicy;
import com.aerospike.client.query.BVal;

import io.netty.buffer.ByteBuf;

/**
 * Serialization and deserialization of Aerospike format wire data.
 */
public class Serde extends Command {
    // FIXME: convert all parseXXX method arguments from byte[] to InputStream.
    // TODO: Don't subclass Command, copy all Aerospike wire payload logic here.

    public static int PROTO_HEADER_SIZE = 8;
    public static int MESSAGE_HEADER_SIZE = 22;

    private static final Policy DUMMY_POLICY = new Policy();

    public Serde() {
        super(DUMMY_POLICY.socketTimeout, DUMMY_POLICY.totalTimeout,
                DUMMY_POLICY.maxRetries);
    }

    /**
     * Parse the protocol header from the input stream.
     *
     * @param byteBuf Aerospike wire format data
     * @return the protocol header information.
     * @throws AerospikeException.Parse on protocol header parsing failure.
     */
    public ProtoHeader parseProtoHeader(ByteBuf byteBuf) throws AerospikeException.Parse {
        byte[] header = new byte[PROTO_HEADER_SIZE];
        byteBuf.readBytes(header);
        long proto = Buffer.bytesToLong(header, 0);

        long type = (proto >> 48) & 0xFF;
        int size = (int) (proto & 0xFFFFFFFFFFFFL);

        // TODO: parse version?
        return new ProtoHeader(2, type, size);
    }

    public ProtoMessageHeader parseMessageHeader(ByteBuf byteBuf) throws AerospikeException.Parse {
        byte[] header = new byte[MESSAGE_HEADER_SIZE];
        byteBuf.readBytes(header);

        int size = header[0];
        int info1 = header[1];
        int info2 = header[2];
        int info3 = header[3];
        int resultCode = header[5] & 0xFF;
        int generation = Buffer.bytesToInt(header, 6);
        int expiration = Buffer.bytesToInt(header, 10);
        int fieldCount = Buffer.bytesToShort(header, 18);
        int opCount = Buffer.bytesToShort(header, 20);
        return new ProtoMessageHeader(size, info1, info2, info3, resultCode,
                generation, expiration, fieldCount, opCount);
    }

    public ProtoKey parseKey(ByteBuf byteBuf, int numFields) throws AerospikeException.Parse {
        byte[] digest = null;
        String namespace = null;
        String setName = null;
        Value userKey = null;
        BVal bval = null;

        for (int i = 0; i < numFields; i++) {
            int fieldLength = Buffer.bytesToInt(toArray(byteBuf, 4), 0);

            int fieldType = byteBuf.readByte();
            int size = fieldLength - 1;
            byte[] field = new byte[size];
            byteBuf.readBytes(field);

            switch (fieldType) {
                case FieldType.DIGEST_RIPE:
                    digest = field;
                    break;

                case FieldType.NAMESPACE:
                    namespace = Buffer.utf8ToString(field, 0, size);
                    break;

                case FieldType.TABLE:
                    setName = Buffer.utf8ToString(field, 0, size);
                    break;

                case FieldType.KEY:
                    int type = field[0];
                    userKey = Buffer.bytesToKeyValue(type, field, 1, size - 1);
                    break;

                case FieldType.BVAL_ARRAY:
                    bval = new BVal();
                    bval.val = Buffer.littleBytesToLong(field, 0);
                    break;
            }
        }
        return new ProtoKey(new Key(namespace, digest, setName, userKey), bval);
    }

    private byte[] toArray(ByteBuf byteBuf, int size) {
        byte[] bytes = new byte[size];
        byteBuf.readBytes(bytes);
        return bytes;
    }

    public void skipKey(ByteBuf byteBuf, int numFields) throws AerospikeException.Parse {
        for (int i = 0; i < numFields; i++) {
            int fieldLen = Buffer.bytesToInt(toArray(byteBuf, 4), 0);
            byteBuf.skipBytes(fieldLen);
        }
    }

    public Map<String, Object> parseBins(ByteBuf byteBuf, int numBins,
                                         boolean isOperation) throws AerospikeException.Parse {
        Map<String, Object> bins = new LinkedHashMap<>();

        for (int i = 0; i < numBins; i++) {
            int opSize = Buffer.bytesToInt(toArray(byteBuf, 4), 0);
            byteBuf.skipBytes(1); // Skip operation to apply.
            byte particleType = byteBuf.readByte();
            byteBuf.skipBytes(1); // Unused.
            byte nameSize = byteBuf.readByte();
            String name = Buffer.utf8ToString(toArray(byteBuf, nameSize),
                    0, nameSize);

            int particleSize = opSize - (4 + nameSize);
            byte[] particle = toArray(byteBuf, particleSize);
            Object value = Buffer.bytesToParticle(particleType, particle, 0,
                    particleSize);

            if (isOperation) {
                if (bins.containsKey(name)) {
                    // Multiple values returned for the same bin.
                    Object prev = bins.get(name);

                    if (prev instanceof OpResults) {
                        // List already exists.  Add to it.
                        OpResults list = (OpResults) prev;
                        list.add(value);
                    } else {
                        // Make a list to store all values.
                        OpResults list = new OpResults();
                        list.add(prev);
                        list.add(value);
                        bins.put(name, list);
                    }
                } else {
                    bins.put(name, value);
                }
            } else {
                bins.put(name, value);
            }
        }
        return bins;
    }

    private static class OpResults extends ArrayList<Object> {
        private static final long serialVersionUID = 1L;
    }

    /**
     * Write the "get" operation payload to the output stream.
     *
     * @param output   Aerospike wire format payload is written to this stream.
     * @param policy   read policy.
     * @param key      the Aerospike key to identify the record.
     * @param binNames only these bins are fetched from the Aerospike server.
     *                 Use <code>null</code> to fetch all bins.
     */
    public void writeGetPayload(OutputStream output, Policy policy, Key key,
                                String[] binNames) throws IOException {
        // TODO: Copy all Aerospike wire payload logic here.
        setRead(policy, key, binNames);
        output.write(dataBuffer, 0, dataOffset);
    }

    /**
     * Write the "put" operation payload to the output stream.
     *
     * @param output Aerospike wire format payload is written to this stream.
     * @param policy write policy.
     * @param key    the Aerospike key to identify the record.
     * @param bins   bins to update in the record.
     */
    public void writePutPayload(OutputStream output, WritePolicy policy,
                                Key key, Bin[] bins) throws IOException {
        // TODO: Copy all Aerospike wire payload logic here.
        setWrite(policy, Operation.Type.WRITE, key, bins);
        output.write(dataBuffer, 0, dataOffset);
    }

    /**
     * Write the error response payload to the output stream.
     *
     * @param output Aerospike wire format payload is written to this stream.
     * @param key    the Aerospike key to identify the record.
     */
    public void writeErrorResponse(OutputStream output, Key key,
                                   int resultCode) throws IOException {
        // TODO: Copy all Aerospike wire payload logic here.
        begin();
        int fieldCount = estimateKeySize(key);

        sizeBuffer();
        // Write all header data except total size which must be written last.
        dataBuffer[8] = MSG_REMAINING_HEADER_SIZE; // Message header length.
        for (int i = 9; i < 22; i++) {
            dataBuffer[i] = 0;
        }
        dataBuffer[13] = (byte) resultCode;

        // Timeout
        Buffer.intToBytes(0, dataBuffer, 22);
        // Field Count
        Buffer.shortToBytes(fieldCount, dataBuffer, 26);

        // Operation Count
        Buffer.shortToBytes(0, dataBuffer, 28);

        dataOffset = MSG_TOTAL_HEADER_SIZE;
        writeKey(key);

        end();
        output.write(dataBuffer, 0, dataOffset);
    }

    // FIXME: Do not subclass Command, remove this method.
    @Override
    protected void sizeBuffer() {
        dataBuffer = new byte[dataOffset];
    }


    /**
     * The header preceding all Aerospike wire format messages.
     */
    public static class ProtoHeader {
        /**
         * Protocol version. Current version = 2.
         */
        public final int version;
        /**
         * Message type: 1 = Info, 3 = Message, 4 = Compressed Message.
         */
        public final long type;
        /**
         * Number of bytes in the Aerospike message to follow this Protocol
         * Header.
         */
        public final long size;
        /**
         * Is the Aerospike message compressed?
         */
        public final boolean isCompressed;

        public ProtoHeader(int version, long type, int size) {
            this.version = version;
            this.type = type;
            this.size = size;
            this.isCompressed = type == Command.MSG_TYPE_COMPRESSED;
        }
    }


    /**
     * The header for an Aerospike Message type (type = 3).
     */
    public static class ProtoMessageHeader {
        /**
         * Number of bytes in this Protocol Message Header, always equal to 22.
         */
        public final int size;
        /**
         * Info flags.
         */
        public final int info1;
        public final int info2;
        public final int info3;
        /**
         * Request result code.
         */
        public final int resultCode;
        /**
         * Record modification count.
         */
        public final int generation;
        /**
         * Date record will expire, in seconds from Jan 01 2010 00:00:00 GMT
         */
        public final int expiration;
        /**
         * Number of fields to follow in the data payload. These fields
         * precede the operations in the payload. For the data received
         * by client the fields hold the Aerospike key (namespace, set, user
         * key, digest).
         */
        public final int numFields;
        /**
         * Number of operations to follow in the data payload. For the data
         * received by the client the operations hold the Record bins.
         */
        public final int numOperations;

        public ProtoMessageHeader(int size, int info1, int info2, int info3,
                                  int resultCode, int generation,
                                  int expiration, int numFields,
                                  int numOperations) {
            this.size = size;
            this.info1 = info1;
            this.info2 = info2;
            this.info3 = info3;
            this.resultCode = resultCode;
            this.generation = generation;
            this.expiration = expiration;
            this.numFields = numFields;
            this.numOperations = numOperations;
        }
    }

    /**
     * The Aerospike key.
     */
    public static class ProtoKey {
        public final Key key;
        /**
         * An opaque value useful in pause/resume of queries.
         * Can be <code>null</code>.
         */

        public final BVal bval;

        public ProtoKey(Key key, BVal bval) {
            this.key = key;
            this.bval = bval;
        }
    }
}
