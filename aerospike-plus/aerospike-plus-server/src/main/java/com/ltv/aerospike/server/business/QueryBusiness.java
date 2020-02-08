/*
 * Copyright (c) 2019 LINE Corporation. All rights reserved.
 * LINE Corporation PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 */

package com.ltv.aerospike.server.business;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.log4j.Logger;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.Key;
import com.aerospike.client.Record;
import com.aerospike.client.Value;
import com.aerospike.client.listener.RecordListener;
import com.aerospike.client.policy.Policy;
import com.aerospike.helper.query.KeyRecordIterator;
import com.aerospike.helper.query.Qualifier;
import com.google.protobuf.ByteString;
import com.ltv.aerospike.api.proto.GetServices.GetRequest;
import com.ltv.aerospike.api.proto.GetServices.GetResponse;
import com.ltv.aerospike.api.proto.QueryServices.QueryRequest;
import com.ltv.aerospike.api.proto.QueryServices.QueryRequest.FilterOperation;
import com.ltv.aerospike.api.proto.QueryServices.QueryResponse;
import com.ltv.aerospike.api.proto.QueryServices.QueryResponse.Builder;
import com.ltv.aerospike.api.util.ErrorCode;
import com.ltv.aerospike.server.util.AppConstant;

import io.grpc.stub.StreamObserver;

public class QueryBusiness extends BaseBusiness {

    private static Logger log = Logger.getLogger(QueryBusiness.class.getSimpleName());
    public QueryBusiness(StreamObserver responseObserver) {
        super(responseObserver);
    }

    public void query(QueryRequest request) {
        try {
            // get request param
            final String namespace = request.getNamespace();
            final String set = request.getSet();

            // validate null
            if (isNull(responseObserver, namespace,
                       QueryResponse.newBuilder().setErrorCode(ErrorCode.INPUT_REQUIRE.getValue()).build()))
                return;
            if (isNull(responseObserver, set,
                       QueryResponse.newBuilder().setErrorCode(ErrorCode.INPUT_REQUIRE.getValue()).build()))
                return;

            // validate empty
            final Record namespaceRecord = SessionBusiness.namespaces.get(namespace);
            if (isEmpty(responseObserver, namespaceRecord,
                        QueryResponse.newBuilder().setErrorCode(ErrorCode.NOT_EXIST.getValue()).build()))
                return;
            final Record setRecord = SessionBusiness.sets.get(namespaceRecord.getString(AppConstant.KEY) + "_" + set);
            if (isEmpty(responseObserver, setRecord,
                        QueryResponse.newBuilder().setErrorCode(ErrorCode.NOT_EXIST.getValue()).build()))
                return;

            // validate role
            if (!hasPermission(responseObserver, namespaceRecord, setRecord, AppConstant.PERMISSION_SELECT,
                               QueryResponse
                                       .newBuilder().setErrorCode(ErrorCode.PERMISSION_REQUIRE.getValue())
                                       .build())) return;

            Builder builder = QueryResponse.newBuilder();

            HashMap mapValue1 = new HashMap();
            mapValue1.putAll(request.getStringValue1Map());
            mapValue1.putAll(request.getBooleanValue1Map());
            mapValue1.putAll(request.getLongValue1Map());
            mapValue1.putAll(request.getLongValue1Map());
            mapValue1.putAll(request.getFloatValue1Map());
            mapValue1.putAll(request.getDoubleValue1Map());
            mapValue1.putAll(request.getBytesValue1Map());

            HashMap mapValue2 = new HashMap();
            mapValue2.putAll(request.getStringValue2Map());
            mapValue2.putAll(request.getBooleanValue2Map());
            mapValue2.putAll(request.getLongValue2Map());
            mapValue2.putAll(request.getLongValue2Map());
            mapValue2.putAll(request.getFloatValue2Map());
            mapValue2.putAll(request.getDoubleValue2Map());
            mapValue2.putAll(request.getBytesValue2Map());

            List<Qualifier> qualifiers = parseQualifier(request.getFieldsMap(), request.getFilterMap(), mapValue1, mapValue2);
            
            KeyRecordIterator it = queryEngine.select(AEROSPIKE_NAMESPACE, "" + setRecord.getString(AppConstant.KEY), null,
                                                      qualifiers.toArray(new Qualifier[qualifiers.size()]));
            try {
                while (it.hasNext()) {
                    Record record = it.next().record;
                    if(record.getLong(AppConstant.DEL_FLAG) == 1) continue;
                    record.bins.entrySet().stream().forEach(bin -> {
                        if (bin.getValue() instanceof String) {
                            builder.putStringBin(bin.getKey(), (String) bin.getValue());
                        } else if (bin.getValue() instanceof Integer) {
                            builder.putIntBin(bin.getKey(), (int) bin.getValue());
                        } else if (bin.getValue() instanceof Long) {
                            builder.putLongBin(bin.getKey(), (long) bin.getValue());
                        } else if (bin.getValue() instanceof Float) {
                            builder.putFloatBin(bin.getKey(), (float) bin.getValue());
                        } else if (bin.getValue() instanceof Double) {
                            builder.putDoubleBin(bin.getKey(), (double) bin.getValue());
                        } else if (bin.getValue() instanceof Boolean) {
                            builder.putBooleanBin(bin.getKey(), (boolean) bin.getValue());
                        } else if (bin.getValue() instanceof ByteString) {
                            builder.putBytesBin(bin.getKey(), (ByteString) bin.getValue());
                        }
                    });
                    responseObserver.onNext(builder.build());
                }
            } finally {
                it.close();
            }
            responseObserver.onCompleted();
        } catch (Exception ex) {
            responseObserver.onError(ex);
            responseObserver.onCompleted();
        }
    };

    private List<Qualifier> parseQualifier(Map<String, String> field, Map<String, FilterOperation> operation, Map<String, Object> value1, Map<String, Object> value2) {
        return field.entrySet().stream().map(entry -> {
            Qualifier  qualifier = null;
            if(value2.get(entry.getKey()) != null) {
                qualifier = new Qualifier(entry.getValue(), parseFilter(operation.get(entry.getKey())),
                                          Value.get(value1.get(entry.getKey())), Value.get(value2.get(entry.getKey())));
            } else {
                qualifier = new Qualifier(entry.getValue(), parseFilter(operation.get(entry.getKey())),
                                          Value.get(value1.get(entry.getKey())));
            }
            return qualifier;
        }).collect(Collectors.toList());
    }

    private Qualifier.FilterOperation parseFilter(FilterOperation filterOperation) {
        switch (filterOperation.getNumber()) {
            case FilterOperation.EQ_VALUE:
                return Qualifier.FilterOperation.EQ;
            case FilterOperation.GT_VALUE:
                return Qualifier.FilterOperation.GT;
            case FilterOperation.GTEQ_VALUE:
                return Qualifier.FilterOperation.GTEQ;
            case FilterOperation.LT_VALUE:
                return Qualifier.FilterOperation.LT;
            case FilterOperation.LTEQ_VALUE:
                return Qualifier.FilterOperation.LTEQ;
            case FilterOperation.NOTEQ_VALUE:
                return Qualifier.FilterOperation.NOTEQ;
            case FilterOperation.BETWEEN_VALUE:
                return Qualifier.FilterOperation.BETWEEN;
            case FilterOperation.START_WITH_VALUE:
                return Qualifier.FilterOperation.START_WITH;
            case FilterOperation.ENDS_WITH_VALUE:
                return Qualifier.FilterOperation.ENDS_WITH;
            case FilterOperation.CONTAINING_VALUE:
                return Qualifier.FilterOperation.CONTAINING;
            case FilterOperation.IN_VALUE:
                return Qualifier.FilterOperation.IN;
            case FilterOperation.LIST_CONTAINS_VALUE:
                return Qualifier.FilterOperation.LIST_CONTAINS;
            case FilterOperation.MAP_KEYS_CONTAINS_VALUE:
                return Qualifier.FilterOperation.MAP_KEYS_CONTAINS;
            case FilterOperation.MAP_VALUES_CONTAINS_VALUE:
                return Qualifier.FilterOperation.MAP_VALUES_CONTAINS;
            case FilterOperation.LIST_BETWEEN_VALUE:
                return Qualifier.FilterOperation.LIST_BETWEEN;
            case FilterOperation.MAP_KEYS_BETWEEN_VALUE:
                return Qualifier.FilterOperation.MAP_KEYS_BETWEEN;
            case FilterOperation.MAP_VALUES_BETWEEN_VALUE:
                return Qualifier.FilterOperation.MAP_VALUES_BETWEEN;
            case FilterOperation.GEO_WITHIN_VALUE:
                return Qualifier.FilterOperation.GEO_WITHIN;
            case FilterOperation.OR_VALUE:
                return Qualifier.FilterOperation.OR;
            case FilterOperation.AND_VALUE:
                return Qualifier.FilterOperation.AND;
            default:
                return null;
        }
    }

    public void get(GetRequest request) {
        try {
            // get request param
            final String namespace = request.getNamespace();
            final String set = request.getSet();
            final String key = request.getKey();
            final int timeout = request.getTimeout();

            // validate null
            if (isNull(responseObserver, namespace,
                       QueryResponse.newBuilder().setErrorCode(ErrorCode.INPUT_REQUIRE.getValue()).build()))
                return;
            if (isNull(responseObserver, set,
                       QueryResponse.newBuilder().setErrorCode(ErrorCode.INPUT_REQUIRE.getValue()).build()))
                return;

            // validate empty
            final Record namespaceRecord = SessionBusiness.namespaces.get(namespace);
            if (isEmpty(responseObserver, namespaceRecord,
                        QueryResponse.newBuilder().setErrorCode(ErrorCode.NOT_EXIST.getValue()).build()))
                return;
            final Record setRecord = SessionBusiness.sets.get(namespaceRecord.getString(AppConstant.KEY) + "_" + set);
            if (isEmpty(responseObserver, setRecord,
                        QueryResponse.newBuilder().setErrorCode(ErrorCode.NOT_EXIST.getValue()).build()))
                return;

            // validate role
            if (!hasPermission(responseObserver, namespaceRecord, setRecord, AppConstant.PERMISSION_SELECT,
                               QueryResponse
                                       .newBuilder().setErrorCode(ErrorCode.PERMISSION_REQUIRE.getValue())
                                       .build())) return;


            Key recordKey = new Key(AEROSPIKE_NAMESPACE, setRecord.getValue(AppConstant.KEY).toString(), key);
            Policy policy = new Policy();
            if(timeout != 0) policy.setTimeout(timeout);
            aeClient.get(eventLoops.next(), new RecordListener() {
                @Override
                public void onSuccess(Key key, Record record) {
                    GetResponse.Builder builder = GetResponse.newBuilder();
                    if(record == null) {
                        responseObserver.onNext(builder.setErrorCode(ErrorCode.NOT_EXIST.getValue()).build());
                        responseObserver.onCompleted();
                    } else {
                        if (record.getLong(AppConstant.DEL_FLAG) == 1) {
                            responseObserver.onNext(
                                    builder.setErrorCode(ErrorCode.NOT_EXIST.getValue()).build());
                            responseObserver.onCompleted();
                        } else {
                            record.bins.entrySet().stream().forEach(bin -> {
                                if (bin.getValue() instanceof String) {
                                    builder.putStringBin(bin.getKey(), (String) bin.getValue());
                                } else if (bin.getValue() instanceof Integer) {
                                    builder.putIntBin(bin.getKey(), (int) bin.getValue());
                                } else if (bin.getValue() instanceof Long) {
                                    builder.putLongBin(bin.getKey(), (long) bin.getValue());
                                } else if (bin.getValue() instanceof Float) {
                                    builder.putFloatBin(bin.getKey(), (float) bin.getValue());
                                } else if (bin.getValue() instanceof Double) {
                                    builder.putDoubleBin(bin.getKey(), (double) bin.getValue());
                                } else if (bin.getValue() instanceof Boolean) {
                                    builder.putBooleanBin(bin.getKey(), (boolean) bin.getValue());
                                } else if (bin.getValue() instanceof ByteString) {
                                    builder.putBytesBin(bin.getKey(), (ByteString) bin.getValue());
                                }
                            });
                            responseObserver.onNext(builder.setErrorCode(ErrorCode.SUCCESS.getValue()).build());
                            responseObserver.onCompleted();
                        }
                    }
                }

                @Override
                public void onFailure(AerospikeException exception) {
                    responseObserver.onError(exception);
                    responseObserver.onCompleted();
                }
            }, policy, recordKey);
        } catch (Exception ex) {
            responseObserver.onError(ex);
            responseObserver.onCompleted();
        }
    }
}
