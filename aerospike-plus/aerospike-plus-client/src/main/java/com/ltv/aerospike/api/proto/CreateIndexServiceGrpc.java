package com.ltv.aerospike.api.proto;

import static io.grpc.MethodDescriptor.generateFullMethodName;
import static io.grpc.stub.ClientCalls.asyncBidiStreamingCall;
import static io.grpc.stub.ClientCalls.asyncClientStreamingCall;
import static io.grpc.stub.ClientCalls.asyncServerStreamingCall;
import static io.grpc.stub.ClientCalls.asyncUnaryCall;
import static io.grpc.stub.ClientCalls.blockingServerStreamingCall;
import static io.grpc.stub.ClientCalls.blockingUnaryCall;
import static io.grpc.stub.ClientCalls.futureUnaryCall;
import static io.grpc.stub.ServerCalls.asyncBidiStreamingCall;
import static io.grpc.stub.ServerCalls.asyncClientStreamingCall;
import static io.grpc.stub.ServerCalls.asyncServerStreamingCall;
import static io.grpc.stub.ServerCalls.asyncUnaryCall;
import static io.grpc.stub.ServerCalls.asyncUnimplementedStreamingCall;
import static io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall;

/**
 */
@javax.annotation.Generated(
    value = "by gRPC proto compiler (version 1.25.0)",
    comments = "Source: CreateIndexServices.proto")
public final class CreateIndexServiceGrpc {

  private CreateIndexServiceGrpc() {}

  public static final String SERVICE_NAME = "com.ltv.aerospike.api.proto.CreateIndexService";

  // Static method descriptors that strictly reflect the proto.
  private static volatile io.grpc.MethodDescriptor<com.ltv.aerospike.api.proto.CreateIndexServices.CreateIndexRequest,
      com.ltv.aerospike.api.proto.CreateIndexServices.CreateIndexResponse> getCreateIndexMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "createIndex",
      requestType = com.ltv.aerospike.api.proto.CreateIndexServices.CreateIndexRequest.class,
      responseType = com.ltv.aerospike.api.proto.CreateIndexServices.CreateIndexResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.ltv.aerospike.api.proto.CreateIndexServices.CreateIndexRequest,
      com.ltv.aerospike.api.proto.CreateIndexServices.CreateIndexResponse> getCreateIndexMethod() {
    io.grpc.MethodDescriptor<com.ltv.aerospike.api.proto.CreateIndexServices.CreateIndexRequest, com.ltv.aerospike.api.proto.CreateIndexServices.CreateIndexResponse> getCreateIndexMethod;
    if ((getCreateIndexMethod = CreateIndexServiceGrpc.getCreateIndexMethod) == null) {
      synchronized (CreateIndexServiceGrpc.class) {
        if ((getCreateIndexMethod = CreateIndexServiceGrpc.getCreateIndexMethod) == null) {
          CreateIndexServiceGrpc.getCreateIndexMethod = getCreateIndexMethod =
              io.grpc.MethodDescriptor.<com.ltv.aerospike.api.proto.CreateIndexServices.CreateIndexRequest, com.ltv.aerospike.api.proto.CreateIndexServices.CreateIndexResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "createIndex"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.ltv.aerospike.api.proto.CreateIndexServices.CreateIndexRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.ltv.aerospike.api.proto.CreateIndexServices.CreateIndexResponse.getDefaultInstance()))
              .setSchemaDescriptor(new CreateIndexServiceMethodDescriptorSupplier("createIndex"))
              .build();
        }
      }
    }
    return getCreateIndexMethod;
  }

  /**
   * Creates a new async stub that supports all call types for the service
   */
  public static CreateIndexServiceStub newStub(io.grpc.Channel channel) {
    return new CreateIndexServiceStub(channel);
  }

  /**
   * Creates a new blocking-style stub that supports unary and streaming output calls on the service
   */
  public static CreateIndexServiceBlockingStub newBlockingStub(
      io.grpc.Channel channel) {
    return new CreateIndexServiceBlockingStub(channel);
  }

  /**
   * Creates a new ListenableFuture-style stub that supports unary calls on the service
   */
  public static CreateIndexServiceFutureStub newFutureStub(
      io.grpc.Channel channel) {
    return new CreateIndexServiceFutureStub(channel);
  }

  /**
   */
  public static abstract class CreateIndexServiceImplBase implements io.grpc.BindableService {

    /**
     */
    public void createIndex(com.ltv.aerospike.api.proto.CreateIndexServices.CreateIndexRequest request,
        io.grpc.stub.StreamObserver<com.ltv.aerospike.api.proto.CreateIndexServices.CreateIndexResponse> responseObserver) {
      asyncUnimplementedUnaryCall(getCreateIndexMethod(), responseObserver);
    }

    @java.lang.Override public final io.grpc.ServerServiceDefinition bindService() {
      return io.grpc.ServerServiceDefinition.builder(getServiceDescriptor())
          .addMethod(
            getCreateIndexMethod(),
            asyncUnaryCall(
              new MethodHandlers<
                com.ltv.aerospike.api.proto.CreateIndexServices.CreateIndexRequest,
                com.ltv.aerospike.api.proto.CreateIndexServices.CreateIndexResponse>(
                  this, METHODID_CREATE_INDEX)))
          .build();
    }
  }

  /**
   */
  public static final class CreateIndexServiceStub extends io.grpc.stub.AbstractStub<CreateIndexServiceStub> {
    private CreateIndexServiceStub(io.grpc.Channel channel) {
      super(channel);
    }

    private CreateIndexServiceStub(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected CreateIndexServiceStub build(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      return new CreateIndexServiceStub(channel, callOptions);
    }

    /**
     */
    public void createIndex(com.ltv.aerospike.api.proto.CreateIndexServices.CreateIndexRequest request,
        io.grpc.stub.StreamObserver<com.ltv.aerospike.api.proto.CreateIndexServices.CreateIndexResponse> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(getCreateIndexMethod(), getCallOptions()), request, responseObserver);
    }
  }

  /**
   */
  public static final class CreateIndexServiceBlockingStub extends io.grpc.stub.AbstractStub<CreateIndexServiceBlockingStub> {
    private CreateIndexServiceBlockingStub(io.grpc.Channel channel) {
      super(channel);
    }

    private CreateIndexServiceBlockingStub(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected CreateIndexServiceBlockingStub build(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      return new CreateIndexServiceBlockingStub(channel, callOptions);
    }

    /**
     */
    public com.ltv.aerospike.api.proto.CreateIndexServices.CreateIndexResponse createIndex(com.ltv.aerospike.api.proto.CreateIndexServices.CreateIndexRequest request) {
      return blockingUnaryCall(
          getChannel(), getCreateIndexMethod(), getCallOptions(), request);
    }
  }

  /**
   */
  public static final class CreateIndexServiceFutureStub extends io.grpc.stub.AbstractStub<CreateIndexServiceFutureStub> {
    private CreateIndexServiceFutureStub(io.grpc.Channel channel) {
      super(channel);
    }

    private CreateIndexServiceFutureStub(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected CreateIndexServiceFutureStub build(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      return new CreateIndexServiceFutureStub(channel, callOptions);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<com.ltv.aerospike.api.proto.CreateIndexServices.CreateIndexResponse> createIndex(
        com.ltv.aerospike.api.proto.CreateIndexServices.CreateIndexRequest request) {
      return futureUnaryCall(
          getChannel().newCall(getCreateIndexMethod(), getCallOptions()), request);
    }
  }

  private static final int METHODID_CREATE_INDEX = 0;

  private static final class MethodHandlers<Req, Resp> implements
      io.grpc.stub.ServerCalls.UnaryMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.ServerStreamingMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.ClientStreamingMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.BidiStreamingMethod<Req, Resp> {
    private final CreateIndexServiceImplBase serviceImpl;
    private final int methodId;

    MethodHandlers(CreateIndexServiceImplBase serviceImpl, int methodId) {
      this.serviceImpl = serviceImpl;
      this.methodId = methodId;
    }

    @java.lang.Override
    @java.lang.SuppressWarnings("unchecked")
    public void invoke(Req request, io.grpc.stub.StreamObserver<Resp> responseObserver) {
      switch (methodId) {
        case METHODID_CREATE_INDEX:
          serviceImpl.createIndex((com.ltv.aerospike.api.proto.CreateIndexServices.CreateIndexRequest) request,
              (io.grpc.stub.StreamObserver<com.ltv.aerospike.api.proto.CreateIndexServices.CreateIndexResponse>) responseObserver);
          break;
        default:
          throw new AssertionError();
      }
    }

    @java.lang.Override
    @java.lang.SuppressWarnings("unchecked")
    public io.grpc.stub.StreamObserver<Req> invoke(
        io.grpc.stub.StreamObserver<Resp> responseObserver) {
      switch (methodId) {
        default:
          throw new AssertionError();
      }
    }
  }

  private static abstract class CreateIndexServiceBaseDescriptorSupplier
      implements io.grpc.protobuf.ProtoFileDescriptorSupplier, io.grpc.protobuf.ProtoServiceDescriptorSupplier {
    CreateIndexServiceBaseDescriptorSupplier() {}

    @java.lang.Override
    public com.google.protobuf.Descriptors.FileDescriptor getFileDescriptor() {
      return com.ltv.aerospike.api.proto.CreateIndexServices.getDescriptor();
    }

    @java.lang.Override
    public com.google.protobuf.Descriptors.ServiceDescriptor getServiceDescriptor() {
      return getFileDescriptor().findServiceByName("CreateIndexService");
    }
  }

  private static final class CreateIndexServiceFileDescriptorSupplier
      extends CreateIndexServiceBaseDescriptorSupplier {
    CreateIndexServiceFileDescriptorSupplier() {}
  }

  private static final class CreateIndexServiceMethodDescriptorSupplier
      extends CreateIndexServiceBaseDescriptorSupplier
      implements io.grpc.protobuf.ProtoMethodDescriptorSupplier {
    private final String methodName;

    CreateIndexServiceMethodDescriptorSupplier(String methodName) {
      this.methodName = methodName;
    }

    @java.lang.Override
    public com.google.protobuf.Descriptors.MethodDescriptor getMethodDescriptor() {
      return getServiceDescriptor().findMethodByName(methodName);
    }
  }

  private static volatile io.grpc.ServiceDescriptor serviceDescriptor;

  public static io.grpc.ServiceDescriptor getServiceDescriptor() {
    io.grpc.ServiceDescriptor result = serviceDescriptor;
    if (result == null) {
      synchronized (CreateIndexServiceGrpc.class) {
        result = serviceDescriptor;
        if (result == null) {
          serviceDescriptor = result = io.grpc.ServiceDescriptor.newBuilder(SERVICE_NAME)
              .setSchemaDescriptor(new CreateIndexServiceFileDescriptorSupplier())
              .addMethod(getCreateIndexMethod())
              .build();
        }
      }
    }
    return result;
  }
}
