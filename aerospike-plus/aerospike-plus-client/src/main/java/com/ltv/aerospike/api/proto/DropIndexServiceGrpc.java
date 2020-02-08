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
    comments = "Source: DropIndexServices.proto")
public final class DropIndexServiceGrpc {

  private DropIndexServiceGrpc() {}

  public static final String SERVICE_NAME = "com.ltv.aerospike.api.proto.DropIndexService";

  // Static method descriptors that strictly reflect the proto.
  private static volatile io.grpc.MethodDescriptor<com.ltv.aerospike.api.proto.DropIndexServices.DropIndexRequest,
      com.ltv.aerospike.api.proto.DropIndexServices.DropIndexResponse> getDropIndexMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "dropIndex",
      requestType = com.ltv.aerospike.api.proto.DropIndexServices.DropIndexRequest.class,
      responseType = com.ltv.aerospike.api.proto.DropIndexServices.DropIndexResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.ltv.aerospike.api.proto.DropIndexServices.DropIndexRequest,
      com.ltv.aerospike.api.proto.DropIndexServices.DropIndexResponse> getDropIndexMethod() {
    io.grpc.MethodDescriptor<com.ltv.aerospike.api.proto.DropIndexServices.DropIndexRequest, com.ltv.aerospike.api.proto.DropIndexServices.DropIndexResponse> getDropIndexMethod;
    if ((getDropIndexMethod = DropIndexServiceGrpc.getDropIndexMethod) == null) {
      synchronized (DropIndexServiceGrpc.class) {
        if ((getDropIndexMethod = DropIndexServiceGrpc.getDropIndexMethod) == null) {
          DropIndexServiceGrpc.getDropIndexMethod = getDropIndexMethod =
              io.grpc.MethodDescriptor.<com.ltv.aerospike.api.proto.DropIndexServices.DropIndexRequest, com.ltv.aerospike.api.proto.DropIndexServices.DropIndexResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "dropIndex"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.ltv.aerospike.api.proto.DropIndexServices.DropIndexRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.ltv.aerospike.api.proto.DropIndexServices.DropIndexResponse.getDefaultInstance()))
              .setSchemaDescriptor(new DropIndexServiceMethodDescriptorSupplier("dropIndex"))
              .build();
        }
      }
    }
    return getDropIndexMethod;
  }

  /**
   * Creates a new async stub that supports all call types for the service
   */
  public static DropIndexServiceStub newStub(io.grpc.Channel channel) {
    return new DropIndexServiceStub(channel);
  }

  /**
   * Creates a new blocking-style stub that supports unary and streaming output calls on the service
   */
  public static DropIndexServiceBlockingStub newBlockingStub(
      io.grpc.Channel channel) {
    return new DropIndexServiceBlockingStub(channel);
  }

  /**
   * Creates a new ListenableFuture-style stub that supports unary calls on the service
   */
  public static DropIndexServiceFutureStub newFutureStub(
      io.grpc.Channel channel) {
    return new DropIndexServiceFutureStub(channel);
  }

  /**
   */
  public static abstract class DropIndexServiceImplBase implements io.grpc.BindableService {

    /**
     */
    public void dropIndex(com.ltv.aerospike.api.proto.DropIndexServices.DropIndexRequest request,
        io.grpc.stub.StreamObserver<com.ltv.aerospike.api.proto.DropIndexServices.DropIndexResponse> responseObserver) {
      asyncUnimplementedUnaryCall(getDropIndexMethod(), responseObserver);
    }

    @java.lang.Override public final io.grpc.ServerServiceDefinition bindService() {
      return io.grpc.ServerServiceDefinition.builder(getServiceDescriptor())
          .addMethod(
            getDropIndexMethod(),
            asyncUnaryCall(
              new MethodHandlers<
                com.ltv.aerospike.api.proto.DropIndexServices.DropIndexRequest,
                com.ltv.aerospike.api.proto.DropIndexServices.DropIndexResponse>(
                  this, METHODID_DROP_INDEX)))
          .build();
    }
  }

  /**
   */
  public static final class DropIndexServiceStub extends io.grpc.stub.AbstractStub<DropIndexServiceStub> {
    private DropIndexServiceStub(io.grpc.Channel channel) {
      super(channel);
    }

    private DropIndexServiceStub(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected DropIndexServiceStub build(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      return new DropIndexServiceStub(channel, callOptions);
    }

    /**
     */
    public void dropIndex(com.ltv.aerospike.api.proto.DropIndexServices.DropIndexRequest request,
        io.grpc.stub.StreamObserver<com.ltv.aerospike.api.proto.DropIndexServices.DropIndexResponse> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(getDropIndexMethod(), getCallOptions()), request, responseObserver);
    }
  }

  /**
   */
  public static final class DropIndexServiceBlockingStub extends io.grpc.stub.AbstractStub<DropIndexServiceBlockingStub> {
    private DropIndexServiceBlockingStub(io.grpc.Channel channel) {
      super(channel);
    }

    private DropIndexServiceBlockingStub(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected DropIndexServiceBlockingStub build(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      return new DropIndexServiceBlockingStub(channel, callOptions);
    }

    /**
     */
    public com.ltv.aerospike.api.proto.DropIndexServices.DropIndexResponse dropIndex(com.ltv.aerospike.api.proto.DropIndexServices.DropIndexRequest request) {
      return blockingUnaryCall(
          getChannel(), getDropIndexMethod(), getCallOptions(), request);
    }
  }

  /**
   */
  public static final class DropIndexServiceFutureStub extends io.grpc.stub.AbstractStub<DropIndexServiceFutureStub> {
    private DropIndexServiceFutureStub(io.grpc.Channel channel) {
      super(channel);
    }

    private DropIndexServiceFutureStub(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected DropIndexServiceFutureStub build(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      return new DropIndexServiceFutureStub(channel, callOptions);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<com.ltv.aerospike.api.proto.DropIndexServices.DropIndexResponse> dropIndex(
        com.ltv.aerospike.api.proto.DropIndexServices.DropIndexRequest request) {
      return futureUnaryCall(
          getChannel().newCall(getDropIndexMethod(), getCallOptions()), request);
    }
  }

  private static final int METHODID_DROP_INDEX = 0;

  private static final class MethodHandlers<Req, Resp> implements
      io.grpc.stub.ServerCalls.UnaryMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.ServerStreamingMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.ClientStreamingMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.BidiStreamingMethod<Req, Resp> {
    private final DropIndexServiceImplBase serviceImpl;
    private final int methodId;

    MethodHandlers(DropIndexServiceImplBase serviceImpl, int methodId) {
      this.serviceImpl = serviceImpl;
      this.methodId = methodId;
    }

    @java.lang.Override
    @java.lang.SuppressWarnings("unchecked")
    public void invoke(Req request, io.grpc.stub.StreamObserver<Resp> responseObserver) {
      switch (methodId) {
        case METHODID_DROP_INDEX:
          serviceImpl.dropIndex((com.ltv.aerospike.api.proto.DropIndexServices.DropIndexRequest) request,
              (io.grpc.stub.StreamObserver<com.ltv.aerospike.api.proto.DropIndexServices.DropIndexResponse>) responseObserver);
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

  private static abstract class DropIndexServiceBaseDescriptorSupplier
      implements io.grpc.protobuf.ProtoFileDescriptorSupplier, io.grpc.protobuf.ProtoServiceDescriptorSupplier {
    DropIndexServiceBaseDescriptorSupplier() {}

    @java.lang.Override
    public com.google.protobuf.Descriptors.FileDescriptor getFileDescriptor() {
      return com.ltv.aerospike.api.proto.DropIndexServices.getDescriptor();
    }

    @java.lang.Override
    public com.google.protobuf.Descriptors.ServiceDescriptor getServiceDescriptor() {
      return getFileDescriptor().findServiceByName("DropIndexService");
    }
  }

  private static final class DropIndexServiceFileDescriptorSupplier
      extends DropIndexServiceBaseDescriptorSupplier {
    DropIndexServiceFileDescriptorSupplier() {}
  }

  private static final class DropIndexServiceMethodDescriptorSupplier
      extends DropIndexServiceBaseDescriptorSupplier
      implements io.grpc.protobuf.ProtoMethodDescriptorSupplier {
    private final String methodName;

    DropIndexServiceMethodDescriptorSupplier(String methodName) {
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
      synchronized (DropIndexServiceGrpc.class) {
        result = serviceDescriptor;
        if (result == null) {
          serviceDescriptor = result = io.grpc.ServiceDescriptor.newBuilder(SERVICE_NAME)
              .setSchemaDescriptor(new DropIndexServiceFileDescriptorSupplier())
              .addMethod(getDropIndexMethod())
              .build();
        }
      }
    }
    return result;
  }
}
