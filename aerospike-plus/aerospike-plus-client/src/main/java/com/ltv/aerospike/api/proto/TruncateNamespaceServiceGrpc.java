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
    comments = "Source: TruncateNamespaceServices.proto")
public final class TruncateNamespaceServiceGrpc {

  private TruncateNamespaceServiceGrpc() {}

  public static final String SERVICE_NAME = "com.ltv.aerospike.api.proto.TruncateNamespaceService";

  // Static method descriptors that strictly reflect the proto.
  private static volatile io.grpc.MethodDescriptor<com.ltv.aerospike.api.proto.TruncateNamespaceServices.TruncateNamespaceRequest,
      com.ltv.aerospike.api.proto.TruncateNamespaceServices.TruncateNamespaceResponse> getTruncateNamespaceMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "truncateNamespace",
      requestType = com.ltv.aerospike.api.proto.TruncateNamespaceServices.TruncateNamespaceRequest.class,
      responseType = com.ltv.aerospike.api.proto.TruncateNamespaceServices.TruncateNamespaceResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.ltv.aerospike.api.proto.TruncateNamespaceServices.TruncateNamespaceRequest,
      com.ltv.aerospike.api.proto.TruncateNamespaceServices.TruncateNamespaceResponse> getTruncateNamespaceMethod() {
    io.grpc.MethodDescriptor<com.ltv.aerospike.api.proto.TruncateNamespaceServices.TruncateNamespaceRequest, com.ltv.aerospike.api.proto.TruncateNamespaceServices.TruncateNamespaceResponse> getTruncateNamespaceMethod;
    if ((getTruncateNamespaceMethod = TruncateNamespaceServiceGrpc.getTruncateNamespaceMethod) == null) {
      synchronized (TruncateNamespaceServiceGrpc.class) {
        if ((getTruncateNamespaceMethod = TruncateNamespaceServiceGrpc.getTruncateNamespaceMethod) == null) {
          TruncateNamespaceServiceGrpc.getTruncateNamespaceMethod = getTruncateNamespaceMethod =
              io.grpc.MethodDescriptor.<com.ltv.aerospike.api.proto.TruncateNamespaceServices.TruncateNamespaceRequest, com.ltv.aerospike.api.proto.TruncateNamespaceServices.TruncateNamespaceResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "truncateNamespace"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.ltv.aerospike.api.proto.TruncateNamespaceServices.TruncateNamespaceRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.ltv.aerospike.api.proto.TruncateNamespaceServices.TruncateNamespaceResponse.getDefaultInstance()))
              .setSchemaDescriptor(new TruncateNamespaceServiceMethodDescriptorSupplier("truncateNamespace"))
              .build();
        }
      }
    }
    return getTruncateNamespaceMethod;
  }

  /**
   * Creates a new async stub that supports all call types for the service
   */
  public static TruncateNamespaceServiceStub newStub(io.grpc.Channel channel) {
    return new TruncateNamespaceServiceStub(channel);
  }

  /**
   * Creates a new blocking-style stub that supports unary and streaming output calls on the service
   */
  public static TruncateNamespaceServiceBlockingStub newBlockingStub(
      io.grpc.Channel channel) {
    return new TruncateNamespaceServiceBlockingStub(channel);
  }

  /**
   * Creates a new ListenableFuture-style stub that supports unary calls on the service
   */
  public static TruncateNamespaceServiceFutureStub newFutureStub(
      io.grpc.Channel channel) {
    return new TruncateNamespaceServiceFutureStub(channel);
  }

  /**
   */
  public static abstract class TruncateNamespaceServiceImplBase implements io.grpc.BindableService {

    /**
     */
    public void truncateNamespace(com.ltv.aerospike.api.proto.TruncateNamespaceServices.TruncateNamespaceRequest request,
        io.grpc.stub.StreamObserver<com.ltv.aerospike.api.proto.TruncateNamespaceServices.TruncateNamespaceResponse> responseObserver) {
      asyncUnimplementedUnaryCall(getTruncateNamespaceMethod(), responseObserver);
    }

    @java.lang.Override public final io.grpc.ServerServiceDefinition bindService() {
      return io.grpc.ServerServiceDefinition.builder(getServiceDescriptor())
          .addMethod(
            getTruncateNamespaceMethod(),
            asyncUnaryCall(
              new MethodHandlers<
                com.ltv.aerospike.api.proto.TruncateNamespaceServices.TruncateNamespaceRequest,
                com.ltv.aerospike.api.proto.TruncateNamespaceServices.TruncateNamespaceResponse>(
                  this, METHODID_TRUNCATE_NAMESPACE)))
          .build();
    }
  }

  /**
   */
  public static final class TruncateNamespaceServiceStub extends io.grpc.stub.AbstractStub<TruncateNamespaceServiceStub> {
    private TruncateNamespaceServiceStub(io.grpc.Channel channel) {
      super(channel);
    }

    private TruncateNamespaceServiceStub(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected TruncateNamespaceServiceStub build(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      return new TruncateNamespaceServiceStub(channel, callOptions);
    }

    /**
     */
    public void truncateNamespace(com.ltv.aerospike.api.proto.TruncateNamespaceServices.TruncateNamespaceRequest request,
        io.grpc.stub.StreamObserver<com.ltv.aerospike.api.proto.TruncateNamespaceServices.TruncateNamespaceResponse> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(getTruncateNamespaceMethod(), getCallOptions()), request, responseObserver);
    }
  }

  /**
   */
  public static final class TruncateNamespaceServiceBlockingStub extends io.grpc.stub.AbstractStub<TruncateNamespaceServiceBlockingStub> {
    private TruncateNamespaceServiceBlockingStub(io.grpc.Channel channel) {
      super(channel);
    }

    private TruncateNamespaceServiceBlockingStub(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected TruncateNamespaceServiceBlockingStub build(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      return new TruncateNamespaceServiceBlockingStub(channel, callOptions);
    }

    /**
     */
    public com.ltv.aerospike.api.proto.TruncateNamespaceServices.TruncateNamespaceResponse truncateNamespace(com.ltv.aerospike.api.proto.TruncateNamespaceServices.TruncateNamespaceRequest request) {
      return blockingUnaryCall(
          getChannel(), getTruncateNamespaceMethod(), getCallOptions(), request);
    }
  }

  /**
   */
  public static final class TruncateNamespaceServiceFutureStub extends io.grpc.stub.AbstractStub<TruncateNamespaceServiceFutureStub> {
    private TruncateNamespaceServiceFutureStub(io.grpc.Channel channel) {
      super(channel);
    }

    private TruncateNamespaceServiceFutureStub(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected TruncateNamespaceServiceFutureStub build(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      return new TruncateNamespaceServiceFutureStub(channel, callOptions);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<com.ltv.aerospike.api.proto.TruncateNamespaceServices.TruncateNamespaceResponse> truncateNamespace(
        com.ltv.aerospike.api.proto.TruncateNamespaceServices.TruncateNamespaceRequest request) {
      return futureUnaryCall(
          getChannel().newCall(getTruncateNamespaceMethod(), getCallOptions()), request);
    }
  }

  private static final int METHODID_TRUNCATE_NAMESPACE = 0;

  private static final class MethodHandlers<Req, Resp> implements
      io.grpc.stub.ServerCalls.UnaryMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.ServerStreamingMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.ClientStreamingMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.BidiStreamingMethod<Req, Resp> {
    private final TruncateNamespaceServiceImplBase serviceImpl;
    private final int methodId;

    MethodHandlers(TruncateNamespaceServiceImplBase serviceImpl, int methodId) {
      this.serviceImpl = serviceImpl;
      this.methodId = methodId;
    }

    @java.lang.Override
    @java.lang.SuppressWarnings("unchecked")
    public void invoke(Req request, io.grpc.stub.StreamObserver<Resp> responseObserver) {
      switch (methodId) {
        case METHODID_TRUNCATE_NAMESPACE:
          serviceImpl.truncateNamespace((com.ltv.aerospike.api.proto.TruncateNamespaceServices.TruncateNamespaceRequest) request,
              (io.grpc.stub.StreamObserver<com.ltv.aerospike.api.proto.TruncateNamespaceServices.TruncateNamespaceResponse>) responseObserver);
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

  private static abstract class TruncateNamespaceServiceBaseDescriptorSupplier
      implements io.grpc.protobuf.ProtoFileDescriptorSupplier, io.grpc.protobuf.ProtoServiceDescriptorSupplier {
    TruncateNamespaceServiceBaseDescriptorSupplier() {}

    @java.lang.Override
    public com.google.protobuf.Descriptors.FileDescriptor getFileDescriptor() {
      return com.ltv.aerospike.api.proto.TruncateNamespaceServices.getDescriptor();
    }

    @java.lang.Override
    public com.google.protobuf.Descriptors.ServiceDescriptor getServiceDescriptor() {
      return getFileDescriptor().findServiceByName("TruncateNamespaceService");
    }
  }

  private static final class TruncateNamespaceServiceFileDescriptorSupplier
      extends TruncateNamespaceServiceBaseDescriptorSupplier {
    TruncateNamespaceServiceFileDescriptorSupplier() {}
  }

  private static final class TruncateNamespaceServiceMethodDescriptorSupplier
      extends TruncateNamespaceServiceBaseDescriptorSupplier
      implements io.grpc.protobuf.ProtoMethodDescriptorSupplier {
    private final String methodName;

    TruncateNamespaceServiceMethodDescriptorSupplier(String methodName) {
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
      synchronized (TruncateNamespaceServiceGrpc.class) {
        result = serviceDescriptor;
        if (result == null) {
          serviceDescriptor = result = io.grpc.ServiceDescriptor.newBuilder(SERVICE_NAME)
              .setSchemaDescriptor(new TruncateNamespaceServiceFileDescriptorSupplier())
              .addMethod(getTruncateNamespaceMethod())
              .build();
        }
      }
    }
    return result;
  }
}
