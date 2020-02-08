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
    comments = "Source: DropNamespaceServices.proto")
public final class DropNamespaceServiceGrpc {

  private DropNamespaceServiceGrpc() {}

  public static final String SERVICE_NAME = "com.ltv.aerospike.api.proto.DropNamespaceService";

  // Static method descriptors that strictly reflect the proto.
  private static volatile io.grpc.MethodDescriptor<com.ltv.aerospike.api.proto.DropNamespaceServices.DropNamespaceRequest,
      com.ltv.aerospike.api.proto.DropNamespaceServices.DropNamespaceResponse> getDropNamespaceMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "dropNamespace",
      requestType = com.ltv.aerospike.api.proto.DropNamespaceServices.DropNamespaceRequest.class,
      responseType = com.ltv.aerospike.api.proto.DropNamespaceServices.DropNamespaceResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.ltv.aerospike.api.proto.DropNamespaceServices.DropNamespaceRequest,
      com.ltv.aerospike.api.proto.DropNamespaceServices.DropNamespaceResponse> getDropNamespaceMethod() {
    io.grpc.MethodDescriptor<com.ltv.aerospike.api.proto.DropNamespaceServices.DropNamespaceRequest, com.ltv.aerospike.api.proto.DropNamespaceServices.DropNamespaceResponse> getDropNamespaceMethod;
    if ((getDropNamespaceMethod = DropNamespaceServiceGrpc.getDropNamespaceMethod) == null) {
      synchronized (DropNamespaceServiceGrpc.class) {
        if ((getDropNamespaceMethod = DropNamespaceServiceGrpc.getDropNamespaceMethod) == null) {
          DropNamespaceServiceGrpc.getDropNamespaceMethod = getDropNamespaceMethod =
              io.grpc.MethodDescriptor.<com.ltv.aerospike.api.proto.DropNamespaceServices.DropNamespaceRequest, com.ltv.aerospike.api.proto.DropNamespaceServices.DropNamespaceResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "dropNamespace"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.ltv.aerospike.api.proto.DropNamespaceServices.DropNamespaceRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.ltv.aerospike.api.proto.DropNamespaceServices.DropNamespaceResponse.getDefaultInstance()))
              .setSchemaDescriptor(new DropNamespaceServiceMethodDescriptorSupplier("dropNamespace"))
              .build();
        }
      }
    }
    return getDropNamespaceMethod;
  }

  /**
   * Creates a new async stub that supports all call types for the service
   */
  public static DropNamespaceServiceStub newStub(io.grpc.Channel channel) {
    return new DropNamespaceServiceStub(channel);
  }

  /**
   * Creates a new blocking-style stub that supports unary and streaming output calls on the service
   */
  public static DropNamespaceServiceBlockingStub newBlockingStub(
      io.grpc.Channel channel) {
    return new DropNamespaceServiceBlockingStub(channel);
  }

  /**
   * Creates a new ListenableFuture-style stub that supports unary calls on the service
   */
  public static DropNamespaceServiceFutureStub newFutureStub(
      io.grpc.Channel channel) {
    return new DropNamespaceServiceFutureStub(channel);
  }

  /**
   */
  public static abstract class DropNamespaceServiceImplBase implements io.grpc.BindableService {

    /**
     */
    public void dropNamespace(com.ltv.aerospike.api.proto.DropNamespaceServices.DropNamespaceRequest request,
        io.grpc.stub.StreamObserver<com.ltv.aerospike.api.proto.DropNamespaceServices.DropNamespaceResponse> responseObserver) {
      asyncUnimplementedUnaryCall(getDropNamespaceMethod(), responseObserver);
    }

    @java.lang.Override public final io.grpc.ServerServiceDefinition bindService() {
      return io.grpc.ServerServiceDefinition.builder(getServiceDescriptor())
          .addMethod(
            getDropNamespaceMethod(),
            asyncUnaryCall(
              new MethodHandlers<
                com.ltv.aerospike.api.proto.DropNamespaceServices.DropNamespaceRequest,
                com.ltv.aerospike.api.proto.DropNamespaceServices.DropNamespaceResponse>(
                  this, METHODID_DROP_NAMESPACE)))
          .build();
    }
  }

  /**
   */
  public static final class DropNamespaceServiceStub extends io.grpc.stub.AbstractStub<DropNamespaceServiceStub> {
    private DropNamespaceServiceStub(io.grpc.Channel channel) {
      super(channel);
    }

    private DropNamespaceServiceStub(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected DropNamespaceServiceStub build(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      return new DropNamespaceServiceStub(channel, callOptions);
    }

    /**
     */
    public void dropNamespace(com.ltv.aerospike.api.proto.DropNamespaceServices.DropNamespaceRequest request,
        io.grpc.stub.StreamObserver<com.ltv.aerospike.api.proto.DropNamespaceServices.DropNamespaceResponse> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(getDropNamespaceMethod(), getCallOptions()), request, responseObserver);
    }
  }

  /**
   */
  public static final class DropNamespaceServiceBlockingStub extends io.grpc.stub.AbstractStub<DropNamespaceServiceBlockingStub> {
    private DropNamespaceServiceBlockingStub(io.grpc.Channel channel) {
      super(channel);
    }

    private DropNamespaceServiceBlockingStub(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected DropNamespaceServiceBlockingStub build(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      return new DropNamespaceServiceBlockingStub(channel, callOptions);
    }

    /**
     */
    public com.ltv.aerospike.api.proto.DropNamespaceServices.DropNamespaceResponse dropNamespace(com.ltv.aerospike.api.proto.DropNamespaceServices.DropNamespaceRequest request) {
      return blockingUnaryCall(
          getChannel(), getDropNamespaceMethod(), getCallOptions(), request);
    }
  }

  /**
   */
  public static final class DropNamespaceServiceFutureStub extends io.grpc.stub.AbstractStub<DropNamespaceServiceFutureStub> {
    private DropNamespaceServiceFutureStub(io.grpc.Channel channel) {
      super(channel);
    }

    private DropNamespaceServiceFutureStub(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected DropNamespaceServiceFutureStub build(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      return new DropNamespaceServiceFutureStub(channel, callOptions);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<com.ltv.aerospike.api.proto.DropNamespaceServices.DropNamespaceResponse> dropNamespace(
        com.ltv.aerospike.api.proto.DropNamespaceServices.DropNamespaceRequest request) {
      return futureUnaryCall(
          getChannel().newCall(getDropNamespaceMethod(), getCallOptions()), request);
    }
  }

  private static final int METHODID_DROP_NAMESPACE = 0;

  private static final class MethodHandlers<Req, Resp> implements
      io.grpc.stub.ServerCalls.UnaryMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.ServerStreamingMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.ClientStreamingMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.BidiStreamingMethod<Req, Resp> {
    private final DropNamespaceServiceImplBase serviceImpl;
    private final int methodId;

    MethodHandlers(DropNamespaceServiceImplBase serviceImpl, int methodId) {
      this.serviceImpl = serviceImpl;
      this.methodId = methodId;
    }

    @java.lang.Override
    @java.lang.SuppressWarnings("unchecked")
    public void invoke(Req request, io.grpc.stub.StreamObserver<Resp> responseObserver) {
      switch (methodId) {
        case METHODID_DROP_NAMESPACE:
          serviceImpl.dropNamespace((com.ltv.aerospike.api.proto.DropNamespaceServices.DropNamespaceRequest) request,
              (io.grpc.stub.StreamObserver<com.ltv.aerospike.api.proto.DropNamespaceServices.DropNamespaceResponse>) responseObserver);
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

  private static abstract class DropNamespaceServiceBaseDescriptorSupplier
      implements io.grpc.protobuf.ProtoFileDescriptorSupplier, io.grpc.protobuf.ProtoServiceDescriptorSupplier {
    DropNamespaceServiceBaseDescriptorSupplier() {}

    @java.lang.Override
    public com.google.protobuf.Descriptors.FileDescriptor getFileDescriptor() {
      return com.ltv.aerospike.api.proto.DropNamespaceServices.getDescriptor();
    }

    @java.lang.Override
    public com.google.protobuf.Descriptors.ServiceDescriptor getServiceDescriptor() {
      return getFileDescriptor().findServiceByName("DropNamespaceService");
    }
  }

  private static final class DropNamespaceServiceFileDescriptorSupplier
      extends DropNamespaceServiceBaseDescriptorSupplier {
    DropNamespaceServiceFileDescriptorSupplier() {}
  }

  private static final class DropNamespaceServiceMethodDescriptorSupplier
      extends DropNamespaceServiceBaseDescriptorSupplier
      implements io.grpc.protobuf.ProtoMethodDescriptorSupplier {
    private final String methodName;

    DropNamespaceServiceMethodDescriptorSupplier(String methodName) {
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
      synchronized (DropNamespaceServiceGrpc.class) {
        result = serviceDescriptor;
        if (result == null) {
          serviceDescriptor = result = io.grpc.ServiceDescriptor.newBuilder(SERVICE_NAME)
              .setSchemaDescriptor(new DropNamespaceServiceFileDescriptorSupplier())
              .addMethod(getDropNamespaceMethod())
              .build();
        }
      }
    }
    return result;
  }
}
