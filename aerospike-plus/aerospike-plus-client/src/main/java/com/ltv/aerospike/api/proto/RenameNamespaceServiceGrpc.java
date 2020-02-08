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
    comments = "Source: RenameNamespaceServices.proto")
public final class RenameNamespaceServiceGrpc {

  private RenameNamespaceServiceGrpc() {}

  public static final String SERVICE_NAME = "com.ltv.aerospike.api.proto.RenameNamespaceService";

  // Static method descriptors that strictly reflect the proto.
  private static volatile io.grpc.MethodDescriptor<com.ltv.aerospike.api.proto.RenameNamespaceServices.RenameNamespaceRequest,
      com.ltv.aerospike.api.proto.RenameNamespaceServices.RenameNamespaceResponse> getRenameNamespaceMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "renameNamespace",
      requestType = com.ltv.aerospike.api.proto.RenameNamespaceServices.RenameNamespaceRequest.class,
      responseType = com.ltv.aerospike.api.proto.RenameNamespaceServices.RenameNamespaceResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.ltv.aerospike.api.proto.RenameNamespaceServices.RenameNamespaceRequest,
      com.ltv.aerospike.api.proto.RenameNamespaceServices.RenameNamespaceResponse> getRenameNamespaceMethod() {
    io.grpc.MethodDescriptor<com.ltv.aerospike.api.proto.RenameNamespaceServices.RenameNamespaceRequest, com.ltv.aerospike.api.proto.RenameNamespaceServices.RenameNamespaceResponse> getRenameNamespaceMethod;
    if ((getRenameNamespaceMethod = RenameNamespaceServiceGrpc.getRenameNamespaceMethod) == null) {
      synchronized (RenameNamespaceServiceGrpc.class) {
        if ((getRenameNamespaceMethod = RenameNamespaceServiceGrpc.getRenameNamespaceMethod) == null) {
          RenameNamespaceServiceGrpc.getRenameNamespaceMethod = getRenameNamespaceMethod =
              io.grpc.MethodDescriptor.<com.ltv.aerospike.api.proto.RenameNamespaceServices.RenameNamespaceRequest, com.ltv.aerospike.api.proto.RenameNamespaceServices.RenameNamespaceResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "renameNamespace"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.ltv.aerospike.api.proto.RenameNamespaceServices.RenameNamespaceRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.ltv.aerospike.api.proto.RenameNamespaceServices.RenameNamespaceResponse.getDefaultInstance()))
              .setSchemaDescriptor(new RenameNamespaceServiceMethodDescriptorSupplier("renameNamespace"))
              .build();
        }
      }
    }
    return getRenameNamespaceMethod;
  }

  /**
   * Creates a new async stub that supports all call types for the service
   */
  public static RenameNamespaceServiceStub newStub(io.grpc.Channel channel) {
    return new RenameNamespaceServiceStub(channel);
  }

  /**
   * Creates a new blocking-style stub that supports unary and streaming output calls on the service
   */
  public static RenameNamespaceServiceBlockingStub newBlockingStub(
      io.grpc.Channel channel) {
    return new RenameNamespaceServiceBlockingStub(channel);
  }

  /**
   * Creates a new ListenableFuture-style stub that supports unary calls on the service
   */
  public static RenameNamespaceServiceFutureStub newFutureStub(
      io.grpc.Channel channel) {
    return new RenameNamespaceServiceFutureStub(channel);
  }

  /**
   */
  public static abstract class RenameNamespaceServiceImplBase implements io.grpc.BindableService {

    /**
     */
    public void renameNamespace(com.ltv.aerospike.api.proto.RenameNamespaceServices.RenameNamespaceRequest request,
        io.grpc.stub.StreamObserver<com.ltv.aerospike.api.proto.RenameNamespaceServices.RenameNamespaceResponse> responseObserver) {
      asyncUnimplementedUnaryCall(getRenameNamespaceMethod(), responseObserver);
    }

    @java.lang.Override public final io.grpc.ServerServiceDefinition bindService() {
      return io.grpc.ServerServiceDefinition.builder(getServiceDescriptor())
          .addMethod(
            getRenameNamespaceMethod(),
            asyncUnaryCall(
              new MethodHandlers<
                com.ltv.aerospike.api.proto.RenameNamespaceServices.RenameNamespaceRequest,
                com.ltv.aerospike.api.proto.RenameNamespaceServices.RenameNamespaceResponse>(
                  this, METHODID_RENAME_NAMESPACE)))
          .build();
    }
  }

  /**
   */
  public static final class RenameNamespaceServiceStub extends io.grpc.stub.AbstractStub<RenameNamespaceServiceStub> {
    private RenameNamespaceServiceStub(io.grpc.Channel channel) {
      super(channel);
    }

    private RenameNamespaceServiceStub(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected RenameNamespaceServiceStub build(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      return new RenameNamespaceServiceStub(channel, callOptions);
    }

    /**
     */
    public void renameNamespace(com.ltv.aerospike.api.proto.RenameNamespaceServices.RenameNamespaceRequest request,
        io.grpc.stub.StreamObserver<com.ltv.aerospike.api.proto.RenameNamespaceServices.RenameNamespaceResponse> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(getRenameNamespaceMethod(), getCallOptions()), request, responseObserver);
    }
  }

  /**
   */
  public static final class RenameNamespaceServiceBlockingStub extends io.grpc.stub.AbstractStub<RenameNamespaceServiceBlockingStub> {
    private RenameNamespaceServiceBlockingStub(io.grpc.Channel channel) {
      super(channel);
    }

    private RenameNamespaceServiceBlockingStub(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected RenameNamespaceServiceBlockingStub build(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      return new RenameNamespaceServiceBlockingStub(channel, callOptions);
    }

    /**
     */
    public com.ltv.aerospike.api.proto.RenameNamespaceServices.RenameNamespaceResponse renameNamespace(com.ltv.aerospike.api.proto.RenameNamespaceServices.RenameNamespaceRequest request) {
      return blockingUnaryCall(
          getChannel(), getRenameNamespaceMethod(), getCallOptions(), request);
    }
  }

  /**
   */
  public static final class RenameNamespaceServiceFutureStub extends io.grpc.stub.AbstractStub<RenameNamespaceServiceFutureStub> {
    private RenameNamespaceServiceFutureStub(io.grpc.Channel channel) {
      super(channel);
    }

    private RenameNamespaceServiceFutureStub(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected RenameNamespaceServiceFutureStub build(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      return new RenameNamespaceServiceFutureStub(channel, callOptions);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<com.ltv.aerospike.api.proto.RenameNamespaceServices.RenameNamespaceResponse> renameNamespace(
        com.ltv.aerospike.api.proto.RenameNamespaceServices.RenameNamespaceRequest request) {
      return futureUnaryCall(
          getChannel().newCall(getRenameNamespaceMethod(), getCallOptions()), request);
    }
  }

  private static final int METHODID_RENAME_NAMESPACE = 0;

  private static final class MethodHandlers<Req, Resp> implements
      io.grpc.stub.ServerCalls.UnaryMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.ServerStreamingMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.ClientStreamingMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.BidiStreamingMethod<Req, Resp> {
    private final RenameNamespaceServiceImplBase serviceImpl;
    private final int methodId;

    MethodHandlers(RenameNamespaceServiceImplBase serviceImpl, int methodId) {
      this.serviceImpl = serviceImpl;
      this.methodId = methodId;
    }

    @java.lang.Override
    @java.lang.SuppressWarnings("unchecked")
    public void invoke(Req request, io.grpc.stub.StreamObserver<Resp> responseObserver) {
      switch (methodId) {
        case METHODID_RENAME_NAMESPACE:
          serviceImpl.renameNamespace((com.ltv.aerospike.api.proto.RenameNamespaceServices.RenameNamespaceRequest) request,
              (io.grpc.stub.StreamObserver<com.ltv.aerospike.api.proto.RenameNamespaceServices.RenameNamespaceResponse>) responseObserver);
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

  private static abstract class RenameNamespaceServiceBaseDescriptorSupplier
      implements io.grpc.protobuf.ProtoFileDescriptorSupplier, io.grpc.protobuf.ProtoServiceDescriptorSupplier {
    RenameNamespaceServiceBaseDescriptorSupplier() {}

    @java.lang.Override
    public com.google.protobuf.Descriptors.FileDescriptor getFileDescriptor() {
      return com.ltv.aerospike.api.proto.RenameNamespaceServices.getDescriptor();
    }

    @java.lang.Override
    public com.google.protobuf.Descriptors.ServiceDescriptor getServiceDescriptor() {
      return getFileDescriptor().findServiceByName("RenameNamespaceService");
    }
  }

  private static final class RenameNamespaceServiceFileDescriptorSupplier
      extends RenameNamespaceServiceBaseDescriptorSupplier {
    RenameNamespaceServiceFileDescriptorSupplier() {}
  }

  private static final class RenameNamespaceServiceMethodDescriptorSupplier
      extends RenameNamespaceServiceBaseDescriptorSupplier
      implements io.grpc.protobuf.ProtoMethodDescriptorSupplier {
    private final String methodName;

    RenameNamespaceServiceMethodDescriptorSupplier(String methodName) {
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
      synchronized (RenameNamespaceServiceGrpc.class) {
        result = serviceDescriptor;
        if (result == null) {
          serviceDescriptor = result = io.grpc.ServiceDescriptor.newBuilder(SERVICE_NAME)
              .setSchemaDescriptor(new RenameNamespaceServiceFileDescriptorSupplier())
              .addMethod(getRenameNamespaceMethod())
              .build();
        }
      }
    }
    return result;
  }
}
