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
    comments = "Source: GrantNamespaceServices.proto")
public final class GrantNamespaceServiceGrpc {

  private GrantNamespaceServiceGrpc() {}

  public static final String SERVICE_NAME = "com.ltv.aerospike.api.proto.GrantNamespaceService";

  // Static method descriptors that strictly reflect the proto.
  private static volatile io.grpc.MethodDescriptor<com.ltv.aerospike.api.proto.GrantNamespaceServices.GrantNamespaceRequest,
      com.ltv.aerospike.api.proto.GrantNamespaceServices.GrantNamespaceResponse> getGrantNamespaceMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "grantNamespace",
      requestType = com.ltv.aerospike.api.proto.GrantNamespaceServices.GrantNamespaceRequest.class,
      responseType = com.ltv.aerospike.api.proto.GrantNamespaceServices.GrantNamespaceResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.ltv.aerospike.api.proto.GrantNamespaceServices.GrantNamespaceRequest,
      com.ltv.aerospike.api.proto.GrantNamespaceServices.GrantNamespaceResponse> getGrantNamespaceMethod() {
    io.grpc.MethodDescriptor<com.ltv.aerospike.api.proto.GrantNamespaceServices.GrantNamespaceRequest, com.ltv.aerospike.api.proto.GrantNamespaceServices.GrantNamespaceResponse> getGrantNamespaceMethod;
    if ((getGrantNamespaceMethod = GrantNamespaceServiceGrpc.getGrantNamespaceMethod) == null) {
      synchronized (GrantNamespaceServiceGrpc.class) {
        if ((getGrantNamespaceMethod = GrantNamespaceServiceGrpc.getGrantNamespaceMethod) == null) {
          GrantNamespaceServiceGrpc.getGrantNamespaceMethod = getGrantNamespaceMethod =
              io.grpc.MethodDescriptor.<com.ltv.aerospike.api.proto.GrantNamespaceServices.GrantNamespaceRequest, com.ltv.aerospike.api.proto.GrantNamespaceServices.GrantNamespaceResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "grantNamespace"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.ltv.aerospike.api.proto.GrantNamespaceServices.GrantNamespaceRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.ltv.aerospike.api.proto.GrantNamespaceServices.GrantNamespaceResponse.getDefaultInstance()))
              .setSchemaDescriptor(new GrantNamespaceServiceMethodDescriptorSupplier("grantNamespace"))
              .build();
        }
      }
    }
    return getGrantNamespaceMethod;
  }

  /**
   * Creates a new async stub that supports all call types for the service
   */
  public static GrantNamespaceServiceStub newStub(io.grpc.Channel channel) {
    return new GrantNamespaceServiceStub(channel);
  }

  /**
   * Creates a new blocking-style stub that supports unary and streaming output calls on the service
   */
  public static GrantNamespaceServiceBlockingStub newBlockingStub(
      io.grpc.Channel channel) {
    return new GrantNamespaceServiceBlockingStub(channel);
  }

  /**
   * Creates a new ListenableFuture-style stub that supports unary calls on the service
   */
  public static GrantNamespaceServiceFutureStub newFutureStub(
      io.grpc.Channel channel) {
    return new GrantNamespaceServiceFutureStub(channel);
  }

  /**
   */
  public static abstract class GrantNamespaceServiceImplBase implements io.grpc.BindableService {

    /**
     */
    public void grantNamespace(com.ltv.aerospike.api.proto.GrantNamespaceServices.GrantNamespaceRequest request,
        io.grpc.stub.StreamObserver<com.ltv.aerospike.api.proto.GrantNamespaceServices.GrantNamespaceResponse> responseObserver) {
      asyncUnimplementedUnaryCall(getGrantNamespaceMethod(), responseObserver);
    }

    @java.lang.Override public final io.grpc.ServerServiceDefinition bindService() {
      return io.grpc.ServerServiceDefinition.builder(getServiceDescriptor())
          .addMethod(
            getGrantNamespaceMethod(),
            asyncUnaryCall(
              new MethodHandlers<
                com.ltv.aerospike.api.proto.GrantNamespaceServices.GrantNamespaceRequest,
                com.ltv.aerospike.api.proto.GrantNamespaceServices.GrantNamespaceResponse>(
                  this, METHODID_GRANT_NAMESPACE)))
          .build();
    }
  }

  /**
   */
  public static final class GrantNamespaceServiceStub extends io.grpc.stub.AbstractStub<GrantNamespaceServiceStub> {
    private GrantNamespaceServiceStub(io.grpc.Channel channel) {
      super(channel);
    }

    private GrantNamespaceServiceStub(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected GrantNamespaceServiceStub build(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      return new GrantNamespaceServiceStub(channel, callOptions);
    }

    /**
     */
    public void grantNamespace(com.ltv.aerospike.api.proto.GrantNamespaceServices.GrantNamespaceRequest request,
        io.grpc.stub.StreamObserver<com.ltv.aerospike.api.proto.GrantNamespaceServices.GrantNamespaceResponse> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(getGrantNamespaceMethod(), getCallOptions()), request, responseObserver);
    }
  }

  /**
   */
  public static final class GrantNamespaceServiceBlockingStub extends io.grpc.stub.AbstractStub<GrantNamespaceServiceBlockingStub> {
    private GrantNamespaceServiceBlockingStub(io.grpc.Channel channel) {
      super(channel);
    }

    private GrantNamespaceServiceBlockingStub(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected GrantNamespaceServiceBlockingStub build(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      return new GrantNamespaceServiceBlockingStub(channel, callOptions);
    }

    /**
     */
    public com.ltv.aerospike.api.proto.GrantNamespaceServices.GrantNamespaceResponse grantNamespace(com.ltv.aerospike.api.proto.GrantNamespaceServices.GrantNamespaceRequest request) {
      return blockingUnaryCall(
          getChannel(), getGrantNamespaceMethod(), getCallOptions(), request);
    }
  }

  /**
   */
  public static final class GrantNamespaceServiceFutureStub extends io.grpc.stub.AbstractStub<GrantNamespaceServiceFutureStub> {
    private GrantNamespaceServiceFutureStub(io.grpc.Channel channel) {
      super(channel);
    }

    private GrantNamespaceServiceFutureStub(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected GrantNamespaceServiceFutureStub build(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      return new GrantNamespaceServiceFutureStub(channel, callOptions);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<com.ltv.aerospike.api.proto.GrantNamespaceServices.GrantNamespaceResponse> grantNamespace(
        com.ltv.aerospike.api.proto.GrantNamespaceServices.GrantNamespaceRequest request) {
      return futureUnaryCall(
          getChannel().newCall(getGrantNamespaceMethod(), getCallOptions()), request);
    }
  }

  private static final int METHODID_GRANT_NAMESPACE = 0;

  private static final class MethodHandlers<Req, Resp> implements
      io.grpc.stub.ServerCalls.UnaryMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.ServerStreamingMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.ClientStreamingMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.BidiStreamingMethod<Req, Resp> {
    private final GrantNamespaceServiceImplBase serviceImpl;
    private final int methodId;

    MethodHandlers(GrantNamespaceServiceImplBase serviceImpl, int methodId) {
      this.serviceImpl = serviceImpl;
      this.methodId = methodId;
    }

    @java.lang.Override
    @java.lang.SuppressWarnings("unchecked")
    public void invoke(Req request, io.grpc.stub.StreamObserver<Resp> responseObserver) {
      switch (methodId) {
        case METHODID_GRANT_NAMESPACE:
          serviceImpl.grantNamespace((com.ltv.aerospike.api.proto.GrantNamespaceServices.GrantNamespaceRequest) request,
              (io.grpc.stub.StreamObserver<com.ltv.aerospike.api.proto.GrantNamespaceServices.GrantNamespaceResponse>) responseObserver);
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

  private static abstract class GrantNamespaceServiceBaseDescriptorSupplier
      implements io.grpc.protobuf.ProtoFileDescriptorSupplier, io.grpc.protobuf.ProtoServiceDescriptorSupplier {
    GrantNamespaceServiceBaseDescriptorSupplier() {}

    @java.lang.Override
    public com.google.protobuf.Descriptors.FileDescriptor getFileDescriptor() {
      return com.ltv.aerospike.api.proto.GrantNamespaceServices.getDescriptor();
    }

    @java.lang.Override
    public com.google.protobuf.Descriptors.ServiceDescriptor getServiceDescriptor() {
      return getFileDescriptor().findServiceByName("GrantNamespaceService");
    }
  }

  private static final class GrantNamespaceServiceFileDescriptorSupplier
      extends GrantNamespaceServiceBaseDescriptorSupplier {
    GrantNamespaceServiceFileDescriptorSupplier() {}
  }

  private static final class GrantNamespaceServiceMethodDescriptorSupplier
      extends GrantNamespaceServiceBaseDescriptorSupplier
      implements io.grpc.protobuf.ProtoMethodDescriptorSupplier {
    private final String methodName;

    GrantNamespaceServiceMethodDescriptorSupplier(String methodName) {
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
      synchronized (GrantNamespaceServiceGrpc.class) {
        result = serviceDescriptor;
        if (result == null) {
          serviceDescriptor = result = io.grpc.ServiceDescriptor.newBuilder(SERVICE_NAME)
              .setSchemaDescriptor(new GrantNamespaceServiceFileDescriptorSupplier())
              .addMethod(getGrantNamespaceMethod())
              .build();
        }
      }
    }
    return result;
  }
}
