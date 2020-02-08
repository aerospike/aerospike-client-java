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
    comments = "Source: GrantSetServices.proto")
public final class GrantSetServiceGrpc {

  private GrantSetServiceGrpc() {}

  public static final String SERVICE_NAME = "com.ltv.aerospike.api.proto.GrantSetService";

  // Static method descriptors that strictly reflect the proto.
  private static volatile io.grpc.MethodDescriptor<com.ltv.aerospike.api.proto.GrantSetServices.GrantSetRequest,
      com.ltv.aerospike.api.proto.GrantSetServices.GrantSetResponse> getGrantSetMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "grantSet",
      requestType = com.ltv.aerospike.api.proto.GrantSetServices.GrantSetRequest.class,
      responseType = com.ltv.aerospike.api.proto.GrantSetServices.GrantSetResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.ltv.aerospike.api.proto.GrantSetServices.GrantSetRequest,
      com.ltv.aerospike.api.proto.GrantSetServices.GrantSetResponse> getGrantSetMethod() {
    io.grpc.MethodDescriptor<com.ltv.aerospike.api.proto.GrantSetServices.GrantSetRequest, com.ltv.aerospike.api.proto.GrantSetServices.GrantSetResponse> getGrantSetMethod;
    if ((getGrantSetMethod = GrantSetServiceGrpc.getGrantSetMethod) == null) {
      synchronized (GrantSetServiceGrpc.class) {
        if ((getGrantSetMethod = GrantSetServiceGrpc.getGrantSetMethod) == null) {
          GrantSetServiceGrpc.getGrantSetMethod = getGrantSetMethod =
              io.grpc.MethodDescriptor.<com.ltv.aerospike.api.proto.GrantSetServices.GrantSetRequest, com.ltv.aerospike.api.proto.GrantSetServices.GrantSetResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "grantSet"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.ltv.aerospike.api.proto.GrantSetServices.GrantSetRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.ltv.aerospike.api.proto.GrantSetServices.GrantSetResponse.getDefaultInstance()))
              .setSchemaDescriptor(new GrantSetServiceMethodDescriptorSupplier("grantSet"))
              .build();
        }
      }
    }
    return getGrantSetMethod;
  }

  /**
   * Creates a new async stub that supports all call types for the service
   */
  public static GrantSetServiceStub newStub(io.grpc.Channel channel) {
    return new GrantSetServiceStub(channel);
  }

  /**
   * Creates a new blocking-style stub that supports unary and streaming output calls on the service
   */
  public static GrantSetServiceBlockingStub newBlockingStub(
      io.grpc.Channel channel) {
    return new GrantSetServiceBlockingStub(channel);
  }

  /**
   * Creates a new ListenableFuture-style stub that supports unary calls on the service
   */
  public static GrantSetServiceFutureStub newFutureStub(
      io.grpc.Channel channel) {
    return new GrantSetServiceFutureStub(channel);
  }

  /**
   */
  public static abstract class GrantSetServiceImplBase implements io.grpc.BindableService {

    /**
     */
    public void grantSet(com.ltv.aerospike.api.proto.GrantSetServices.GrantSetRequest request,
        io.grpc.stub.StreamObserver<com.ltv.aerospike.api.proto.GrantSetServices.GrantSetResponse> responseObserver) {
      asyncUnimplementedUnaryCall(getGrantSetMethod(), responseObserver);
    }

    @java.lang.Override public final io.grpc.ServerServiceDefinition bindService() {
      return io.grpc.ServerServiceDefinition.builder(getServiceDescriptor())
          .addMethod(
            getGrantSetMethod(),
            asyncUnaryCall(
              new MethodHandlers<
                com.ltv.aerospike.api.proto.GrantSetServices.GrantSetRequest,
                com.ltv.aerospike.api.proto.GrantSetServices.GrantSetResponse>(
                  this, METHODID_GRANT_SET)))
          .build();
    }
  }

  /**
   */
  public static final class GrantSetServiceStub extends io.grpc.stub.AbstractStub<GrantSetServiceStub> {
    private GrantSetServiceStub(io.grpc.Channel channel) {
      super(channel);
    }

    private GrantSetServiceStub(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected GrantSetServiceStub build(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      return new GrantSetServiceStub(channel, callOptions);
    }

    /**
     */
    public void grantSet(com.ltv.aerospike.api.proto.GrantSetServices.GrantSetRequest request,
        io.grpc.stub.StreamObserver<com.ltv.aerospike.api.proto.GrantSetServices.GrantSetResponse> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(getGrantSetMethod(), getCallOptions()), request, responseObserver);
    }
  }

  /**
   */
  public static final class GrantSetServiceBlockingStub extends io.grpc.stub.AbstractStub<GrantSetServiceBlockingStub> {
    private GrantSetServiceBlockingStub(io.grpc.Channel channel) {
      super(channel);
    }

    private GrantSetServiceBlockingStub(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected GrantSetServiceBlockingStub build(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      return new GrantSetServiceBlockingStub(channel, callOptions);
    }

    /**
     */
    public com.ltv.aerospike.api.proto.GrantSetServices.GrantSetResponse grantSet(com.ltv.aerospike.api.proto.GrantSetServices.GrantSetRequest request) {
      return blockingUnaryCall(
          getChannel(), getGrantSetMethod(), getCallOptions(), request);
    }
  }

  /**
   */
  public static final class GrantSetServiceFutureStub extends io.grpc.stub.AbstractStub<GrantSetServiceFutureStub> {
    private GrantSetServiceFutureStub(io.grpc.Channel channel) {
      super(channel);
    }

    private GrantSetServiceFutureStub(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected GrantSetServiceFutureStub build(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      return new GrantSetServiceFutureStub(channel, callOptions);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<com.ltv.aerospike.api.proto.GrantSetServices.GrantSetResponse> grantSet(
        com.ltv.aerospike.api.proto.GrantSetServices.GrantSetRequest request) {
      return futureUnaryCall(
          getChannel().newCall(getGrantSetMethod(), getCallOptions()), request);
    }
  }

  private static final int METHODID_GRANT_SET = 0;

  private static final class MethodHandlers<Req, Resp> implements
      io.grpc.stub.ServerCalls.UnaryMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.ServerStreamingMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.ClientStreamingMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.BidiStreamingMethod<Req, Resp> {
    private final GrantSetServiceImplBase serviceImpl;
    private final int methodId;

    MethodHandlers(GrantSetServiceImplBase serviceImpl, int methodId) {
      this.serviceImpl = serviceImpl;
      this.methodId = methodId;
    }

    @java.lang.Override
    @java.lang.SuppressWarnings("unchecked")
    public void invoke(Req request, io.grpc.stub.StreamObserver<Resp> responseObserver) {
      switch (methodId) {
        case METHODID_GRANT_SET:
          serviceImpl.grantSet((com.ltv.aerospike.api.proto.GrantSetServices.GrantSetRequest) request,
              (io.grpc.stub.StreamObserver<com.ltv.aerospike.api.proto.GrantSetServices.GrantSetResponse>) responseObserver);
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

  private static abstract class GrantSetServiceBaseDescriptorSupplier
      implements io.grpc.protobuf.ProtoFileDescriptorSupplier, io.grpc.protobuf.ProtoServiceDescriptorSupplier {
    GrantSetServiceBaseDescriptorSupplier() {}

    @java.lang.Override
    public com.google.protobuf.Descriptors.FileDescriptor getFileDescriptor() {
      return com.ltv.aerospike.api.proto.GrantSetServices.getDescriptor();
    }

    @java.lang.Override
    public com.google.protobuf.Descriptors.ServiceDescriptor getServiceDescriptor() {
      return getFileDescriptor().findServiceByName("GrantSetService");
    }
  }

  private static final class GrantSetServiceFileDescriptorSupplier
      extends GrantSetServiceBaseDescriptorSupplier {
    GrantSetServiceFileDescriptorSupplier() {}
  }

  private static final class GrantSetServiceMethodDescriptorSupplier
      extends GrantSetServiceBaseDescriptorSupplier
      implements io.grpc.protobuf.ProtoMethodDescriptorSupplier {
    private final String methodName;

    GrantSetServiceMethodDescriptorSupplier(String methodName) {
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
      synchronized (GrantSetServiceGrpc.class) {
        result = serviceDescriptor;
        if (result == null) {
          serviceDescriptor = result = io.grpc.ServiceDescriptor.newBuilder(SERVICE_NAME)
              .setSchemaDescriptor(new GrantSetServiceFileDescriptorSupplier())
              .addMethod(getGrantSetMethod())
              .build();
        }
      }
    }
    return result;
  }
}
