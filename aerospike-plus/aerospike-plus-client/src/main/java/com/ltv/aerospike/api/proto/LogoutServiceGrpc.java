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
    comments = "Source: LogoutServices.proto")
public final class LogoutServiceGrpc {

  private LogoutServiceGrpc() {}

  public static final String SERVICE_NAME = "com.ltv.aerospike.api.proto.LogoutService";

  // Static method descriptors that strictly reflect the proto.
  private static volatile io.grpc.MethodDescriptor<com.ltv.aerospike.api.proto.LogoutServices.LogoutRequest,
      com.ltv.aerospike.api.proto.LogoutServices.LogoutResponse> getLogoutMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "logout",
      requestType = com.ltv.aerospike.api.proto.LogoutServices.LogoutRequest.class,
      responseType = com.ltv.aerospike.api.proto.LogoutServices.LogoutResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.ltv.aerospike.api.proto.LogoutServices.LogoutRequest,
      com.ltv.aerospike.api.proto.LogoutServices.LogoutResponse> getLogoutMethod() {
    io.grpc.MethodDescriptor<com.ltv.aerospike.api.proto.LogoutServices.LogoutRequest, com.ltv.aerospike.api.proto.LogoutServices.LogoutResponse> getLogoutMethod;
    if ((getLogoutMethod = LogoutServiceGrpc.getLogoutMethod) == null) {
      synchronized (LogoutServiceGrpc.class) {
        if ((getLogoutMethod = LogoutServiceGrpc.getLogoutMethod) == null) {
          LogoutServiceGrpc.getLogoutMethod = getLogoutMethod =
              io.grpc.MethodDescriptor.<com.ltv.aerospike.api.proto.LogoutServices.LogoutRequest, com.ltv.aerospike.api.proto.LogoutServices.LogoutResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "logout"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.ltv.aerospike.api.proto.LogoutServices.LogoutRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.ltv.aerospike.api.proto.LogoutServices.LogoutResponse.getDefaultInstance()))
              .setSchemaDescriptor(new LogoutServiceMethodDescriptorSupplier("logout"))
              .build();
        }
      }
    }
    return getLogoutMethod;
  }

  /**
   * Creates a new async stub that supports all call types for the service
   */
  public static LogoutServiceStub newStub(io.grpc.Channel channel) {
    return new LogoutServiceStub(channel);
  }

  /**
   * Creates a new blocking-style stub that supports unary and streaming output calls on the service
   */
  public static LogoutServiceBlockingStub newBlockingStub(
      io.grpc.Channel channel) {
    return new LogoutServiceBlockingStub(channel);
  }

  /**
   * Creates a new ListenableFuture-style stub that supports unary calls on the service
   */
  public static LogoutServiceFutureStub newFutureStub(
      io.grpc.Channel channel) {
    return new LogoutServiceFutureStub(channel);
  }

  /**
   */
  public static abstract class LogoutServiceImplBase implements io.grpc.BindableService {

    /**
     */
    public void logout(com.ltv.aerospike.api.proto.LogoutServices.LogoutRequest request,
        io.grpc.stub.StreamObserver<com.ltv.aerospike.api.proto.LogoutServices.LogoutResponse> responseObserver) {
      asyncUnimplementedUnaryCall(getLogoutMethod(), responseObserver);
    }

    @java.lang.Override public final io.grpc.ServerServiceDefinition bindService() {
      return io.grpc.ServerServiceDefinition.builder(getServiceDescriptor())
          .addMethod(
            getLogoutMethod(),
            asyncUnaryCall(
              new MethodHandlers<
                com.ltv.aerospike.api.proto.LogoutServices.LogoutRequest,
                com.ltv.aerospike.api.proto.LogoutServices.LogoutResponse>(
                  this, METHODID_LOGOUT)))
          .build();
    }
  }

  /**
   */
  public static final class LogoutServiceStub extends io.grpc.stub.AbstractStub<LogoutServiceStub> {
    private LogoutServiceStub(io.grpc.Channel channel) {
      super(channel);
    }

    private LogoutServiceStub(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected LogoutServiceStub build(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      return new LogoutServiceStub(channel, callOptions);
    }

    /**
     */
    public void logout(com.ltv.aerospike.api.proto.LogoutServices.LogoutRequest request,
        io.grpc.stub.StreamObserver<com.ltv.aerospike.api.proto.LogoutServices.LogoutResponse> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(getLogoutMethod(), getCallOptions()), request, responseObserver);
    }
  }

  /**
   */
  public static final class LogoutServiceBlockingStub extends io.grpc.stub.AbstractStub<LogoutServiceBlockingStub> {
    private LogoutServiceBlockingStub(io.grpc.Channel channel) {
      super(channel);
    }

    private LogoutServiceBlockingStub(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected LogoutServiceBlockingStub build(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      return new LogoutServiceBlockingStub(channel, callOptions);
    }

    /**
     */
    public com.ltv.aerospike.api.proto.LogoutServices.LogoutResponse logout(com.ltv.aerospike.api.proto.LogoutServices.LogoutRequest request) {
      return blockingUnaryCall(
          getChannel(), getLogoutMethod(), getCallOptions(), request);
    }
  }

  /**
   */
  public static final class LogoutServiceFutureStub extends io.grpc.stub.AbstractStub<LogoutServiceFutureStub> {
    private LogoutServiceFutureStub(io.grpc.Channel channel) {
      super(channel);
    }

    private LogoutServiceFutureStub(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected LogoutServiceFutureStub build(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      return new LogoutServiceFutureStub(channel, callOptions);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<com.ltv.aerospike.api.proto.LogoutServices.LogoutResponse> logout(
        com.ltv.aerospike.api.proto.LogoutServices.LogoutRequest request) {
      return futureUnaryCall(
          getChannel().newCall(getLogoutMethod(), getCallOptions()), request);
    }
  }

  private static final int METHODID_LOGOUT = 0;

  private static final class MethodHandlers<Req, Resp> implements
      io.grpc.stub.ServerCalls.UnaryMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.ServerStreamingMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.ClientStreamingMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.BidiStreamingMethod<Req, Resp> {
    private final LogoutServiceImplBase serviceImpl;
    private final int methodId;

    MethodHandlers(LogoutServiceImplBase serviceImpl, int methodId) {
      this.serviceImpl = serviceImpl;
      this.methodId = methodId;
    }

    @java.lang.Override
    @java.lang.SuppressWarnings("unchecked")
    public void invoke(Req request, io.grpc.stub.StreamObserver<Resp> responseObserver) {
      switch (methodId) {
        case METHODID_LOGOUT:
          serviceImpl.logout((com.ltv.aerospike.api.proto.LogoutServices.LogoutRequest) request,
              (io.grpc.stub.StreamObserver<com.ltv.aerospike.api.proto.LogoutServices.LogoutResponse>) responseObserver);
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

  private static abstract class LogoutServiceBaseDescriptorSupplier
      implements io.grpc.protobuf.ProtoFileDescriptorSupplier, io.grpc.protobuf.ProtoServiceDescriptorSupplier {
    LogoutServiceBaseDescriptorSupplier() {}

    @java.lang.Override
    public com.google.protobuf.Descriptors.FileDescriptor getFileDescriptor() {
      return com.ltv.aerospike.api.proto.LogoutServices.getDescriptor();
    }

    @java.lang.Override
    public com.google.protobuf.Descriptors.ServiceDescriptor getServiceDescriptor() {
      return getFileDescriptor().findServiceByName("LogoutService");
    }
  }

  private static final class LogoutServiceFileDescriptorSupplier
      extends LogoutServiceBaseDescriptorSupplier {
    LogoutServiceFileDescriptorSupplier() {}
  }

  private static final class LogoutServiceMethodDescriptorSupplier
      extends LogoutServiceBaseDescriptorSupplier
      implements io.grpc.protobuf.ProtoMethodDescriptorSupplier {
    private final String methodName;

    LogoutServiceMethodDescriptorSupplier(String methodName) {
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
      synchronized (LogoutServiceGrpc.class) {
        result = serviceDescriptor;
        if (result == null) {
          serviceDescriptor = result = io.grpc.ServiceDescriptor.newBuilder(SERVICE_NAME)
              .setSchemaDescriptor(new LogoutServiceFileDescriptorSupplier())
              .addMethod(getLogoutMethod())
              .build();
        }
      }
    }
    return result;
  }
}
