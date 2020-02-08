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
    comments = "Source: RenameBinServices.proto")
public final class RenameBinServiceGrpc {

  private RenameBinServiceGrpc() {}

  public static final String SERVICE_NAME = "com.ltv.aerospike.api.proto.RenameBinService";

  // Static method descriptors that strictly reflect the proto.
  private static volatile io.grpc.MethodDescriptor<com.ltv.aerospike.api.proto.RenameBinServices.RenameBinRequest,
      com.ltv.aerospike.api.proto.RenameBinServices.RenameBinResponse> getRenameBinMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "renameBin",
      requestType = com.ltv.aerospike.api.proto.RenameBinServices.RenameBinRequest.class,
      responseType = com.ltv.aerospike.api.proto.RenameBinServices.RenameBinResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.ltv.aerospike.api.proto.RenameBinServices.RenameBinRequest,
      com.ltv.aerospike.api.proto.RenameBinServices.RenameBinResponse> getRenameBinMethod() {
    io.grpc.MethodDescriptor<com.ltv.aerospike.api.proto.RenameBinServices.RenameBinRequest, com.ltv.aerospike.api.proto.RenameBinServices.RenameBinResponse> getRenameBinMethod;
    if ((getRenameBinMethod = RenameBinServiceGrpc.getRenameBinMethod) == null) {
      synchronized (RenameBinServiceGrpc.class) {
        if ((getRenameBinMethod = RenameBinServiceGrpc.getRenameBinMethod) == null) {
          RenameBinServiceGrpc.getRenameBinMethod = getRenameBinMethod =
              io.grpc.MethodDescriptor.<com.ltv.aerospike.api.proto.RenameBinServices.RenameBinRequest, com.ltv.aerospike.api.proto.RenameBinServices.RenameBinResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "renameBin"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.ltv.aerospike.api.proto.RenameBinServices.RenameBinRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.ltv.aerospike.api.proto.RenameBinServices.RenameBinResponse.getDefaultInstance()))
              .setSchemaDescriptor(new RenameBinServiceMethodDescriptorSupplier("renameBin"))
              .build();
        }
      }
    }
    return getRenameBinMethod;
  }

  /**
   * Creates a new async stub that supports all call types for the service
   */
  public static RenameBinServiceStub newStub(io.grpc.Channel channel) {
    return new RenameBinServiceStub(channel);
  }

  /**
   * Creates a new blocking-style stub that supports unary and streaming output calls on the service
   */
  public static RenameBinServiceBlockingStub newBlockingStub(
      io.grpc.Channel channel) {
    return new RenameBinServiceBlockingStub(channel);
  }

  /**
   * Creates a new ListenableFuture-style stub that supports unary calls on the service
   */
  public static RenameBinServiceFutureStub newFutureStub(
      io.grpc.Channel channel) {
    return new RenameBinServiceFutureStub(channel);
  }

  /**
   */
  public static abstract class RenameBinServiceImplBase implements io.grpc.BindableService {

    /**
     */
    public void renameBin(com.ltv.aerospike.api.proto.RenameBinServices.RenameBinRequest request,
        io.grpc.stub.StreamObserver<com.ltv.aerospike.api.proto.RenameBinServices.RenameBinResponse> responseObserver) {
      asyncUnimplementedUnaryCall(getRenameBinMethod(), responseObserver);
    }

    @java.lang.Override public final io.grpc.ServerServiceDefinition bindService() {
      return io.grpc.ServerServiceDefinition.builder(getServiceDescriptor())
          .addMethod(
            getRenameBinMethod(),
            asyncUnaryCall(
              new MethodHandlers<
                com.ltv.aerospike.api.proto.RenameBinServices.RenameBinRequest,
                com.ltv.aerospike.api.proto.RenameBinServices.RenameBinResponse>(
                  this, METHODID_RENAME_BIN)))
          .build();
    }
  }

  /**
   */
  public static final class RenameBinServiceStub extends io.grpc.stub.AbstractStub<RenameBinServiceStub> {
    private RenameBinServiceStub(io.grpc.Channel channel) {
      super(channel);
    }

    private RenameBinServiceStub(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected RenameBinServiceStub build(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      return new RenameBinServiceStub(channel, callOptions);
    }

    /**
     */
    public void renameBin(com.ltv.aerospike.api.proto.RenameBinServices.RenameBinRequest request,
        io.grpc.stub.StreamObserver<com.ltv.aerospike.api.proto.RenameBinServices.RenameBinResponse> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(getRenameBinMethod(), getCallOptions()), request, responseObserver);
    }
  }

  /**
   */
  public static final class RenameBinServiceBlockingStub extends io.grpc.stub.AbstractStub<RenameBinServiceBlockingStub> {
    private RenameBinServiceBlockingStub(io.grpc.Channel channel) {
      super(channel);
    }

    private RenameBinServiceBlockingStub(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected RenameBinServiceBlockingStub build(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      return new RenameBinServiceBlockingStub(channel, callOptions);
    }

    /**
     */
    public com.ltv.aerospike.api.proto.RenameBinServices.RenameBinResponse renameBin(com.ltv.aerospike.api.proto.RenameBinServices.RenameBinRequest request) {
      return blockingUnaryCall(
          getChannel(), getRenameBinMethod(), getCallOptions(), request);
    }
  }

  /**
   */
  public static final class RenameBinServiceFutureStub extends io.grpc.stub.AbstractStub<RenameBinServiceFutureStub> {
    private RenameBinServiceFutureStub(io.grpc.Channel channel) {
      super(channel);
    }

    private RenameBinServiceFutureStub(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected RenameBinServiceFutureStub build(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      return new RenameBinServiceFutureStub(channel, callOptions);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<com.ltv.aerospike.api.proto.RenameBinServices.RenameBinResponse> renameBin(
        com.ltv.aerospike.api.proto.RenameBinServices.RenameBinRequest request) {
      return futureUnaryCall(
          getChannel().newCall(getRenameBinMethod(), getCallOptions()), request);
    }
  }

  private static final int METHODID_RENAME_BIN = 0;

  private static final class MethodHandlers<Req, Resp> implements
      io.grpc.stub.ServerCalls.UnaryMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.ServerStreamingMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.ClientStreamingMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.BidiStreamingMethod<Req, Resp> {
    private final RenameBinServiceImplBase serviceImpl;
    private final int methodId;

    MethodHandlers(RenameBinServiceImplBase serviceImpl, int methodId) {
      this.serviceImpl = serviceImpl;
      this.methodId = methodId;
    }

    @java.lang.Override
    @java.lang.SuppressWarnings("unchecked")
    public void invoke(Req request, io.grpc.stub.StreamObserver<Resp> responseObserver) {
      switch (methodId) {
        case METHODID_RENAME_BIN:
          serviceImpl.renameBin((com.ltv.aerospike.api.proto.RenameBinServices.RenameBinRequest) request,
              (io.grpc.stub.StreamObserver<com.ltv.aerospike.api.proto.RenameBinServices.RenameBinResponse>) responseObserver);
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

  private static abstract class RenameBinServiceBaseDescriptorSupplier
      implements io.grpc.protobuf.ProtoFileDescriptorSupplier, io.grpc.protobuf.ProtoServiceDescriptorSupplier {
    RenameBinServiceBaseDescriptorSupplier() {}

    @java.lang.Override
    public com.google.protobuf.Descriptors.FileDescriptor getFileDescriptor() {
      return com.ltv.aerospike.api.proto.RenameBinServices.getDescriptor();
    }

    @java.lang.Override
    public com.google.protobuf.Descriptors.ServiceDescriptor getServiceDescriptor() {
      return getFileDescriptor().findServiceByName("RenameBinService");
    }
  }

  private static final class RenameBinServiceFileDescriptorSupplier
      extends RenameBinServiceBaseDescriptorSupplier {
    RenameBinServiceFileDescriptorSupplier() {}
  }

  private static final class RenameBinServiceMethodDescriptorSupplier
      extends RenameBinServiceBaseDescriptorSupplier
      implements io.grpc.protobuf.ProtoMethodDescriptorSupplier {
    private final String methodName;

    RenameBinServiceMethodDescriptorSupplier(String methodName) {
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
      synchronized (RenameBinServiceGrpc.class) {
        result = serviceDescriptor;
        if (result == null) {
          serviceDescriptor = result = io.grpc.ServiceDescriptor.newBuilder(SERVICE_NAME)
              .setSchemaDescriptor(new RenameBinServiceFileDescriptorSupplier())
              .addMethod(getRenameBinMethod())
              .build();
        }
      }
    }
    return result;
  }
}
