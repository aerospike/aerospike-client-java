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
    comments = "Source: DropBinServices.proto")
public final class DropBinServiceGrpc {

  private DropBinServiceGrpc() {}

  public static final String SERVICE_NAME = "com.ltv.aerospike.api.proto.DropBinService";

  // Static method descriptors that strictly reflect the proto.
  private static volatile io.grpc.MethodDescriptor<com.ltv.aerospike.api.proto.DropBinServices.DropBinRequest,
      com.ltv.aerospike.api.proto.DropBinServices.DropBinResponse> getDropBinMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "dropBin",
      requestType = com.ltv.aerospike.api.proto.DropBinServices.DropBinRequest.class,
      responseType = com.ltv.aerospike.api.proto.DropBinServices.DropBinResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.ltv.aerospike.api.proto.DropBinServices.DropBinRequest,
      com.ltv.aerospike.api.proto.DropBinServices.DropBinResponse> getDropBinMethod() {
    io.grpc.MethodDescriptor<com.ltv.aerospike.api.proto.DropBinServices.DropBinRequest, com.ltv.aerospike.api.proto.DropBinServices.DropBinResponse> getDropBinMethod;
    if ((getDropBinMethod = DropBinServiceGrpc.getDropBinMethod) == null) {
      synchronized (DropBinServiceGrpc.class) {
        if ((getDropBinMethod = DropBinServiceGrpc.getDropBinMethod) == null) {
          DropBinServiceGrpc.getDropBinMethod = getDropBinMethod =
              io.grpc.MethodDescriptor.<com.ltv.aerospike.api.proto.DropBinServices.DropBinRequest, com.ltv.aerospike.api.proto.DropBinServices.DropBinResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "dropBin"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.ltv.aerospike.api.proto.DropBinServices.DropBinRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.ltv.aerospike.api.proto.DropBinServices.DropBinResponse.getDefaultInstance()))
              .setSchemaDescriptor(new DropBinServiceMethodDescriptorSupplier("dropBin"))
              .build();
        }
      }
    }
    return getDropBinMethod;
  }

  /**
   * Creates a new async stub that supports all call types for the service
   */
  public static DropBinServiceStub newStub(io.grpc.Channel channel) {
    return new DropBinServiceStub(channel);
  }

  /**
   * Creates a new blocking-style stub that supports unary and streaming output calls on the service
   */
  public static DropBinServiceBlockingStub newBlockingStub(
      io.grpc.Channel channel) {
    return new DropBinServiceBlockingStub(channel);
  }

  /**
   * Creates a new ListenableFuture-style stub that supports unary calls on the service
   */
  public static DropBinServiceFutureStub newFutureStub(
      io.grpc.Channel channel) {
    return new DropBinServiceFutureStub(channel);
  }

  /**
   */
  public static abstract class DropBinServiceImplBase implements io.grpc.BindableService {

    /**
     */
    public void dropBin(com.ltv.aerospike.api.proto.DropBinServices.DropBinRequest request,
        io.grpc.stub.StreamObserver<com.ltv.aerospike.api.proto.DropBinServices.DropBinResponse> responseObserver) {
      asyncUnimplementedUnaryCall(getDropBinMethod(), responseObserver);
    }

    @java.lang.Override public final io.grpc.ServerServiceDefinition bindService() {
      return io.grpc.ServerServiceDefinition.builder(getServiceDescriptor())
          .addMethod(
            getDropBinMethod(),
            asyncUnaryCall(
              new MethodHandlers<
                com.ltv.aerospike.api.proto.DropBinServices.DropBinRequest,
                com.ltv.aerospike.api.proto.DropBinServices.DropBinResponse>(
                  this, METHODID_DROP_BIN)))
          .build();
    }
  }

  /**
   */
  public static final class DropBinServiceStub extends io.grpc.stub.AbstractStub<DropBinServiceStub> {
    private DropBinServiceStub(io.grpc.Channel channel) {
      super(channel);
    }

    private DropBinServiceStub(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected DropBinServiceStub build(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      return new DropBinServiceStub(channel, callOptions);
    }

    /**
     */
    public void dropBin(com.ltv.aerospike.api.proto.DropBinServices.DropBinRequest request,
        io.grpc.stub.StreamObserver<com.ltv.aerospike.api.proto.DropBinServices.DropBinResponse> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(getDropBinMethod(), getCallOptions()), request, responseObserver);
    }
  }

  /**
   */
  public static final class DropBinServiceBlockingStub extends io.grpc.stub.AbstractStub<DropBinServiceBlockingStub> {
    private DropBinServiceBlockingStub(io.grpc.Channel channel) {
      super(channel);
    }

    private DropBinServiceBlockingStub(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected DropBinServiceBlockingStub build(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      return new DropBinServiceBlockingStub(channel, callOptions);
    }

    /**
     */
    public com.ltv.aerospike.api.proto.DropBinServices.DropBinResponse dropBin(com.ltv.aerospike.api.proto.DropBinServices.DropBinRequest request) {
      return blockingUnaryCall(
          getChannel(), getDropBinMethod(), getCallOptions(), request);
    }
  }

  /**
   */
  public static final class DropBinServiceFutureStub extends io.grpc.stub.AbstractStub<DropBinServiceFutureStub> {
    private DropBinServiceFutureStub(io.grpc.Channel channel) {
      super(channel);
    }

    private DropBinServiceFutureStub(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected DropBinServiceFutureStub build(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      return new DropBinServiceFutureStub(channel, callOptions);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<com.ltv.aerospike.api.proto.DropBinServices.DropBinResponse> dropBin(
        com.ltv.aerospike.api.proto.DropBinServices.DropBinRequest request) {
      return futureUnaryCall(
          getChannel().newCall(getDropBinMethod(), getCallOptions()), request);
    }
  }

  private static final int METHODID_DROP_BIN = 0;

  private static final class MethodHandlers<Req, Resp> implements
      io.grpc.stub.ServerCalls.UnaryMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.ServerStreamingMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.ClientStreamingMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.BidiStreamingMethod<Req, Resp> {
    private final DropBinServiceImplBase serviceImpl;
    private final int methodId;

    MethodHandlers(DropBinServiceImplBase serviceImpl, int methodId) {
      this.serviceImpl = serviceImpl;
      this.methodId = methodId;
    }

    @java.lang.Override
    @java.lang.SuppressWarnings("unchecked")
    public void invoke(Req request, io.grpc.stub.StreamObserver<Resp> responseObserver) {
      switch (methodId) {
        case METHODID_DROP_BIN:
          serviceImpl.dropBin((com.ltv.aerospike.api.proto.DropBinServices.DropBinRequest) request,
              (io.grpc.stub.StreamObserver<com.ltv.aerospike.api.proto.DropBinServices.DropBinResponse>) responseObserver);
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

  private static abstract class DropBinServiceBaseDescriptorSupplier
      implements io.grpc.protobuf.ProtoFileDescriptorSupplier, io.grpc.protobuf.ProtoServiceDescriptorSupplier {
    DropBinServiceBaseDescriptorSupplier() {}

    @java.lang.Override
    public com.google.protobuf.Descriptors.FileDescriptor getFileDescriptor() {
      return com.ltv.aerospike.api.proto.DropBinServices.getDescriptor();
    }

    @java.lang.Override
    public com.google.protobuf.Descriptors.ServiceDescriptor getServiceDescriptor() {
      return getFileDescriptor().findServiceByName("DropBinService");
    }
  }

  private static final class DropBinServiceFileDescriptorSupplier
      extends DropBinServiceBaseDescriptorSupplier {
    DropBinServiceFileDescriptorSupplier() {}
  }

  private static final class DropBinServiceMethodDescriptorSupplier
      extends DropBinServiceBaseDescriptorSupplier
      implements io.grpc.protobuf.ProtoMethodDescriptorSupplier {
    private final String methodName;

    DropBinServiceMethodDescriptorSupplier(String methodName) {
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
      synchronized (DropBinServiceGrpc.class) {
        result = serviceDescriptor;
        if (result == null) {
          serviceDescriptor = result = io.grpc.ServiceDescriptor.newBuilder(SERVICE_NAME)
              .setSchemaDescriptor(new DropBinServiceFileDescriptorSupplier())
              .addMethod(getDropBinMethod())
              .build();
        }
      }
    }
    return result;
  }
}
