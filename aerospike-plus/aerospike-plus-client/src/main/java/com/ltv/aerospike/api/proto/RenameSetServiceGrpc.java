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
    comments = "Source: RenameSetServices.proto")
public final class RenameSetServiceGrpc {

  private RenameSetServiceGrpc() {}

  public static final String SERVICE_NAME = "com.ltv.aerospike.api.proto.RenameSetService";

  // Static method descriptors that strictly reflect the proto.
  private static volatile io.grpc.MethodDescriptor<com.ltv.aerospike.api.proto.RenameSetServices.RenameSetRequest,
      com.ltv.aerospike.api.proto.RenameSetServices.RenameSetResponse> getRenameSetMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "renameSet",
      requestType = com.ltv.aerospike.api.proto.RenameSetServices.RenameSetRequest.class,
      responseType = com.ltv.aerospike.api.proto.RenameSetServices.RenameSetResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.ltv.aerospike.api.proto.RenameSetServices.RenameSetRequest,
      com.ltv.aerospike.api.proto.RenameSetServices.RenameSetResponse> getRenameSetMethod() {
    io.grpc.MethodDescriptor<com.ltv.aerospike.api.proto.RenameSetServices.RenameSetRequest, com.ltv.aerospike.api.proto.RenameSetServices.RenameSetResponse> getRenameSetMethod;
    if ((getRenameSetMethod = RenameSetServiceGrpc.getRenameSetMethod) == null) {
      synchronized (RenameSetServiceGrpc.class) {
        if ((getRenameSetMethod = RenameSetServiceGrpc.getRenameSetMethod) == null) {
          RenameSetServiceGrpc.getRenameSetMethod = getRenameSetMethod =
              io.grpc.MethodDescriptor.<com.ltv.aerospike.api.proto.RenameSetServices.RenameSetRequest, com.ltv.aerospike.api.proto.RenameSetServices.RenameSetResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "renameSet"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.ltv.aerospike.api.proto.RenameSetServices.RenameSetRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.ltv.aerospike.api.proto.RenameSetServices.RenameSetResponse.getDefaultInstance()))
              .setSchemaDescriptor(new RenameSetServiceMethodDescriptorSupplier("renameSet"))
              .build();
        }
      }
    }
    return getRenameSetMethod;
  }

  /**
   * Creates a new async stub that supports all call types for the service
   */
  public static RenameSetServiceStub newStub(io.grpc.Channel channel) {
    return new RenameSetServiceStub(channel);
  }

  /**
   * Creates a new blocking-style stub that supports unary and streaming output calls on the service
   */
  public static RenameSetServiceBlockingStub newBlockingStub(
      io.grpc.Channel channel) {
    return new RenameSetServiceBlockingStub(channel);
  }

  /**
   * Creates a new ListenableFuture-style stub that supports unary calls on the service
   */
  public static RenameSetServiceFutureStub newFutureStub(
      io.grpc.Channel channel) {
    return new RenameSetServiceFutureStub(channel);
  }

  /**
   */
  public static abstract class RenameSetServiceImplBase implements io.grpc.BindableService {

    /**
     */
    public void renameSet(com.ltv.aerospike.api.proto.RenameSetServices.RenameSetRequest request,
        io.grpc.stub.StreamObserver<com.ltv.aerospike.api.proto.RenameSetServices.RenameSetResponse> responseObserver) {
      asyncUnimplementedUnaryCall(getRenameSetMethod(), responseObserver);
    }

    @java.lang.Override public final io.grpc.ServerServiceDefinition bindService() {
      return io.grpc.ServerServiceDefinition.builder(getServiceDescriptor())
          .addMethod(
            getRenameSetMethod(),
            asyncUnaryCall(
              new MethodHandlers<
                com.ltv.aerospike.api.proto.RenameSetServices.RenameSetRequest,
                com.ltv.aerospike.api.proto.RenameSetServices.RenameSetResponse>(
                  this, METHODID_RENAME_SET)))
          .build();
    }
  }

  /**
   */
  public static final class RenameSetServiceStub extends io.grpc.stub.AbstractStub<RenameSetServiceStub> {
    private RenameSetServiceStub(io.grpc.Channel channel) {
      super(channel);
    }

    private RenameSetServiceStub(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected RenameSetServiceStub build(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      return new RenameSetServiceStub(channel, callOptions);
    }

    /**
     */
    public void renameSet(com.ltv.aerospike.api.proto.RenameSetServices.RenameSetRequest request,
        io.grpc.stub.StreamObserver<com.ltv.aerospike.api.proto.RenameSetServices.RenameSetResponse> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(getRenameSetMethod(), getCallOptions()), request, responseObserver);
    }
  }

  /**
   */
  public static final class RenameSetServiceBlockingStub extends io.grpc.stub.AbstractStub<RenameSetServiceBlockingStub> {
    private RenameSetServiceBlockingStub(io.grpc.Channel channel) {
      super(channel);
    }

    private RenameSetServiceBlockingStub(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected RenameSetServiceBlockingStub build(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      return new RenameSetServiceBlockingStub(channel, callOptions);
    }

    /**
     */
    public com.ltv.aerospike.api.proto.RenameSetServices.RenameSetResponse renameSet(com.ltv.aerospike.api.proto.RenameSetServices.RenameSetRequest request) {
      return blockingUnaryCall(
          getChannel(), getRenameSetMethod(), getCallOptions(), request);
    }
  }

  /**
   */
  public static final class RenameSetServiceFutureStub extends io.grpc.stub.AbstractStub<RenameSetServiceFutureStub> {
    private RenameSetServiceFutureStub(io.grpc.Channel channel) {
      super(channel);
    }

    private RenameSetServiceFutureStub(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected RenameSetServiceFutureStub build(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      return new RenameSetServiceFutureStub(channel, callOptions);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<com.ltv.aerospike.api.proto.RenameSetServices.RenameSetResponse> renameSet(
        com.ltv.aerospike.api.proto.RenameSetServices.RenameSetRequest request) {
      return futureUnaryCall(
          getChannel().newCall(getRenameSetMethod(), getCallOptions()), request);
    }
  }

  private static final int METHODID_RENAME_SET = 0;

  private static final class MethodHandlers<Req, Resp> implements
      io.grpc.stub.ServerCalls.UnaryMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.ServerStreamingMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.ClientStreamingMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.BidiStreamingMethod<Req, Resp> {
    private final RenameSetServiceImplBase serviceImpl;
    private final int methodId;

    MethodHandlers(RenameSetServiceImplBase serviceImpl, int methodId) {
      this.serviceImpl = serviceImpl;
      this.methodId = methodId;
    }

    @java.lang.Override
    @java.lang.SuppressWarnings("unchecked")
    public void invoke(Req request, io.grpc.stub.StreamObserver<Resp> responseObserver) {
      switch (methodId) {
        case METHODID_RENAME_SET:
          serviceImpl.renameSet((com.ltv.aerospike.api.proto.RenameSetServices.RenameSetRequest) request,
              (io.grpc.stub.StreamObserver<com.ltv.aerospike.api.proto.RenameSetServices.RenameSetResponse>) responseObserver);
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

  private static abstract class RenameSetServiceBaseDescriptorSupplier
      implements io.grpc.protobuf.ProtoFileDescriptorSupplier, io.grpc.protobuf.ProtoServiceDescriptorSupplier {
    RenameSetServiceBaseDescriptorSupplier() {}

    @java.lang.Override
    public com.google.protobuf.Descriptors.FileDescriptor getFileDescriptor() {
      return com.ltv.aerospike.api.proto.RenameSetServices.getDescriptor();
    }

    @java.lang.Override
    public com.google.protobuf.Descriptors.ServiceDescriptor getServiceDescriptor() {
      return getFileDescriptor().findServiceByName("RenameSetService");
    }
  }

  private static final class RenameSetServiceFileDescriptorSupplier
      extends RenameSetServiceBaseDescriptorSupplier {
    RenameSetServiceFileDescriptorSupplier() {}
  }

  private static final class RenameSetServiceMethodDescriptorSupplier
      extends RenameSetServiceBaseDescriptorSupplier
      implements io.grpc.protobuf.ProtoMethodDescriptorSupplier {
    private final String methodName;

    RenameSetServiceMethodDescriptorSupplier(String methodName) {
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
      synchronized (RenameSetServiceGrpc.class) {
        result = serviceDescriptor;
        if (result == null) {
          serviceDescriptor = result = io.grpc.ServiceDescriptor.newBuilder(SERVICE_NAME)
              .setSchemaDescriptor(new RenameSetServiceFileDescriptorSupplier())
              .addMethod(getRenameSetMethod())
              .build();
        }
      }
    }
    return result;
  }
}
