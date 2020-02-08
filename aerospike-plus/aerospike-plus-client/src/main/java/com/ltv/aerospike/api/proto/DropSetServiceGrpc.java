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
    comments = "Source: DropSetServices.proto")
public final class DropSetServiceGrpc {

  private DropSetServiceGrpc() {}

  public static final String SERVICE_NAME = "com.ltv.aerospike.api.proto.DropSetService";

  // Static method descriptors that strictly reflect the proto.
  private static volatile io.grpc.MethodDescriptor<com.ltv.aerospike.api.proto.DropSetServices.DropSetRequest,
      com.ltv.aerospike.api.proto.DropSetServices.DropSetResponse> getDropSetMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "dropSet",
      requestType = com.ltv.aerospike.api.proto.DropSetServices.DropSetRequest.class,
      responseType = com.ltv.aerospike.api.proto.DropSetServices.DropSetResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.ltv.aerospike.api.proto.DropSetServices.DropSetRequest,
      com.ltv.aerospike.api.proto.DropSetServices.DropSetResponse> getDropSetMethod() {
    io.grpc.MethodDescriptor<com.ltv.aerospike.api.proto.DropSetServices.DropSetRequest, com.ltv.aerospike.api.proto.DropSetServices.DropSetResponse> getDropSetMethod;
    if ((getDropSetMethod = DropSetServiceGrpc.getDropSetMethod) == null) {
      synchronized (DropSetServiceGrpc.class) {
        if ((getDropSetMethod = DropSetServiceGrpc.getDropSetMethod) == null) {
          DropSetServiceGrpc.getDropSetMethod = getDropSetMethod =
              io.grpc.MethodDescriptor.<com.ltv.aerospike.api.proto.DropSetServices.DropSetRequest, com.ltv.aerospike.api.proto.DropSetServices.DropSetResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "dropSet"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.ltv.aerospike.api.proto.DropSetServices.DropSetRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.ltv.aerospike.api.proto.DropSetServices.DropSetResponse.getDefaultInstance()))
              .setSchemaDescriptor(new DropSetServiceMethodDescriptorSupplier("dropSet"))
              .build();
        }
      }
    }
    return getDropSetMethod;
  }

  /**
   * Creates a new async stub that supports all call types for the service
   */
  public static DropSetServiceStub newStub(io.grpc.Channel channel) {
    return new DropSetServiceStub(channel);
  }

  /**
   * Creates a new blocking-style stub that supports unary and streaming output calls on the service
   */
  public static DropSetServiceBlockingStub newBlockingStub(
      io.grpc.Channel channel) {
    return new DropSetServiceBlockingStub(channel);
  }

  /**
   * Creates a new ListenableFuture-style stub that supports unary calls on the service
   */
  public static DropSetServiceFutureStub newFutureStub(
      io.grpc.Channel channel) {
    return new DropSetServiceFutureStub(channel);
  }

  /**
   */
  public static abstract class DropSetServiceImplBase implements io.grpc.BindableService {

    /**
     */
    public void dropSet(com.ltv.aerospike.api.proto.DropSetServices.DropSetRequest request,
        io.grpc.stub.StreamObserver<com.ltv.aerospike.api.proto.DropSetServices.DropSetResponse> responseObserver) {
      asyncUnimplementedUnaryCall(getDropSetMethod(), responseObserver);
    }

    @java.lang.Override public final io.grpc.ServerServiceDefinition bindService() {
      return io.grpc.ServerServiceDefinition.builder(getServiceDescriptor())
          .addMethod(
            getDropSetMethod(),
            asyncUnaryCall(
              new MethodHandlers<
                com.ltv.aerospike.api.proto.DropSetServices.DropSetRequest,
                com.ltv.aerospike.api.proto.DropSetServices.DropSetResponse>(
                  this, METHODID_DROP_SET)))
          .build();
    }
  }

  /**
   */
  public static final class DropSetServiceStub extends io.grpc.stub.AbstractStub<DropSetServiceStub> {
    private DropSetServiceStub(io.grpc.Channel channel) {
      super(channel);
    }

    private DropSetServiceStub(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected DropSetServiceStub build(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      return new DropSetServiceStub(channel, callOptions);
    }

    /**
     */
    public void dropSet(com.ltv.aerospike.api.proto.DropSetServices.DropSetRequest request,
        io.grpc.stub.StreamObserver<com.ltv.aerospike.api.proto.DropSetServices.DropSetResponse> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(getDropSetMethod(), getCallOptions()), request, responseObserver);
    }
  }

  /**
   */
  public static final class DropSetServiceBlockingStub extends io.grpc.stub.AbstractStub<DropSetServiceBlockingStub> {
    private DropSetServiceBlockingStub(io.grpc.Channel channel) {
      super(channel);
    }

    private DropSetServiceBlockingStub(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected DropSetServiceBlockingStub build(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      return new DropSetServiceBlockingStub(channel, callOptions);
    }

    /**
     */
    public com.ltv.aerospike.api.proto.DropSetServices.DropSetResponse dropSet(com.ltv.aerospike.api.proto.DropSetServices.DropSetRequest request) {
      return blockingUnaryCall(
          getChannel(), getDropSetMethod(), getCallOptions(), request);
    }
  }

  /**
   */
  public static final class DropSetServiceFutureStub extends io.grpc.stub.AbstractStub<DropSetServiceFutureStub> {
    private DropSetServiceFutureStub(io.grpc.Channel channel) {
      super(channel);
    }

    private DropSetServiceFutureStub(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected DropSetServiceFutureStub build(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      return new DropSetServiceFutureStub(channel, callOptions);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<com.ltv.aerospike.api.proto.DropSetServices.DropSetResponse> dropSet(
        com.ltv.aerospike.api.proto.DropSetServices.DropSetRequest request) {
      return futureUnaryCall(
          getChannel().newCall(getDropSetMethod(), getCallOptions()), request);
    }
  }

  private static final int METHODID_DROP_SET = 0;

  private static final class MethodHandlers<Req, Resp> implements
      io.grpc.stub.ServerCalls.UnaryMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.ServerStreamingMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.ClientStreamingMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.BidiStreamingMethod<Req, Resp> {
    private final DropSetServiceImplBase serviceImpl;
    private final int methodId;

    MethodHandlers(DropSetServiceImplBase serviceImpl, int methodId) {
      this.serviceImpl = serviceImpl;
      this.methodId = methodId;
    }

    @java.lang.Override
    @java.lang.SuppressWarnings("unchecked")
    public void invoke(Req request, io.grpc.stub.StreamObserver<Resp> responseObserver) {
      switch (methodId) {
        case METHODID_DROP_SET:
          serviceImpl.dropSet((com.ltv.aerospike.api.proto.DropSetServices.DropSetRequest) request,
              (io.grpc.stub.StreamObserver<com.ltv.aerospike.api.proto.DropSetServices.DropSetResponse>) responseObserver);
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

  private static abstract class DropSetServiceBaseDescriptorSupplier
      implements io.grpc.protobuf.ProtoFileDescriptorSupplier, io.grpc.protobuf.ProtoServiceDescriptorSupplier {
    DropSetServiceBaseDescriptorSupplier() {}

    @java.lang.Override
    public com.google.protobuf.Descriptors.FileDescriptor getFileDescriptor() {
      return com.ltv.aerospike.api.proto.DropSetServices.getDescriptor();
    }

    @java.lang.Override
    public com.google.protobuf.Descriptors.ServiceDescriptor getServiceDescriptor() {
      return getFileDescriptor().findServiceByName("DropSetService");
    }
  }

  private static final class DropSetServiceFileDescriptorSupplier
      extends DropSetServiceBaseDescriptorSupplier {
    DropSetServiceFileDescriptorSupplier() {}
  }

  private static final class DropSetServiceMethodDescriptorSupplier
      extends DropSetServiceBaseDescriptorSupplier
      implements io.grpc.protobuf.ProtoMethodDescriptorSupplier {
    private final String methodName;

    DropSetServiceMethodDescriptorSupplier(String methodName) {
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
      synchronized (DropSetServiceGrpc.class) {
        result = serviceDescriptor;
        if (result == null) {
          serviceDescriptor = result = io.grpc.ServiceDescriptor.newBuilder(SERVICE_NAME)
              .setSchemaDescriptor(new DropSetServiceFileDescriptorSupplier())
              .addMethod(getDropSetMethod())
              .build();
        }
      }
    }
    return result;
  }
}
