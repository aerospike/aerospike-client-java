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
    comments = "Source: DropSequenceServices.proto")
public final class DropSequenceServiceGrpc {

  private DropSequenceServiceGrpc() {}

  public static final String SERVICE_NAME = "com.ltv.aerospike.api.proto.DropSequenceService";

  // Static method descriptors that strictly reflect the proto.
  private static volatile io.grpc.MethodDescriptor<com.ltv.aerospike.api.proto.DropSequenceServices.DropSequenceRequest,
      com.ltv.aerospike.api.proto.DropSequenceServices.DropSequenceResponse> getDropSequenceMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "dropSequence",
      requestType = com.ltv.aerospike.api.proto.DropSequenceServices.DropSequenceRequest.class,
      responseType = com.ltv.aerospike.api.proto.DropSequenceServices.DropSequenceResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.ltv.aerospike.api.proto.DropSequenceServices.DropSequenceRequest,
      com.ltv.aerospike.api.proto.DropSequenceServices.DropSequenceResponse> getDropSequenceMethod() {
    io.grpc.MethodDescriptor<com.ltv.aerospike.api.proto.DropSequenceServices.DropSequenceRequest, com.ltv.aerospike.api.proto.DropSequenceServices.DropSequenceResponse> getDropSequenceMethod;
    if ((getDropSequenceMethod = DropSequenceServiceGrpc.getDropSequenceMethod) == null) {
      synchronized (DropSequenceServiceGrpc.class) {
        if ((getDropSequenceMethod = DropSequenceServiceGrpc.getDropSequenceMethod) == null) {
          DropSequenceServiceGrpc.getDropSequenceMethod = getDropSequenceMethod =
              io.grpc.MethodDescriptor.<com.ltv.aerospike.api.proto.DropSequenceServices.DropSequenceRequest, com.ltv.aerospike.api.proto.DropSequenceServices.DropSequenceResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "dropSequence"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.ltv.aerospike.api.proto.DropSequenceServices.DropSequenceRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.ltv.aerospike.api.proto.DropSequenceServices.DropSequenceResponse.getDefaultInstance()))
              .setSchemaDescriptor(new DropSequenceServiceMethodDescriptorSupplier("dropSequence"))
              .build();
        }
      }
    }
    return getDropSequenceMethod;
  }

  /**
   * Creates a new async stub that supports all call types for the service
   */
  public static DropSequenceServiceStub newStub(io.grpc.Channel channel) {
    return new DropSequenceServiceStub(channel);
  }

  /**
   * Creates a new blocking-style stub that supports unary and streaming output calls on the service
   */
  public static DropSequenceServiceBlockingStub newBlockingStub(
      io.grpc.Channel channel) {
    return new DropSequenceServiceBlockingStub(channel);
  }

  /**
   * Creates a new ListenableFuture-style stub that supports unary calls on the service
   */
  public static DropSequenceServiceFutureStub newFutureStub(
      io.grpc.Channel channel) {
    return new DropSequenceServiceFutureStub(channel);
  }

  /**
   */
  public static abstract class DropSequenceServiceImplBase implements io.grpc.BindableService {

    /**
     */
    public void dropSequence(com.ltv.aerospike.api.proto.DropSequenceServices.DropSequenceRequest request,
        io.grpc.stub.StreamObserver<com.ltv.aerospike.api.proto.DropSequenceServices.DropSequenceResponse> responseObserver) {
      asyncUnimplementedUnaryCall(getDropSequenceMethod(), responseObserver);
    }

    @java.lang.Override public final io.grpc.ServerServiceDefinition bindService() {
      return io.grpc.ServerServiceDefinition.builder(getServiceDescriptor())
          .addMethod(
            getDropSequenceMethod(),
            asyncUnaryCall(
              new MethodHandlers<
                com.ltv.aerospike.api.proto.DropSequenceServices.DropSequenceRequest,
                com.ltv.aerospike.api.proto.DropSequenceServices.DropSequenceResponse>(
                  this, METHODID_DROP_SEQUENCE)))
          .build();
    }
  }

  /**
   */
  public static final class DropSequenceServiceStub extends io.grpc.stub.AbstractStub<DropSequenceServiceStub> {
    private DropSequenceServiceStub(io.grpc.Channel channel) {
      super(channel);
    }

    private DropSequenceServiceStub(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected DropSequenceServiceStub build(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      return new DropSequenceServiceStub(channel, callOptions);
    }

    /**
     */
    public void dropSequence(com.ltv.aerospike.api.proto.DropSequenceServices.DropSequenceRequest request,
        io.grpc.stub.StreamObserver<com.ltv.aerospike.api.proto.DropSequenceServices.DropSequenceResponse> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(getDropSequenceMethod(), getCallOptions()), request, responseObserver);
    }
  }

  /**
   */
  public static final class DropSequenceServiceBlockingStub extends io.grpc.stub.AbstractStub<DropSequenceServiceBlockingStub> {
    private DropSequenceServiceBlockingStub(io.grpc.Channel channel) {
      super(channel);
    }

    private DropSequenceServiceBlockingStub(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected DropSequenceServiceBlockingStub build(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      return new DropSequenceServiceBlockingStub(channel, callOptions);
    }

    /**
     */
    public com.ltv.aerospike.api.proto.DropSequenceServices.DropSequenceResponse dropSequence(com.ltv.aerospike.api.proto.DropSequenceServices.DropSequenceRequest request) {
      return blockingUnaryCall(
          getChannel(), getDropSequenceMethod(), getCallOptions(), request);
    }
  }

  /**
   */
  public static final class DropSequenceServiceFutureStub extends io.grpc.stub.AbstractStub<DropSequenceServiceFutureStub> {
    private DropSequenceServiceFutureStub(io.grpc.Channel channel) {
      super(channel);
    }

    private DropSequenceServiceFutureStub(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected DropSequenceServiceFutureStub build(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      return new DropSequenceServiceFutureStub(channel, callOptions);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<com.ltv.aerospike.api.proto.DropSequenceServices.DropSequenceResponse> dropSequence(
        com.ltv.aerospike.api.proto.DropSequenceServices.DropSequenceRequest request) {
      return futureUnaryCall(
          getChannel().newCall(getDropSequenceMethod(), getCallOptions()), request);
    }
  }

  private static final int METHODID_DROP_SEQUENCE = 0;

  private static final class MethodHandlers<Req, Resp> implements
      io.grpc.stub.ServerCalls.UnaryMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.ServerStreamingMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.ClientStreamingMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.BidiStreamingMethod<Req, Resp> {
    private final DropSequenceServiceImplBase serviceImpl;
    private final int methodId;

    MethodHandlers(DropSequenceServiceImplBase serviceImpl, int methodId) {
      this.serviceImpl = serviceImpl;
      this.methodId = methodId;
    }

    @java.lang.Override
    @java.lang.SuppressWarnings("unchecked")
    public void invoke(Req request, io.grpc.stub.StreamObserver<Resp> responseObserver) {
      switch (methodId) {
        case METHODID_DROP_SEQUENCE:
          serviceImpl.dropSequence((com.ltv.aerospike.api.proto.DropSequenceServices.DropSequenceRequest) request,
              (io.grpc.stub.StreamObserver<com.ltv.aerospike.api.proto.DropSequenceServices.DropSequenceResponse>) responseObserver);
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

  private static abstract class DropSequenceServiceBaseDescriptorSupplier
      implements io.grpc.protobuf.ProtoFileDescriptorSupplier, io.grpc.protobuf.ProtoServiceDescriptorSupplier {
    DropSequenceServiceBaseDescriptorSupplier() {}

    @java.lang.Override
    public com.google.protobuf.Descriptors.FileDescriptor getFileDescriptor() {
      return com.ltv.aerospike.api.proto.DropSequenceServices.getDescriptor();
    }

    @java.lang.Override
    public com.google.protobuf.Descriptors.ServiceDescriptor getServiceDescriptor() {
      return getFileDescriptor().findServiceByName("DropSequenceService");
    }
  }

  private static final class DropSequenceServiceFileDescriptorSupplier
      extends DropSequenceServiceBaseDescriptorSupplier {
    DropSequenceServiceFileDescriptorSupplier() {}
  }

  private static final class DropSequenceServiceMethodDescriptorSupplier
      extends DropSequenceServiceBaseDescriptorSupplier
      implements io.grpc.protobuf.ProtoMethodDescriptorSupplier {
    private final String methodName;

    DropSequenceServiceMethodDescriptorSupplier(String methodName) {
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
      synchronized (DropSequenceServiceGrpc.class) {
        result = serviceDescriptor;
        if (result == null) {
          serviceDescriptor = result = io.grpc.ServiceDescriptor.newBuilder(SERVICE_NAME)
              .setSchemaDescriptor(new DropSequenceServiceFileDescriptorSupplier())
              .addMethod(getDropSequenceMethod())
              .build();
        }
      }
    }
    return result;
  }
}
