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
    comments = "Source: CreateSequenceServices.proto")
public final class CreateSequenceServiceGrpc {

  private CreateSequenceServiceGrpc() {}

  public static final String SERVICE_NAME = "com.ltv.aerospike.api.proto.CreateSequenceService";

  // Static method descriptors that strictly reflect the proto.
  private static volatile io.grpc.MethodDescriptor<com.ltv.aerospike.api.proto.CreateSequenceServices.CreateSequenceRequest,
      com.ltv.aerospike.api.proto.CreateSequenceServices.CreateSequenceResponse> getCreateSequenceMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "createSequence",
      requestType = com.ltv.aerospike.api.proto.CreateSequenceServices.CreateSequenceRequest.class,
      responseType = com.ltv.aerospike.api.proto.CreateSequenceServices.CreateSequenceResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.ltv.aerospike.api.proto.CreateSequenceServices.CreateSequenceRequest,
      com.ltv.aerospike.api.proto.CreateSequenceServices.CreateSequenceResponse> getCreateSequenceMethod() {
    io.grpc.MethodDescriptor<com.ltv.aerospike.api.proto.CreateSequenceServices.CreateSequenceRequest, com.ltv.aerospike.api.proto.CreateSequenceServices.CreateSequenceResponse> getCreateSequenceMethod;
    if ((getCreateSequenceMethod = CreateSequenceServiceGrpc.getCreateSequenceMethod) == null) {
      synchronized (CreateSequenceServiceGrpc.class) {
        if ((getCreateSequenceMethod = CreateSequenceServiceGrpc.getCreateSequenceMethod) == null) {
          CreateSequenceServiceGrpc.getCreateSequenceMethod = getCreateSequenceMethod =
              io.grpc.MethodDescriptor.<com.ltv.aerospike.api.proto.CreateSequenceServices.CreateSequenceRequest, com.ltv.aerospike.api.proto.CreateSequenceServices.CreateSequenceResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "createSequence"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.ltv.aerospike.api.proto.CreateSequenceServices.CreateSequenceRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.ltv.aerospike.api.proto.CreateSequenceServices.CreateSequenceResponse.getDefaultInstance()))
              .setSchemaDescriptor(new CreateSequenceServiceMethodDescriptorSupplier("createSequence"))
              .build();
        }
      }
    }
    return getCreateSequenceMethod;
  }

  /**
   * Creates a new async stub that supports all call types for the service
   */
  public static CreateSequenceServiceStub newStub(io.grpc.Channel channel) {
    return new CreateSequenceServiceStub(channel);
  }

  /**
   * Creates a new blocking-style stub that supports unary and streaming output calls on the service
   */
  public static CreateSequenceServiceBlockingStub newBlockingStub(
      io.grpc.Channel channel) {
    return new CreateSequenceServiceBlockingStub(channel);
  }

  /**
   * Creates a new ListenableFuture-style stub that supports unary calls on the service
   */
  public static CreateSequenceServiceFutureStub newFutureStub(
      io.grpc.Channel channel) {
    return new CreateSequenceServiceFutureStub(channel);
  }

  /**
   */
  public static abstract class CreateSequenceServiceImplBase implements io.grpc.BindableService {

    /**
     */
    public void createSequence(com.ltv.aerospike.api.proto.CreateSequenceServices.CreateSequenceRequest request,
        io.grpc.stub.StreamObserver<com.ltv.aerospike.api.proto.CreateSequenceServices.CreateSequenceResponse> responseObserver) {
      asyncUnimplementedUnaryCall(getCreateSequenceMethod(), responseObserver);
    }

    @java.lang.Override public final io.grpc.ServerServiceDefinition bindService() {
      return io.grpc.ServerServiceDefinition.builder(getServiceDescriptor())
          .addMethod(
            getCreateSequenceMethod(),
            asyncUnaryCall(
              new MethodHandlers<
                com.ltv.aerospike.api.proto.CreateSequenceServices.CreateSequenceRequest,
                com.ltv.aerospike.api.proto.CreateSequenceServices.CreateSequenceResponse>(
                  this, METHODID_CREATE_SEQUENCE)))
          .build();
    }
  }

  /**
   */
  public static final class CreateSequenceServiceStub extends io.grpc.stub.AbstractStub<CreateSequenceServiceStub> {
    private CreateSequenceServiceStub(io.grpc.Channel channel) {
      super(channel);
    }

    private CreateSequenceServiceStub(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected CreateSequenceServiceStub build(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      return new CreateSequenceServiceStub(channel, callOptions);
    }

    /**
     */
    public void createSequence(com.ltv.aerospike.api.proto.CreateSequenceServices.CreateSequenceRequest request,
        io.grpc.stub.StreamObserver<com.ltv.aerospike.api.proto.CreateSequenceServices.CreateSequenceResponse> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(getCreateSequenceMethod(), getCallOptions()), request, responseObserver);
    }
  }

  /**
   */
  public static final class CreateSequenceServiceBlockingStub extends io.grpc.stub.AbstractStub<CreateSequenceServiceBlockingStub> {
    private CreateSequenceServiceBlockingStub(io.grpc.Channel channel) {
      super(channel);
    }

    private CreateSequenceServiceBlockingStub(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected CreateSequenceServiceBlockingStub build(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      return new CreateSequenceServiceBlockingStub(channel, callOptions);
    }

    /**
     */
    public com.ltv.aerospike.api.proto.CreateSequenceServices.CreateSequenceResponse createSequence(com.ltv.aerospike.api.proto.CreateSequenceServices.CreateSequenceRequest request) {
      return blockingUnaryCall(
          getChannel(), getCreateSequenceMethod(), getCallOptions(), request);
    }
  }

  /**
   */
  public static final class CreateSequenceServiceFutureStub extends io.grpc.stub.AbstractStub<CreateSequenceServiceFutureStub> {
    private CreateSequenceServiceFutureStub(io.grpc.Channel channel) {
      super(channel);
    }

    private CreateSequenceServiceFutureStub(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected CreateSequenceServiceFutureStub build(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      return new CreateSequenceServiceFutureStub(channel, callOptions);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<com.ltv.aerospike.api.proto.CreateSequenceServices.CreateSequenceResponse> createSequence(
        com.ltv.aerospike.api.proto.CreateSequenceServices.CreateSequenceRequest request) {
      return futureUnaryCall(
          getChannel().newCall(getCreateSequenceMethod(), getCallOptions()), request);
    }
  }

  private static final int METHODID_CREATE_SEQUENCE = 0;

  private static final class MethodHandlers<Req, Resp> implements
      io.grpc.stub.ServerCalls.UnaryMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.ServerStreamingMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.ClientStreamingMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.BidiStreamingMethod<Req, Resp> {
    private final CreateSequenceServiceImplBase serviceImpl;
    private final int methodId;

    MethodHandlers(CreateSequenceServiceImplBase serviceImpl, int methodId) {
      this.serviceImpl = serviceImpl;
      this.methodId = methodId;
    }

    @java.lang.Override
    @java.lang.SuppressWarnings("unchecked")
    public void invoke(Req request, io.grpc.stub.StreamObserver<Resp> responseObserver) {
      switch (methodId) {
        case METHODID_CREATE_SEQUENCE:
          serviceImpl.createSequence((com.ltv.aerospike.api.proto.CreateSequenceServices.CreateSequenceRequest) request,
              (io.grpc.stub.StreamObserver<com.ltv.aerospike.api.proto.CreateSequenceServices.CreateSequenceResponse>) responseObserver);
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

  private static abstract class CreateSequenceServiceBaseDescriptorSupplier
      implements io.grpc.protobuf.ProtoFileDescriptorSupplier, io.grpc.protobuf.ProtoServiceDescriptorSupplier {
    CreateSequenceServiceBaseDescriptorSupplier() {}

    @java.lang.Override
    public com.google.protobuf.Descriptors.FileDescriptor getFileDescriptor() {
      return com.ltv.aerospike.api.proto.CreateSequenceServices.getDescriptor();
    }

    @java.lang.Override
    public com.google.protobuf.Descriptors.ServiceDescriptor getServiceDescriptor() {
      return getFileDescriptor().findServiceByName("CreateSequenceService");
    }
  }

  private static final class CreateSequenceServiceFileDescriptorSupplier
      extends CreateSequenceServiceBaseDescriptorSupplier {
    CreateSequenceServiceFileDescriptorSupplier() {}
  }

  private static final class CreateSequenceServiceMethodDescriptorSupplier
      extends CreateSequenceServiceBaseDescriptorSupplier
      implements io.grpc.protobuf.ProtoMethodDescriptorSupplier {
    private final String methodName;

    CreateSequenceServiceMethodDescriptorSupplier(String methodName) {
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
      synchronized (CreateSequenceServiceGrpc.class) {
        result = serviceDescriptor;
        if (result == null) {
          serviceDescriptor = result = io.grpc.ServiceDescriptor.newBuilder(SERVICE_NAME)
              .setSchemaDescriptor(new CreateSequenceServiceFileDescriptorSupplier())
              .addMethod(getCreateSequenceMethod())
              .build();
        }
      }
    }
    return result;
  }
}
