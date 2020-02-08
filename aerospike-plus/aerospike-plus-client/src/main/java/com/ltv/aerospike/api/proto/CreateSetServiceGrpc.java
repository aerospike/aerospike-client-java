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
    comments = "Source: CreateSetServices.proto")
public final class CreateSetServiceGrpc {

  private CreateSetServiceGrpc() {}

  public static final String SERVICE_NAME = "com.ltv.aerospike.api.proto.CreateSetService";

  // Static method descriptors that strictly reflect the proto.
  private static volatile io.grpc.MethodDescriptor<com.ltv.aerospike.api.proto.CreateSetServices.CreateSetRequest,
      com.ltv.aerospike.api.proto.CreateSetServices.CreateSetResponse> getCreateSetMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "createSet",
      requestType = com.ltv.aerospike.api.proto.CreateSetServices.CreateSetRequest.class,
      responseType = com.ltv.aerospike.api.proto.CreateSetServices.CreateSetResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.ltv.aerospike.api.proto.CreateSetServices.CreateSetRequest,
      com.ltv.aerospike.api.proto.CreateSetServices.CreateSetResponse> getCreateSetMethod() {
    io.grpc.MethodDescriptor<com.ltv.aerospike.api.proto.CreateSetServices.CreateSetRequest, com.ltv.aerospike.api.proto.CreateSetServices.CreateSetResponse> getCreateSetMethod;
    if ((getCreateSetMethod = CreateSetServiceGrpc.getCreateSetMethod) == null) {
      synchronized (CreateSetServiceGrpc.class) {
        if ((getCreateSetMethod = CreateSetServiceGrpc.getCreateSetMethod) == null) {
          CreateSetServiceGrpc.getCreateSetMethod = getCreateSetMethod =
              io.grpc.MethodDescriptor.<com.ltv.aerospike.api.proto.CreateSetServices.CreateSetRequest, com.ltv.aerospike.api.proto.CreateSetServices.CreateSetResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "createSet"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.ltv.aerospike.api.proto.CreateSetServices.CreateSetRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.ltv.aerospike.api.proto.CreateSetServices.CreateSetResponse.getDefaultInstance()))
              .setSchemaDescriptor(new CreateSetServiceMethodDescriptorSupplier("createSet"))
              .build();
        }
      }
    }
    return getCreateSetMethod;
  }

  /**
   * Creates a new async stub that supports all call types for the service
   */
  public static CreateSetServiceStub newStub(io.grpc.Channel channel) {
    return new CreateSetServiceStub(channel);
  }

  /**
   * Creates a new blocking-style stub that supports unary and streaming output calls on the service
   */
  public static CreateSetServiceBlockingStub newBlockingStub(
      io.grpc.Channel channel) {
    return new CreateSetServiceBlockingStub(channel);
  }

  /**
   * Creates a new ListenableFuture-style stub that supports unary calls on the service
   */
  public static CreateSetServiceFutureStub newFutureStub(
      io.grpc.Channel channel) {
    return new CreateSetServiceFutureStub(channel);
  }

  /**
   */
  public static abstract class CreateSetServiceImplBase implements io.grpc.BindableService {

    /**
     */
    public void createSet(com.ltv.aerospike.api.proto.CreateSetServices.CreateSetRequest request,
        io.grpc.stub.StreamObserver<com.ltv.aerospike.api.proto.CreateSetServices.CreateSetResponse> responseObserver) {
      asyncUnimplementedUnaryCall(getCreateSetMethod(), responseObserver);
    }

    @java.lang.Override public final io.grpc.ServerServiceDefinition bindService() {
      return io.grpc.ServerServiceDefinition.builder(getServiceDescriptor())
          .addMethod(
            getCreateSetMethod(),
            asyncUnaryCall(
              new MethodHandlers<
                com.ltv.aerospike.api.proto.CreateSetServices.CreateSetRequest,
                com.ltv.aerospike.api.proto.CreateSetServices.CreateSetResponse>(
                  this, METHODID_CREATE_SET)))
          .build();
    }
  }

  /**
   */
  public static final class CreateSetServiceStub extends io.grpc.stub.AbstractStub<CreateSetServiceStub> {
    private CreateSetServiceStub(io.grpc.Channel channel) {
      super(channel);
    }

    private CreateSetServiceStub(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected CreateSetServiceStub build(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      return new CreateSetServiceStub(channel, callOptions);
    }

    /**
     */
    public void createSet(com.ltv.aerospike.api.proto.CreateSetServices.CreateSetRequest request,
        io.grpc.stub.StreamObserver<com.ltv.aerospike.api.proto.CreateSetServices.CreateSetResponse> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(getCreateSetMethod(), getCallOptions()), request, responseObserver);
    }
  }

  /**
   */
  public static final class CreateSetServiceBlockingStub extends io.grpc.stub.AbstractStub<CreateSetServiceBlockingStub> {
    private CreateSetServiceBlockingStub(io.grpc.Channel channel) {
      super(channel);
    }

    private CreateSetServiceBlockingStub(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected CreateSetServiceBlockingStub build(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      return new CreateSetServiceBlockingStub(channel, callOptions);
    }

    /**
     */
    public com.ltv.aerospike.api.proto.CreateSetServices.CreateSetResponse createSet(com.ltv.aerospike.api.proto.CreateSetServices.CreateSetRequest request) {
      return blockingUnaryCall(
          getChannel(), getCreateSetMethod(), getCallOptions(), request);
    }
  }

  /**
   */
  public static final class CreateSetServiceFutureStub extends io.grpc.stub.AbstractStub<CreateSetServiceFutureStub> {
    private CreateSetServiceFutureStub(io.grpc.Channel channel) {
      super(channel);
    }

    private CreateSetServiceFutureStub(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected CreateSetServiceFutureStub build(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      return new CreateSetServiceFutureStub(channel, callOptions);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<com.ltv.aerospike.api.proto.CreateSetServices.CreateSetResponse> createSet(
        com.ltv.aerospike.api.proto.CreateSetServices.CreateSetRequest request) {
      return futureUnaryCall(
          getChannel().newCall(getCreateSetMethod(), getCallOptions()), request);
    }
  }

  private static final int METHODID_CREATE_SET = 0;

  private static final class MethodHandlers<Req, Resp> implements
      io.grpc.stub.ServerCalls.UnaryMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.ServerStreamingMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.ClientStreamingMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.BidiStreamingMethod<Req, Resp> {
    private final CreateSetServiceImplBase serviceImpl;
    private final int methodId;

    MethodHandlers(CreateSetServiceImplBase serviceImpl, int methodId) {
      this.serviceImpl = serviceImpl;
      this.methodId = methodId;
    }

    @java.lang.Override
    @java.lang.SuppressWarnings("unchecked")
    public void invoke(Req request, io.grpc.stub.StreamObserver<Resp> responseObserver) {
      switch (methodId) {
        case METHODID_CREATE_SET:
          serviceImpl.createSet((com.ltv.aerospike.api.proto.CreateSetServices.CreateSetRequest) request,
              (io.grpc.stub.StreamObserver<com.ltv.aerospike.api.proto.CreateSetServices.CreateSetResponse>) responseObserver);
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

  private static abstract class CreateSetServiceBaseDescriptorSupplier
      implements io.grpc.protobuf.ProtoFileDescriptorSupplier, io.grpc.protobuf.ProtoServiceDescriptorSupplier {
    CreateSetServiceBaseDescriptorSupplier() {}

    @java.lang.Override
    public com.google.protobuf.Descriptors.FileDescriptor getFileDescriptor() {
      return com.ltv.aerospike.api.proto.CreateSetServices.getDescriptor();
    }

    @java.lang.Override
    public com.google.protobuf.Descriptors.ServiceDescriptor getServiceDescriptor() {
      return getFileDescriptor().findServiceByName("CreateSetService");
    }
  }

  private static final class CreateSetServiceFileDescriptorSupplier
      extends CreateSetServiceBaseDescriptorSupplier {
    CreateSetServiceFileDescriptorSupplier() {}
  }

  private static final class CreateSetServiceMethodDescriptorSupplier
      extends CreateSetServiceBaseDescriptorSupplier
      implements io.grpc.protobuf.ProtoMethodDescriptorSupplier {
    private final String methodName;

    CreateSetServiceMethodDescriptorSupplier(String methodName) {
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
      synchronized (CreateSetServiceGrpc.class) {
        result = serviceDescriptor;
        if (result == null) {
          serviceDescriptor = result = io.grpc.ServiceDescriptor.newBuilder(SERVICE_NAME)
              .setSchemaDescriptor(new CreateSetServiceFileDescriptorSupplier())
              .addMethod(getCreateSetMethod())
              .build();
        }
      }
    }
    return result;
  }
}
