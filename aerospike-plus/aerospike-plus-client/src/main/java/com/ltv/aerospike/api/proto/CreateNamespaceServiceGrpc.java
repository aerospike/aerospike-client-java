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
    comments = "Source: CreateNamespaceServices.proto")
public final class CreateNamespaceServiceGrpc {

  private CreateNamespaceServiceGrpc() {}

  public static final String SERVICE_NAME = "com.ltv.aerospike.api.proto.CreateNamespaceService";

  // Static method descriptors that strictly reflect the proto.
  private static volatile io.grpc.MethodDescriptor<com.ltv.aerospike.api.proto.CreateNamespaceServices.CreateNamespaceRequest,
      com.ltv.aerospike.api.proto.CreateNamespaceServices.CreateNamespaceResponse> getCreateNamespaceMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "createNamespace",
      requestType = com.ltv.aerospike.api.proto.CreateNamespaceServices.CreateNamespaceRequest.class,
      responseType = com.ltv.aerospike.api.proto.CreateNamespaceServices.CreateNamespaceResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.ltv.aerospike.api.proto.CreateNamespaceServices.CreateNamespaceRequest,
      com.ltv.aerospike.api.proto.CreateNamespaceServices.CreateNamespaceResponse> getCreateNamespaceMethod() {
    io.grpc.MethodDescriptor<com.ltv.aerospike.api.proto.CreateNamespaceServices.CreateNamespaceRequest, com.ltv.aerospike.api.proto.CreateNamespaceServices.CreateNamespaceResponse> getCreateNamespaceMethod;
    if ((getCreateNamespaceMethod = CreateNamespaceServiceGrpc.getCreateNamespaceMethod) == null) {
      synchronized (CreateNamespaceServiceGrpc.class) {
        if ((getCreateNamespaceMethod = CreateNamespaceServiceGrpc.getCreateNamespaceMethod) == null) {
          CreateNamespaceServiceGrpc.getCreateNamespaceMethod = getCreateNamespaceMethod =
              io.grpc.MethodDescriptor.<com.ltv.aerospike.api.proto.CreateNamespaceServices.CreateNamespaceRequest, com.ltv.aerospike.api.proto.CreateNamespaceServices.CreateNamespaceResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "createNamespace"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.ltv.aerospike.api.proto.CreateNamespaceServices.CreateNamespaceRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.ltv.aerospike.api.proto.CreateNamespaceServices.CreateNamespaceResponse.getDefaultInstance()))
              .setSchemaDescriptor(new CreateNamespaceServiceMethodDescriptorSupplier("createNamespace"))
              .build();
        }
      }
    }
    return getCreateNamespaceMethod;
  }

  /**
   * Creates a new async stub that supports all call types for the service
   */
  public static CreateNamespaceServiceStub newStub(io.grpc.Channel channel) {
    return new CreateNamespaceServiceStub(channel);
  }

  /**
   * Creates a new blocking-style stub that supports unary and streaming output calls on the service
   */
  public static CreateNamespaceServiceBlockingStub newBlockingStub(
      io.grpc.Channel channel) {
    return new CreateNamespaceServiceBlockingStub(channel);
  }

  /**
   * Creates a new ListenableFuture-style stub that supports unary calls on the service
   */
  public static CreateNamespaceServiceFutureStub newFutureStub(
      io.grpc.Channel channel) {
    return new CreateNamespaceServiceFutureStub(channel);
  }

  /**
   */
  public static abstract class CreateNamespaceServiceImplBase implements io.grpc.BindableService {

    /**
     */
    public void createNamespace(com.ltv.aerospike.api.proto.CreateNamespaceServices.CreateNamespaceRequest request,
        io.grpc.stub.StreamObserver<com.ltv.aerospike.api.proto.CreateNamespaceServices.CreateNamespaceResponse> responseObserver) {
      asyncUnimplementedUnaryCall(getCreateNamespaceMethod(), responseObserver);
    }

    @java.lang.Override public final io.grpc.ServerServiceDefinition bindService() {
      return io.grpc.ServerServiceDefinition.builder(getServiceDescriptor())
          .addMethod(
            getCreateNamespaceMethod(),
            asyncUnaryCall(
              new MethodHandlers<
                com.ltv.aerospike.api.proto.CreateNamespaceServices.CreateNamespaceRequest,
                com.ltv.aerospike.api.proto.CreateNamespaceServices.CreateNamespaceResponse>(
                  this, METHODID_CREATE_NAMESPACE)))
          .build();
    }
  }

  /**
   */
  public static final class CreateNamespaceServiceStub extends io.grpc.stub.AbstractStub<CreateNamespaceServiceStub> {
    private CreateNamespaceServiceStub(io.grpc.Channel channel) {
      super(channel);
    }

    private CreateNamespaceServiceStub(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected CreateNamespaceServiceStub build(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      return new CreateNamespaceServiceStub(channel, callOptions);
    }

    /**
     */
    public void createNamespace(com.ltv.aerospike.api.proto.CreateNamespaceServices.CreateNamespaceRequest request,
        io.grpc.stub.StreamObserver<com.ltv.aerospike.api.proto.CreateNamespaceServices.CreateNamespaceResponse> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(getCreateNamespaceMethod(), getCallOptions()), request, responseObserver);
    }
  }

  /**
   */
  public static final class CreateNamespaceServiceBlockingStub extends io.grpc.stub.AbstractStub<CreateNamespaceServiceBlockingStub> {
    private CreateNamespaceServiceBlockingStub(io.grpc.Channel channel) {
      super(channel);
    }

    private CreateNamespaceServiceBlockingStub(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected CreateNamespaceServiceBlockingStub build(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      return new CreateNamespaceServiceBlockingStub(channel, callOptions);
    }

    /**
     */
    public com.ltv.aerospike.api.proto.CreateNamespaceServices.CreateNamespaceResponse createNamespace(com.ltv.aerospike.api.proto.CreateNamespaceServices.CreateNamespaceRequest request) {
      return blockingUnaryCall(
          getChannel(), getCreateNamespaceMethod(), getCallOptions(), request);
    }
  }

  /**
   */
  public static final class CreateNamespaceServiceFutureStub extends io.grpc.stub.AbstractStub<CreateNamespaceServiceFutureStub> {
    private CreateNamespaceServiceFutureStub(io.grpc.Channel channel) {
      super(channel);
    }

    private CreateNamespaceServiceFutureStub(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected CreateNamespaceServiceFutureStub build(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      return new CreateNamespaceServiceFutureStub(channel, callOptions);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<com.ltv.aerospike.api.proto.CreateNamespaceServices.CreateNamespaceResponse> createNamespace(
        com.ltv.aerospike.api.proto.CreateNamespaceServices.CreateNamespaceRequest request) {
      return futureUnaryCall(
          getChannel().newCall(getCreateNamespaceMethod(), getCallOptions()), request);
    }
  }

  private static final int METHODID_CREATE_NAMESPACE = 0;

  private static final class MethodHandlers<Req, Resp> implements
      io.grpc.stub.ServerCalls.UnaryMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.ServerStreamingMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.ClientStreamingMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.BidiStreamingMethod<Req, Resp> {
    private final CreateNamespaceServiceImplBase serviceImpl;
    private final int methodId;

    MethodHandlers(CreateNamespaceServiceImplBase serviceImpl, int methodId) {
      this.serviceImpl = serviceImpl;
      this.methodId = methodId;
    }

    @java.lang.Override
    @java.lang.SuppressWarnings("unchecked")
    public void invoke(Req request, io.grpc.stub.StreamObserver<Resp> responseObserver) {
      switch (methodId) {
        case METHODID_CREATE_NAMESPACE:
          serviceImpl.createNamespace((com.ltv.aerospike.api.proto.CreateNamespaceServices.CreateNamespaceRequest) request,
              (io.grpc.stub.StreamObserver<com.ltv.aerospike.api.proto.CreateNamespaceServices.CreateNamespaceResponse>) responseObserver);
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

  private static abstract class CreateNamespaceServiceBaseDescriptorSupplier
      implements io.grpc.protobuf.ProtoFileDescriptorSupplier, io.grpc.protobuf.ProtoServiceDescriptorSupplier {
    CreateNamespaceServiceBaseDescriptorSupplier() {}

    @java.lang.Override
    public com.google.protobuf.Descriptors.FileDescriptor getFileDescriptor() {
      return com.ltv.aerospike.api.proto.CreateNamespaceServices.getDescriptor();
    }

    @java.lang.Override
    public com.google.protobuf.Descriptors.ServiceDescriptor getServiceDescriptor() {
      return getFileDescriptor().findServiceByName("CreateNamespaceService");
    }
  }

  private static final class CreateNamespaceServiceFileDescriptorSupplier
      extends CreateNamespaceServiceBaseDescriptorSupplier {
    CreateNamespaceServiceFileDescriptorSupplier() {}
  }

  private static final class CreateNamespaceServiceMethodDescriptorSupplier
      extends CreateNamespaceServiceBaseDescriptorSupplier
      implements io.grpc.protobuf.ProtoMethodDescriptorSupplier {
    private final String methodName;

    CreateNamespaceServiceMethodDescriptorSupplier(String methodName) {
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
      synchronized (CreateNamespaceServiceGrpc.class) {
        result = serviceDescriptor;
        if (result == null) {
          serviceDescriptor = result = io.grpc.ServiceDescriptor.newBuilder(SERVICE_NAME)
              .setSchemaDescriptor(new CreateNamespaceServiceFileDescriptorSupplier())
              .addMethod(getCreateNamespaceMethod())
              .build();
        }
      }
    }
    return result;
  }
}
