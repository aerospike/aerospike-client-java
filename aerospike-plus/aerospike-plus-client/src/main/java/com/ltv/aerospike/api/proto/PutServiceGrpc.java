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
    comments = "Source: PutServices.proto")
public final class PutServiceGrpc {

  private PutServiceGrpc() {}

  public static final String SERVICE_NAME = "com.ltv.aerospike.api.proto.PutService";

  // Static method descriptors that strictly reflect the proto.
  private static volatile io.grpc.MethodDescriptor<com.ltv.aerospike.api.proto.PutServices.PutRequest,
      com.ltv.aerospike.api.proto.PutServices.PutResponse> getPutMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "put",
      requestType = com.ltv.aerospike.api.proto.PutServices.PutRequest.class,
      responseType = com.ltv.aerospike.api.proto.PutServices.PutResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.ltv.aerospike.api.proto.PutServices.PutRequest,
      com.ltv.aerospike.api.proto.PutServices.PutResponse> getPutMethod() {
    io.grpc.MethodDescriptor<com.ltv.aerospike.api.proto.PutServices.PutRequest, com.ltv.aerospike.api.proto.PutServices.PutResponse> getPutMethod;
    if ((getPutMethod = PutServiceGrpc.getPutMethod) == null) {
      synchronized (PutServiceGrpc.class) {
        if ((getPutMethod = PutServiceGrpc.getPutMethod) == null) {
          PutServiceGrpc.getPutMethod = getPutMethod =
              io.grpc.MethodDescriptor.<com.ltv.aerospike.api.proto.PutServices.PutRequest, com.ltv.aerospike.api.proto.PutServices.PutResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "put"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.ltv.aerospike.api.proto.PutServices.PutRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.ltv.aerospike.api.proto.PutServices.PutResponse.getDefaultInstance()))
              .setSchemaDescriptor(new PutServiceMethodDescriptorSupplier("put"))
              .build();
        }
      }
    }
    return getPutMethod;
  }

  /**
   * Creates a new async stub that supports all call types for the service
   */
  public static PutServiceStub newStub(io.grpc.Channel channel) {
    return new PutServiceStub(channel);
  }

  /**
   * Creates a new blocking-style stub that supports unary and streaming output calls on the service
   */
  public static PutServiceBlockingStub newBlockingStub(
      io.grpc.Channel channel) {
    return new PutServiceBlockingStub(channel);
  }

  /**
   * Creates a new ListenableFuture-style stub that supports unary calls on the service
   */
  public static PutServiceFutureStub newFutureStub(
      io.grpc.Channel channel) {
    return new PutServiceFutureStub(channel);
  }

  /**
   */
  public static abstract class PutServiceImplBase implements io.grpc.BindableService {

    /**
     */
    public void put(com.ltv.aerospike.api.proto.PutServices.PutRequest request,
        io.grpc.stub.StreamObserver<com.ltv.aerospike.api.proto.PutServices.PutResponse> responseObserver) {
      asyncUnimplementedUnaryCall(getPutMethod(), responseObserver);
    }

    @java.lang.Override public final io.grpc.ServerServiceDefinition bindService() {
      return io.grpc.ServerServiceDefinition.builder(getServiceDescriptor())
          .addMethod(
            getPutMethod(),
            asyncUnaryCall(
              new MethodHandlers<
                com.ltv.aerospike.api.proto.PutServices.PutRequest,
                com.ltv.aerospike.api.proto.PutServices.PutResponse>(
                  this, METHODID_PUT)))
          .build();
    }
  }

  /**
   */
  public static final class PutServiceStub extends io.grpc.stub.AbstractStub<PutServiceStub> {
    private PutServiceStub(io.grpc.Channel channel) {
      super(channel);
    }

    private PutServiceStub(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected PutServiceStub build(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      return new PutServiceStub(channel, callOptions);
    }

    /**
     */
    public void put(com.ltv.aerospike.api.proto.PutServices.PutRequest request,
        io.grpc.stub.StreamObserver<com.ltv.aerospike.api.proto.PutServices.PutResponse> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(getPutMethod(), getCallOptions()), request, responseObserver);
    }
  }

  /**
   */
  public static final class PutServiceBlockingStub extends io.grpc.stub.AbstractStub<PutServiceBlockingStub> {
    private PutServiceBlockingStub(io.grpc.Channel channel) {
      super(channel);
    }

    private PutServiceBlockingStub(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected PutServiceBlockingStub build(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      return new PutServiceBlockingStub(channel, callOptions);
    }

    /**
     */
    public com.ltv.aerospike.api.proto.PutServices.PutResponse put(com.ltv.aerospike.api.proto.PutServices.PutRequest request) {
      return blockingUnaryCall(
          getChannel(), getPutMethod(), getCallOptions(), request);
    }
  }

  /**
   */
  public static final class PutServiceFutureStub extends io.grpc.stub.AbstractStub<PutServiceFutureStub> {
    private PutServiceFutureStub(io.grpc.Channel channel) {
      super(channel);
    }

    private PutServiceFutureStub(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected PutServiceFutureStub build(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      return new PutServiceFutureStub(channel, callOptions);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<com.ltv.aerospike.api.proto.PutServices.PutResponse> put(
        com.ltv.aerospike.api.proto.PutServices.PutRequest request) {
      return futureUnaryCall(
          getChannel().newCall(getPutMethod(), getCallOptions()), request);
    }
  }

  private static final int METHODID_PUT = 0;

  private static final class MethodHandlers<Req, Resp> implements
      io.grpc.stub.ServerCalls.UnaryMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.ServerStreamingMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.ClientStreamingMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.BidiStreamingMethod<Req, Resp> {
    private final PutServiceImplBase serviceImpl;
    private final int methodId;

    MethodHandlers(PutServiceImplBase serviceImpl, int methodId) {
      this.serviceImpl = serviceImpl;
      this.methodId = methodId;
    }

    @java.lang.Override
    @java.lang.SuppressWarnings("unchecked")
    public void invoke(Req request, io.grpc.stub.StreamObserver<Resp> responseObserver) {
      switch (methodId) {
        case METHODID_PUT:
          serviceImpl.put((com.ltv.aerospike.api.proto.PutServices.PutRequest) request,
              (io.grpc.stub.StreamObserver<com.ltv.aerospike.api.proto.PutServices.PutResponse>) responseObserver);
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

  private static abstract class PutServiceBaseDescriptorSupplier
      implements io.grpc.protobuf.ProtoFileDescriptorSupplier, io.grpc.protobuf.ProtoServiceDescriptorSupplier {
    PutServiceBaseDescriptorSupplier() {}

    @java.lang.Override
    public com.google.protobuf.Descriptors.FileDescriptor getFileDescriptor() {
      return com.ltv.aerospike.api.proto.PutServices.getDescriptor();
    }

    @java.lang.Override
    public com.google.protobuf.Descriptors.ServiceDescriptor getServiceDescriptor() {
      return getFileDescriptor().findServiceByName("PutService");
    }
  }

  private static final class PutServiceFileDescriptorSupplier
      extends PutServiceBaseDescriptorSupplier {
    PutServiceFileDescriptorSupplier() {}
  }

  private static final class PutServiceMethodDescriptorSupplier
      extends PutServiceBaseDescriptorSupplier
      implements io.grpc.protobuf.ProtoMethodDescriptorSupplier {
    private final String methodName;

    PutServiceMethodDescriptorSupplier(String methodName) {
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
      synchronized (PutServiceGrpc.class) {
        result = serviceDescriptor;
        if (result == null) {
          serviceDescriptor = result = io.grpc.ServiceDescriptor.newBuilder(SERVICE_NAME)
              .setSchemaDescriptor(new PutServiceFileDescriptorSupplier())
              .addMethod(getPutMethod())
              .build();
        }
      }
    }
    return result;
  }
}
