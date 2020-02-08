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
    comments = "Source: GetServices.proto")
public final class GetServiceGrpc {

  private GetServiceGrpc() {}

  public static final String SERVICE_NAME = "com.ltv.aerospike.api.proto.GetService";

  // Static method descriptors that strictly reflect the proto.
  private static volatile io.grpc.MethodDescriptor<com.ltv.aerospike.api.proto.GetServices.GetRequest,
      com.ltv.aerospike.api.proto.GetServices.GetResponse> getGetMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "get",
      requestType = com.ltv.aerospike.api.proto.GetServices.GetRequest.class,
      responseType = com.ltv.aerospike.api.proto.GetServices.GetResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.ltv.aerospike.api.proto.GetServices.GetRequest,
      com.ltv.aerospike.api.proto.GetServices.GetResponse> getGetMethod() {
    io.grpc.MethodDescriptor<com.ltv.aerospike.api.proto.GetServices.GetRequest, com.ltv.aerospike.api.proto.GetServices.GetResponse> getGetMethod;
    if ((getGetMethod = GetServiceGrpc.getGetMethod) == null) {
      synchronized (GetServiceGrpc.class) {
        if ((getGetMethod = GetServiceGrpc.getGetMethod) == null) {
          GetServiceGrpc.getGetMethod = getGetMethod =
              io.grpc.MethodDescriptor.<com.ltv.aerospike.api.proto.GetServices.GetRequest, com.ltv.aerospike.api.proto.GetServices.GetResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "get"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.ltv.aerospike.api.proto.GetServices.GetRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.ltv.aerospike.api.proto.GetServices.GetResponse.getDefaultInstance()))
              .setSchemaDescriptor(new GetServiceMethodDescriptorSupplier("get"))
              .build();
        }
      }
    }
    return getGetMethod;
  }

  /**
   * Creates a new async stub that supports all call types for the service
   */
  public static GetServiceStub newStub(io.grpc.Channel channel) {
    return new GetServiceStub(channel);
  }

  /**
   * Creates a new blocking-style stub that supports unary and streaming output calls on the service
   */
  public static GetServiceBlockingStub newBlockingStub(
      io.grpc.Channel channel) {
    return new GetServiceBlockingStub(channel);
  }

  /**
   * Creates a new ListenableFuture-style stub that supports unary calls on the service
   */
  public static GetServiceFutureStub newFutureStub(
      io.grpc.Channel channel) {
    return new GetServiceFutureStub(channel);
  }

  /**
   */
  public static abstract class GetServiceImplBase implements io.grpc.BindableService {

    /**
     */
    public void get(com.ltv.aerospike.api.proto.GetServices.GetRequest request,
        io.grpc.stub.StreamObserver<com.ltv.aerospike.api.proto.GetServices.GetResponse> responseObserver) {
      asyncUnimplementedUnaryCall(getGetMethod(), responseObserver);
    }

    @java.lang.Override public final io.grpc.ServerServiceDefinition bindService() {
      return io.grpc.ServerServiceDefinition.builder(getServiceDescriptor())
          .addMethod(
            getGetMethod(),
            asyncUnaryCall(
              new MethodHandlers<
                com.ltv.aerospike.api.proto.GetServices.GetRequest,
                com.ltv.aerospike.api.proto.GetServices.GetResponse>(
                  this, METHODID_GET)))
          .build();
    }
  }

  /**
   */
  public static final class GetServiceStub extends io.grpc.stub.AbstractStub<GetServiceStub> {
    private GetServiceStub(io.grpc.Channel channel) {
      super(channel);
    }

    private GetServiceStub(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected GetServiceStub build(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      return new GetServiceStub(channel, callOptions);
    }

    /**
     */
    public void get(com.ltv.aerospike.api.proto.GetServices.GetRequest request,
        io.grpc.stub.StreamObserver<com.ltv.aerospike.api.proto.GetServices.GetResponse> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(getGetMethod(), getCallOptions()), request, responseObserver);
    }
  }

  /**
   */
  public static final class GetServiceBlockingStub extends io.grpc.stub.AbstractStub<GetServiceBlockingStub> {
    private GetServiceBlockingStub(io.grpc.Channel channel) {
      super(channel);
    }

    private GetServiceBlockingStub(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected GetServiceBlockingStub build(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      return new GetServiceBlockingStub(channel, callOptions);
    }

    /**
     */
    public com.ltv.aerospike.api.proto.GetServices.GetResponse get(com.ltv.aerospike.api.proto.GetServices.GetRequest request) {
      return blockingUnaryCall(
          getChannel(), getGetMethod(), getCallOptions(), request);
    }
  }

  /**
   */
  public static final class GetServiceFutureStub extends io.grpc.stub.AbstractStub<GetServiceFutureStub> {
    private GetServiceFutureStub(io.grpc.Channel channel) {
      super(channel);
    }

    private GetServiceFutureStub(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected GetServiceFutureStub build(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      return new GetServiceFutureStub(channel, callOptions);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<com.ltv.aerospike.api.proto.GetServices.GetResponse> get(
        com.ltv.aerospike.api.proto.GetServices.GetRequest request) {
      return futureUnaryCall(
          getChannel().newCall(getGetMethod(), getCallOptions()), request);
    }
  }

  private static final int METHODID_GET = 0;

  private static final class MethodHandlers<Req, Resp> implements
      io.grpc.stub.ServerCalls.UnaryMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.ServerStreamingMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.ClientStreamingMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.BidiStreamingMethod<Req, Resp> {
    private final GetServiceImplBase serviceImpl;
    private final int methodId;

    MethodHandlers(GetServiceImplBase serviceImpl, int methodId) {
      this.serviceImpl = serviceImpl;
      this.methodId = methodId;
    }

    @java.lang.Override
    @java.lang.SuppressWarnings("unchecked")
    public void invoke(Req request, io.grpc.stub.StreamObserver<Resp> responseObserver) {
      switch (methodId) {
        case METHODID_GET:
          serviceImpl.get((com.ltv.aerospike.api.proto.GetServices.GetRequest) request,
              (io.grpc.stub.StreamObserver<com.ltv.aerospike.api.proto.GetServices.GetResponse>) responseObserver);
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

  private static abstract class GetServiceBaseDescriptorSupplier
      implements io.grpc.protobuf.ProtoFileDescriptorSupplier, io.grpc.protobuf.ProtoServiceDescriptorSupplier {
    GetServiceBaseDescriptorSupplier() {}

    @java.lang.Override
    public com.google.protobuf.Descriptors.FileDescriptor getFileDescriptor() {
      return com.ltv.aerospike.api.proto.GetServices.getDescriptor();
    }

    @java.lang.Override
    public com.google.protobuf.Descriptors.ServiceDescriptor getServiceDescriptor() {
      return getFileDescriptor().findServiceByName("GetService");
    }
  }

  private static final class GetServiceFileDescriptorSupplier
      extends GetServiceBaseDescriptorSupplier {
    GetServiceFileDescriptorSupplier() {}
  }

  private static final class GetServiceMethodDescriptorSupplier
      extends GetServiceBaseDescriptorSupplier
      implements io.grpc.protobuf.ProtoMethodDescriptorSupplier {
    private final String methodName;

    GetServiceMethodDescriptorSupplier(String methodName) {
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
      synchronized (GetServiceGrpc.class) {
        result = serviceDescriptor;
        if (result == null) {
          serviceDescriptor = result = io.grpc.ServiceDescriptor.newBuilder(SERVICE_NAME)
              .setSchemaDescriptor(new GetServiceFileDescriptorSupplier())
              .addMethod(getGetMethod())
              .build();
        }
      }
    }
    return result;
  }
}
