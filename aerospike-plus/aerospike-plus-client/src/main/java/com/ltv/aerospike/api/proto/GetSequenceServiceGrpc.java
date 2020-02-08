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
    comments = "Source: GetSequenceServices.proto")
public final class GetSequenceServiceGrpc {

  private GetSequenceServiceGrpc() {}

  public static final String SERVICE_NAME = "com.ltv.aerospike.api.proto.GetSequenceService";

  // Static method descriptors that strictly reflect the proto.
  private static volatile io.grpc.MethodDescriptor<com.ltv.aerospike.api.proto.GetSequenceServices.GetSequenceRequest,
      com.ltv.aerospike.api.proto.GetSequenceServices.GetSequenceResponse> getGetSequenceMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "getSequence",
      requestType = com.ltv.aerospike.api.proto.GetSequenceServices.GetSequenceRequest.class,
      responseType = com.ltv.aerospike.api.proto.GetSequenceServices.GetSequenceResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.ltv.aerospike.api.proto.GetSequenceServices.GetSequenceRequest,
      com.ltv.aerospike.api.proto.GetSequenceServices.GetSequenceResponse> getGetSequenceMethod() {
    io.grpc.MethodDescriptor<com.ltv.aerospike.api.proto.GetSequenceServices.GetSequenceRequest, com.ltv.aerospike.api.proto.GetSequenceServices.GetSequenceResponse> getGetSequenceMethod;
    if ((getGetSequenceMethod = GetSequenceServiceGrpc.getGetSequenceMethod) == null) {
      synchronized (GetSequenceServiceGrpc.class) {
        if ((getGetSequenceMethod = GetSequenceServiceGrpc.getGetSequenceMethod) == null) {
          GetSequenceServiceGrpc.getGetSequenceMethod = getGetSequenceMethod =
              io.grpc.MethodDescriptor.<com.ltv.aerospike.api.proto.GetSequenceServices.GetSequenceRequest, com.ltv.aerospike.api.proto.GetSequenceServices.GetSequenceResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "getSequence"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.ltv.aerospike.api.proto.GetSequenceServices.GetSequenceRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.ltv.aerospike.api.proto.GetSequenceServices.GetSequenceResponse.getDefaultInstance()))
              .setSchemaDescriptor(new GetSequenceServiceMethodDescriptorSupplier("getSequence"))
              .build();
        }
      }
    }
    return getGetSequenceMethod;
  }

  /**
   * Creates a new async stub that supports all call types for the service
   */
  public static GetSequenceServiceStub newStub(io.grpc.Channel channel) {
    return new GetSequenceServiceStub(channel);
  }

  /**
   * Creates a new blocking-style stub that supports unary and streaming output calls on the service
   */
  public static GetSequenceServiceBlockingStub newBlockingStub(
      io.grpc.Channel channel) {
    return new GetSequenceServiceBlockingStub(channel);
  }

  /**
   * Creates a new ListenableFuture-style stub that supports unary calls on the service
   */
  public static GetSequenceServiceFutureStub newFutureStub(
      io.grpc.Channel channel) {
    return new GetSequenceServiceFutureStub(channel);
  }

  /**
   */
  public static abstract class GetSequenceServiceImplBase implements io.grpc.BindableService {

    /**
     */
    public void getSequence(com.ltv.aerospike.api.proto.GetSequenceServices.GetSequenceRequest request,
        io.grpc.stub.StreamObserver<com.ltv.aerospike.api.proto.GetSequenceServices.GetSequenceResponse> responseObserver) {
      asyncUnimplementedUnaryCall(getGetSequenceMethod(), responseObserver);
    }

    @java.lang.Override public final io.grpc.ServerServiceDefinition bindService() {
      return io.grpc.ServerServiceDefinition.builder(getServiceDescriptor())
          .addMethod(
            getGetSequenceMethod(),
            asyncUnaryCall(
              new MethodHandlers<
                com.ltv.aerospike.api.proto.GetSequenceServices.GetSequenceRequest,
                com.ltv.aerospike.api.proto.GetSequenceServices.GetSequenceResponse>(
                  this, METHODID_GET_SEQUENCE)))
          .build();
    }
  }

  /**
   */
  public static final class GetSequenceServiceStub extends io.grpc.stub.AbstractStub<GetSequenceServiceStub> {
    private GetSequenceServiceStub(io.grpc.Channel channel) {
      super(channel);
    }

    private GetSequenceServiceStub(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected GetSequenceServiceStub build(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      return new GetSequenceServiceStub(channel, callOptions);
    }

    /**
     */
    public void getSequence(com.ltv.aerospike.api.proto.GetSequenceServices.GetSequenceRequest request,
        io.grpc.stub.StreamObserver<com.ltv.aerospike.api.proto.GetSequenceServices.GetSequenceResponse> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(getGetSequenceMethod(), getCallOptions()), request, responseObserver);
    }
  }

  /**
   */
  public static final class GetSequenceServiceBlockingStub extends io.grpc.stub.AbstractStub<GetSequenceServiceBlockingStub> {
    private GetSequenceServiceBlockingStub(io.grpc.Channel channel) {
      super(channel);
    }

    private GetSequenceServiceBlockingStub(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected GetSequenceServiceBlockingStub build(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      return new GetSequenceServiceBlockingStub(channel, callOptions);
    }

    /**
     */
    public com.ltv.aerospike.api.proto.GetSequenceServices.GetSequenceResponse getSequence(com.ltv.aerospike.api.proto.GetSequenceServices.GetSequenceRequest request) {
      return blockingUnaryCall(
          getChannel(), getGetSequenceMethod(), getCallOptions(), request);
    }
  }

  /**
   */
  public static final class GetSequenceServiceFutureStub extends io.grpc.stub.AbstractStub<GetSequenceServiceFutureStub> {
    private GetSequenceServiceFutureStub(io.grpc.Channel channel) {
      super(channel);
    }

    private GetSequenceServiceFutureStub(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected GetSequenceServiceFutureStub build(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      return new GetSequenceServiceFutureStub(channel, callOptions);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<com.ltv.aerospike.api.proto.GetSequenceServices.GetSequenceResponse> getSequence(
        com.ltv.aerospike.api.proto.GetSequenceServices.GetSequenceRequest request) {
      return futureUnaryCall(
          getChannel().newCall(getGetSequenceMethod(), getCallOptions()), request);
    }
  }

  private static final int METHODID_GET_SEQUENCE = 0;

  private static final class MethodHandlers<Req, Resp> implements
      io.grpc.stub.ServerCalls.UnaryMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.ServerStreamingMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.ClientStreamingMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.BidiStreamingMethod<Req, Resp> {
    private final GetSequenceServiceImplBase serviceImpl;
    private final int methodId;

    MethodHandlers(GetSequenceServiceImplBase serviceImpl, int methodId) {
      this.serviceImpl = serviceImpl;
      this.methodId = methodId;
    }

    @java.lang.Override
    @java.lang.SuppressWarnings("unchecked")
    public void invoke(Req request, io.grpc.stub.StreamObserver<Resp> responseObserver) {
      switch (methodId) {
        case METHODID_GET_SEQUENCE:
          serviceImpl.getSequence((com.ltv.aerospike.api.proto.GetSequenceServices.GetSequenceRequest) request,
              (io.grpc.stub.StreamObserver<com.ltv.aerospike.api.proto.GetSequenceServices.GetSequenceResponse>) responseObserver);
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

  private static abstract class GetSequenceServiceBaseDescriptorSupplier
      implements io.grpc.protobuf.ProtoFileDescriptorSupplier, io.grpc.protobuf.ProtoServiceDescriptorSupplier {
    GetSequenceServiceBaseDescriptorSupplier() {}

    @java.lang.Override
    public com.google.protobuf.Descriptors.FileDescriptor getFileDescriptor() {
      return com.ltv.aerospike.api.proto.GetSequenceServices.getDescriptor();
    }

    @java.lang.Override
    public com.google.protobuf.Descriptors.ServiceDescriptor getServiceDescriptor() {
      return getFileDescriptor().findServiceByName("GetSequenceService");
    }
  }

  private static final class GetSequenceServiceFileDescriptorSupplier
      extends GetSequenceServiceBaseDescriptorSupplier {
    GetSequenceServiceFileDescriptorSupplier() {}
  }

  private static final class GetSequenceServiceMethodDescriptorSupplier
      extends GetSequenceServiceBaseDescriptorSupplier
      implements io.grpc.protobuf.ProtoMethodDescriptorSupplier {
    private final String methodName;

    GetSequenceServiceMethodDescriptorSupplier(String methodName) {
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
      synchronized (GetSequenceServiceGrpc.class) {
        result = serviceDescriptor;
        if (result == null) {
          serviceDescriptor = result = io.grpc.ServiceDescriptor.newBuilder(SERVICE_NAME)
              .setSchemaDescriptor(new GetSequenceServiceFileDescriptorSupplier())
              .addMethod(getGetSequenceMethod())
              .build();
        }
      }
    }
    return result;
  }
}
