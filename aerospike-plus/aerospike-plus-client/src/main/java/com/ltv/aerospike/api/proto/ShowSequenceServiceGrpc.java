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
    comments = "Source: ShowSequenceServices.proto")
public final class ShowSequenceServiceGrpc {

  private ShowSequenceServiceGrpc() {}

  public static final String SERVICE_NAME = "com.ltv.aerospike.api.proto.ShowSequenceService";

  // Static method descriptors that strictly reflect the proto.
  private static volatile io.grpc.MethodDescriptor<com.ltv.aerospike.api.proto.ShowSequenceServices.ShowSequenceRequest,
      com.ltv.aerospike.api.proto.ShowSequenceServices.ShowSequenceResponse> getShowSequenceMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "showSequence",
      requestType = com.ltv.aerospike.api.proto.ShowSequenceServices.ShowSequenceRequest.class,
      responseType = com.ltv.aerospike.api.proto.ShowSequenceServices.ShowSequenceResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.ltv.aerospike.api.proto.ShowSequenceServices.ShowSequenceRequest,
      com.ltv.aerospike.api.proto.ShowSequenceServices.ShowSequenceResponse> getShowSequenceMethod() {
    io.grpc.MethodDescriptor<com.ltv.aerospike.api.proto.ShowSequenceServices.ShowSequenceRequest, com.ltv.aerospike.api.proto.ShowSequenceServices.ShowSequenceResponse> getShowSequenceMethod;
    if ((getShowSequenceMethod = ShowSequenceServiceGrpc.getShowSequenceMethod) == null) {
      synchronized (ShowSequenceServiceGrpc.class) {
        if ((getShowSequenceMethod = ShowSequenceServiceGrpc.getShowSequenceMethod) == null) {
          ShowSequenceServiceGrpc.getShowSequenceMethod = getShowSequenceMethod =
              io.grpc.MethodDescriptor.<com.ltv.aerospike.api.proto.ShowSequenceServices.ShowSequenceRequest, com.ltv.aerospike.api.proto.ShowSequenceServices.ShowSequenceResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "showSequence"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.ltv.aerospike.api.proto.ShowSequenceServices.ShowSequenceRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.ltv.aerospike.api.proto.ShowSequenceServices.ShowSequenceResponse.getDefaultInstance()))
              .setSchemaDescriptor(new ShowSequenceServiceMethodDescriptorSupplier("showSequence"))
              .build();
        }
      }
    }
    return getShowSequenceMethod;
  }

  /**
   * Creates a new async stub that supports all call types for the service
   */
  public static ShowSequenceServiceStub newStub(io.grpc.Channel channel) {
    return new ShowSequenceServiceStub(channel);
  }

  /**
   * Creates a new blocking-style stub that supports unary and streaming output calls on the service
   */
  public static ShowSequenceServiceBlockingStub newBlockingStub(
      io.grpc.Channel channel) {
    return new ShowSequenceServiceBlockingStub(channel);
  }

  /**
   * Creates a new ListenableFuture-style stub that supports unary calls on the service
   */
  public static ShowSequenceServiceFutureStub newFutureStub(
      io.grpc.Channel channel) {
    return new ShowSequenceServiceFutureStub(channel);
  }

  /**
   */
  public static abstract class ShowSequenceServiceImplBase implements io.grpc.BindableService {

    /**
     */
    public void showSequence(com.ltv.aerospike.api.proto.ShowSequenceServices.ShowSequenceRequest request,
        io.grpc.stub.StreamObserver<com.ltv.aerospike.api.proto.ShowSequenceServices.ShowSequenceResponse> responseObserver) {
      asyncUnimplementedUnaryCall(getShowSequenceMethod(), responseObserver);
    }

    @java.lang.Override public final io.grpc.ServerServiceDefinition bindService() {
      return io.grpc.ServerServiceDefinition.builder(getServiceDescriptor())
          .addMethod(
            getShowSequenceMethod(),
            asyncUnaryCall(
              new MethodHandlers<
                com.ltv.aerospike.api.proto.ShowSequenceServices.ShowSequenceRequest,
                com.ltv.aerospike.api.proto.ShowSequenceServices.ShowSequenceResponse>(
                  this, METHODID_SHOW_SEQUENCE)))
          .build();
    }
  }

  /**
   */
  public static final class ShowSequenceServiceStub extends io.grpc.stub.AbstractStub<ShowSequenceServiceStub> {
    private ShowSequenceServiceStub(io.grpc.Channel channel) {
      super(channel);
    }

    private ShowSequenceServiceStub(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected ShowSequenceServiceStub build(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      return new ShowSequenceServiceStub(channel, callOptions);
    }

    /**
     */
    public void showSequence(com.ltv.aerospike.api.proto.ShowSequenceServices.ShowSequenceRequest request,
        io.grpc.stub.StreamObserver<com.ltv.aerospike.api.proto.ShowSequenceServices.ShowSequenceResponse> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(getShowSequenceMethod(), getCallOptions()), request, responseObserver);
    }
  }

  /**
   */
  public static final class ShowSequenceServiceBlockingStub extends io.grpc.stub.AbstractStub<ShowSequenceServiceBlockingStub> {
    private ShowSequenceServiceBlockingStub(io.grpc.Channel channel) {
      super(channel);
    }

    private ShowSequenceServiceBlockingStub(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected ShowSequenceServiceBlockingStub build(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      return new ShowSequenceServiceBlockingStub(channel, callOptions);
    }

    /**
     */
    public com.ltv.aerospike.api.proto.ShowSequenceServices.ShowSequenceResponse showSequence(com.ltv.aerospike.api.proto.ShowSequenceServices.ShowSequenceRequest request) {
      return blockingUnaryCall(
          getChannel(), getShowSequenceMethod(), getCallOptions(), request);
    }
  }

  /**
   */
  public static final class ShowSequenceServiceFutureStub extends io.grpc.stub.AbstractStub<ShowSequenceServiceFutureStub> {
    private ShowSequenceServiceFutureStub(io.grpc.Channel channel) {
      super(channel);
    }

    private ShowSequenceServiceFutureStub(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected ShowSequenceServiceFutureStub build(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      return new ShowSequenceServiceFutureStub(channel, callOptions);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<com.ltv.aerospike.api.proto.ShowSequenceServices.ShowSequenceResponse> showSequence(
        com.ltv.aerospike.api.proto.ShowSequenceServices.ShowSequenceRequest request) {
      return futureUnaryCall(
          getChannel().newCall(getShowSequenceMethod(), getCallOptions()), request);
    }
  }

  private static final int METHODID_SHOW_SEQUENCE = 0;

  private static final class MethodHandlers<Req, Resp> implements
      io.grpc.stub.ServerCalls.UnaryMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.ServerStreamingMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.ClientStreamingMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.BidiStreamingMethod<Req, Resp> {
    private final ShowSequenceServiceImplBase serviceImpl;
    private final int methodId;

    MethodHandlers(ShowSequenceServiceImplBase serviceImpl, int methodId) {
      this.serviceImpl = serviceImpl;
      this.methodId = methodId;
    }

    @java.lang.Override
    @java.lang.SuppressWarnings("unchecked")
    public void invoke(Req request, io.grpc.stub.StreamObserver<Resp> responseObserver) {
      switch (methodId) {
        case METHODID_SHOW_SEQUENCE:
          serviceImpl.showSequence((com.ltv.aerospike.api.proto.ShowSequenceServices.ShowSequenceRequest) request,
              (io.grpc.stub.StreamObserver<com.ltv.aerospike.api.proto.ShowSequenceServices.ShowSequenceResponse>) responseObserver);
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

  private static abstract class ShowSequenceServiceBaseDescriptorSupplier
      implements io.grpc.protobuf.ProtoFileDescriptorSupplier, io.grpc.protobuf.ProtoServiceDescriptorSupplier {
    ShowSequenceServiceBaseDescriptorSupplier() {}

    @java.lang.Override
    public com.google.protobuf.Descriptors.FileDescriptor getFileDescriptor() {
      return com.ltv.aerospike.api.proto.ShowSequenceServices.getDescriptor();
    }

    @java.lang.Override
    public com.google.protobuf.Descriptors.ServiceDescriptor getServiceDescriptor() {
      return getFileDescriptor().findServiceByName("ShowSequenceService");
    }
  }

  private static final class ShowSequenceServiceFileDescriptorSupplier
      extends ShowSequenceServiceBaseDescriptorSupplier {
    ShowSequenceServiceFileDescriptorSupplier() {}
  }

  private static final class ShowSequenceServiceMethodDescriptorSupplier
      extends ShowSequenceServiceBaseDescriptorSupplier
      implements io.grpc.protobuf.ProtoMethodDescriptorSupplier {
    private final String methodName;

    ShowSequenceServiceMethodDescriptorSupplier(String methodName) {
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
      synchronized (ShowSequenceServiceGrpc.class) {
        result = serviceDescriptor;
        if (result == null) {
          serviceDescriptor = result = io.grpc.ServiceDescriptor.newBuilder(SERVICE_NAME)
              .setSchemaDescriptor(new ShowSequenceServiceFileDescriptorSupplier())
              .addMethod(getShowSequenceMethod())
              .build();
        }
      }
    }
    return result;
  }
}
