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
    comments = "Source: ShowBinServices.proto")
public final class ShowBinServiceGrpc {

  private ShowBinServiceGrpc() {}

  public static final String SERVICE_NAME = "com.ltv.aerospike.api.proto.ShowBinService";

  // Static method descriptors that strictly reflect the proto.
  private static volatile io.grpc.MethodDescriptor<com.ltv.aerospike.api.proto.ShowBinServices.ShowBinRequest,
      com.ltv.aerospike.api.proto.ShowBinServices.ShowBinResponse> getShowBinMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "showBin",
      requestType = com.ltv.aerospike.api.proto.ShowBinServices.ShowBinRequest.class,
      responseType = com.ltv.aerospike.api.proto.ShowBinServices.ShowBinResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.ltv.aerospike.api.proto.ShowBinServices.ShowBinRequest,
      com.ltv.aerospike.api.proto.ShowBinServices.ShowBinResponse> getShowBinMethod() {
    io.grpc.MethodDescriptor<com.ltv.aerospike.api.proto.ShowBinServices.ShowBinRequest, com.ltv.aerospike.api.proto.ShowBinServices.ShowBinResponse> getShowBinMethod;
    if ((getShowBinMethod = ShowBinServiceGrpc.getShowBinMethod) == null) {
      synchronized (ShowBinServiceGrpc.class) {
        if ((getShowBinMethod = ShowBinServiceGrpc.getShowBinMethod) == null) {
          ShowBinServiceGrpc.getShowBinMethod = getShowBinMethod =
              io.grpc.MethodDescriptor.<com.ltv.aerospike.api.proto.ShowBinServices.ShowBinRequest, com.ltv.aerospike.api.proto.ShowBinServices.ShowBinResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "showBin"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.ltv.aerospike.api.proto.ShowBinServices.ShowBinRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.ltv.aerospike.api.proto.ShowBinServices.ShowBinResponse.getDefaultInstance()))
              .setSchemaDescriptor(new ShowBinServiceMethodDescriptorSupplier("showBin"))
              .build();
        }
      }
    }
    return getShowBinMethod;
  }

  /**
   * Creates a new async stub that supports all call types for the service
   */
  public static ShowBinServiceStub newStub(io.grpc.Channel channel) {
    return new ShowBinServiceStub(channel);
  }

  /**
   * Creates a new blocking-style stub that supports unary and streaming output calls on the service
   */
  public static ShowBinServiceBlockingStub newBlockingStub(
      io.grpc.Channel channel) {
    return new ShowBinServiceBlockingStub(channel);
  }

  /**
   * Creates a new ListenableFuture-style stub that supports unary calls on the service
   */
  public static ShowBinServiceFutureStub newFutureStub(
      io.grpc.Channel channel) {
    return new ShowBinServiceFutureStub(channel);
  }

  /**
   */
  public static abstract class ShowBinServiceImplBase implements io.grpc.BindableService {

    /**
     */
    public void showBin(com.ltv.aerospike.api.proto.ShowBinServices.ShowBinRequest request,
        io.grpc.stub.StreamObserver<com.ltv.aerospike.api.proto.ShowBinServices.ShowBinResponse> responseObserver) {
      asyncUnimplementedUnaryCall(getShowBinMethod(), responseObserver);
    }

    @java.lang.Override public final io.grpc.ServerServiceDefinition bindService() {
      return io.grpc.ServerServiceDefinition.builder(getServiceDescriptor())
          .addMethod(
            getShowBinMethod(),
            asyncUnaryCall(
              new MethodHandlers<
                com.ltv.aerospike.api.proto.ShowBinServices.ShowBinRequest,
                com.ltv.aerospike.api.proto.ShowBinServices.ShowBinResponse>(
                  this, METHODID_SHOW_BIN)))
          .build();
    }
  }

  /**
   */
  public static final class ShowBinServiceStub extends io.grpc.stub.AbstractStub<ShowBinServiceStub> {
    private ShowBinServiceStub(io.grpc.Channel channel) {
      super(channel);
    }

    private ShowBinServiceStub(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected ShowBinServiceStub build(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      return new ShowBinServiceStub(channel, callOptions);
    }

    /**
     */
    public void showBin(com.ltv.aerospike.api.proto.ShowBinServices.ShowBinRequest request,
        io.grpc.stub.StreamObserver<com.ltv.aerospike.api.proto.ShowBinServices.ShowBinResponse> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(getShowBinMethod(), getCallOptions()), request, responseObserver);
    }
  }

  /**
   */
  public static final class ShowBinServiceBlockingStub extends io.grpc.stub.AbstractStub<ShowBinServiceBlockingStub> {
    private ShowBinServiceBlockingStub(io.grpc.Channel channel) {
      super(channel);
    }

    private ShowBinServiceBlockingStub(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected ShowBinServiceBlockingStub build(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      return new ShowBinServiceBlockingStub(channel, callOptions);
    }

    /**
     */
    public com.ltv.aerospike.api.proto.ShowBinServices.ShowBinResponse showBin(com.ltv.aerospike.api.proto.ShowBinServices.ShowBinRequest request) {
      return blockingUnaryCall(
          getChannel(), getShowBinMethod(), getCallOptions(), request);
    }
  }

  /**
   */
  public static final class ShowBinServiceFutureStub extends io.grpc.stub.AbstractStub<ShowBinServiceFutureStub> {
    private ShowBinServiceFutureStub(io.grpc.Channel channel) {
      super(channel);
    }

    private ShowBinServiceFutureStub(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected ShowBinServiceFutureStub build(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      return new ShowBinServiceFutureStub(channel, callOptions);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<com.ltv.aerospike.api.proto.ShowBinServices.ShowBinResponse> showBin(
        com.ltv.aerospike.api.proto.ShowBinServices.ShowBinRequest request) {
      return futureUnaryCall(
          getChannel().newCall(getShowBinMethod(), getCallOptions()), request);
    }
  }

  private static final int METHODID_SHOW_BIN = 0;

  private static final class MethodHandlers<Req, Resp> implements
      io.grpc.stub.ServerCalls.UnaryMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.ServerStreamingMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.ClientStreamingMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.BidiStreamingMethod<Req, Resp> {
    private final ShowBinServiceImplBase serviceImpl;
    private final int methodId;

    MethodHandlers(ShowBinServiceImplBase serviceImpl, int methodId) {
      this.serviceImpl = serviceImpl;
      this.methodId = methodId;
    }

    @java.lang.Override
    @java.lang.SuppressWarnings("unchecked")
    public void invoke(Req request, io.grpc.stub.StreamObserver<Resp> responseObserver) {
      switch (methodId) {
        case METHODID_SHOW_BIN:
          serviceImpl.showBin((com.ltv.aerospike.api.proto.ShowBinServices.ShowBinRequest) request,
              (io.grpc.stub.StreamObserver<com.ltv.aerospike.api.proto.ShowBinServices.ShowBinResponse>) responseObserver);
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

  private static abstract class ShowBinServiceBaseDescriptorSupplier
      implements io.grpc.protobuf.ProtoFileDescriptorSupplier, io.grpc.protobuf.ProtoServiceDescriptorSupplier {
    ShowBinServiceBaseDescriptorSupplier() {}

    @java.lang.Override
    public com.google.protobuf.Descriptors.FileDescriptor getFileDescriptor() {
      return com.ltv.aerospike.api.proto.ShowBinServices.getDescriptor();
    }

    @java.lang.Override
    public com.google.protobuf.Descriptors.ServiceDescriptor getServiceDescriptor() {
      return getFileDescriptor().findServiceByName("ShowBinService");
    }
  }

  private static final class ShowBinServiceFileDescriptorSupplier
      extends ShowBinServiceBaseDescriptorSupplier {
    ShowBinServiceFileDescriptorSupplier() {}
  }

  private static final class ShowBinServiceMethodDescriptorSupplier
      extends ShowBinServiceBaseDescriptorSupplier
      implements io.grpc.protobuf.ProtoMethodDescriptorSupplier {
    private final String methodName;

    ShowBinServiceMethodDescriptorSupplier(String methodName) {
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
      synchronized (ShowBinServiceGrpc.class) {
        result = serviceDescriptor;
        if (result == null) {
          serviceDescriptor = result = io.grpc.ServiceDescriptor.newBuilder(SERVICE_NAME)
              .setSchemaDescriptor(new ShowBinServiceFileDescriptorSupplier())
              .addMethod(getShowBinMethod())
              .build();
        }
      }
    }
    return result;
  }
}
