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
    comments = "Source: ShowSetServices.proto")
public final class ShowSetServiceGrpc {

  private ShowSetServiceGrpc() {}

  public static final String SERVICE_NAME = "com.ltv.aerospike.api.proto.ShowSetService";

  // Static method descriptors that strictly reflect the proto.
  private static volatile io.grpc.MethodDescriptor<com.ltv.aerospike.api.proto.ShowSetServices.ShowSetRequest,
      com.ltv.aerospike.api.proto.ShowSetServices.ShowSetResponse> getShowSetMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "showSet",
      requestType = com.ltv.aerospike.api.proto.ShowSetServices.ShowSetRequest.class,
      responseType = com.ltv.aerospike.api.proto.ShowSetServices.ShowSetResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.ltv.aerospike.api.proto.ShowSetServices.ShowSetRequest,
      com.ltv.aerospike.api.proto.ShowSetServices.ShowSetResponse> getShowSetMethod() {
    io.grpc.MethodDescriptor<com.ltv.aerospike.api.proto.ShowSetServices.ShowSetRequest, com.ltv.aerospike.api.proto.ShowSetServices.ShowSetResponse> getShowSetMethod;
    if ((getShowSetMethod = ShowSetServiceGrpc.getShowSetMethod) == null) {
      synchronized (ShowSetServiceGrpc.class) {
        if ((getShowSetMethod = ShowSetServiceGrpc.getShowSetMethod) == null) {
          ShowSetServiceGrpc.getShowSetMethod = getShowSetMethod =
              io.grpc.MethodDescriptor.<com.ltv.aerospike.api.proto.ShowSetServices.ShowSetRequest, com.ltv.aerospike.api.proto.ShowSetServices.ShowSetResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "showSet"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.ltv.aerospike.api.proto.ShowSetServices.ShowSetRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.ltv.aerospike.api.proto.ShowSetServices.ShowSetResponse.getDefaultInstance()))
              .setSchemaDescriptor(new ShowSetServiceMethodDescriptorSupplier("showSet"))
              .build();
        }
      }
    }
    return getShowSetMethod;
  }

  /**
   * Creates a new async stub that supports all call types for the service
   */
  public static ShowSetServiceStub newStub(io.grpc.Channel channel) {
    return new ShowSetServiceStub(channel);
  }

  /**
   * Creates a new blocking-style stub that supports unary and streaming output calls on the service
   */
  public static ShowSetServiceBlockingStub newBlockingStub(
      io.grpc.Channel channel) {
    return new ShowSetServiceBlockingStub(channel);
  }

  /**
   * Creates a new ListenableFuture-style stub that supports unary calls on the service
   */
  public static ShowSetServiceFutureStub newFutureStub(
      io.grpc.Channel channel) {
    return new ShowSetServiceFutureStub(channel);
  }

  /**
   */
  public static abstract class ShowSetServiceImplBase implements io.grpc.BindableService {

    /**
     */
    public void showSet(com.ltv.aerospike.api.proto.ShowSetServices.ShowSetRequest request,
        io.grpc.stub.StreamObserver<com.ltv.aerospike.api.proto.ShowSetServices.ShowSetResponse> responseObserver) {
      asyncUnimplementedUnaryCall(getShowSetMethod(), responseObserver);
    }

    @java.lang.Override public final io.grpc.ServerServiceDefinition bindService() {
      return io.grpc.ServerServiceDefinition.builder(getServiceDescriptor())
          .addMethod(
            getShowSetMethod(),
            asyncUnaryCall(
              new MethodHandlers<
                com.ltv.aerospike.api.proto.ShowSetServices.ShowSetRequest,
                com.ltv.aerospike.api.proto.ShowSetServices.ShowSetResponse>(
                  this, METHODID_SHOW_SET)))
          .build();
    }
  }

  /**
   */
  public static final class ShowSetServiceStub extends io.grpc.stub.AbstractStub<ShowSetServiceStub> {
    private ShowSetServiceStub(io.grpc.Channel channel) {
      super(channel);
    }

    private ShowSetServiceStub(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected ShowSetServiceStub build(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      return new ShowSetServiceStub(channel, callOptions);
    }

    /**
     */
    public void showSet(com.ltv.aerospike.api.proto.ShowSetServices.ShowSetRequest request,
        io.grpc.stub.StreamObserver<com.ltv.aerospike.api.proto.ShowSetServices.ShowSetResponse> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(getShowSetMethod(), getCallOptions()), request, responseObserver);
    }
  }

  /**
   */
  public static final class ShowSetServiceBlockingStub extends io.grpc.stub.AbstractStub<ShowSetServiceBlockingStub> {
    private ShowSetServiceBlockingStub(io.grpc.Channel channel) {
      super(channel);
    }

    private ShowSetServiceBlockingStub(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected ShowSetServiceBlockingStub build(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      return new ShowSetServiceBlockingStub(channel, callOptions);
    }

    /**
     */
    public com.ltv.aerospike.api.proto.ShowSetServices.ShowSetResponse showSet(com.ltv.aerospike.api.proto.ShowSetServices.ShowSetRequest request) {
      return blockingUnaryCall(
          getChannel(), getShowSetMethod(), getCallOptions(), request);
    }
  }

  /**
   */
  public static final class ShowSetServiceFutureStub extends io.grpc.stub.AbstractStub<ShowSetServiceFutureStub> {
    private ShowSetServiceFutureStub(io.grpc.Channel channel) {
      super(channel);
    }

    private ShowSetServiceFutureStub(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected ShowSetServiceFutureStub build(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      return new ShowSetServiceFutureStub(channel, callOptions);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<com.ltv.aerospike.api.proto.ShowSetServices.ShowSetResponse> showSet(
        com.ltv.aerospike.api.proto.ShowSetServices.ShowSetRequest request) {
      return futureUnaryCall(
          getChannel().newCall(getShowSetMethod(), getCallOptions()), request);
    }
  }

  private static final int METHODID_SHOW_SET = 0;

  private static final class MethodHandlers<Req, Resp> implements
      io.grpc.stub.ServerCalls.UnaryMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.ServerStreamingMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.ClientStreamingMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.BidiStreamingMethod<Req, Resp> {
    private final ShowSetServiceImplBase serviceImpl;
    private final int methodId;

    MethodHandlers(ShowSetServiceImplBase serviceImpl, int methodId) {
      this.serviceImpl = serviceImpl;
      this.methodId = methodId;
    }

    @java.lang.Override
    @java.lang.SuppressWarnings("unchecked")
    public void invoke(Req request, io.grpc.stub.StreamObserver<Resp> responseObserver) {
      switch (methodId) {
        case METHODID_SHOW_SET:
          serviceImpl.showSet((com.ltv.aerospike.api.proto.ShowSetServices.ShowSetRequest) request,
              (io.grpc.stub.StreamObserver<com.ltv.aerospike.api.proto.ShowSetServices.ShowSetResponse>) responseObserver);
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

  private static abstract class ShowSetServiceBaseDescriptorSupplier
      implements io.grpc.protobuf.ProtoFileDescriptorSupplier, io.grpc.protobuf.ProtoServiceDescriptorSupplier {
    ShowSetServiceBaseDescriptorSupplier() {}

    @java.lang.Override
    public com.google.protobuf.Descriptors.FileDescriptor getFileDescriptor() {
      return com.ltv.aerospike.api.proto.ShowSetServices.getDescriptor();
    }

    @java.lang.Override
    public com.google.protobuf.Descriptors.ServiceDescriptor getServiceDescriptor() {
      return getFileDescriptor().findServiceByName("ShowSetService");
    }
  }

  private static final class ShowSetServiceFileDescriptorSupplier
      extends ShowSetServiceBaseDescriptorSupplier {
    ShowSetServiceFileDescriptorSupplier() {}
  }

  private static final class ShowSetServiceMethodDescriptorSupplier
      extends ShowSetServiceBaseDescriptorSupplier
      implements io.grpc.protobuf.ProtoMethodDescriptorSupplier {
    private final String methodName;

    ShowSetServiceMethodDescriptorSupplier(String methodName) {
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
      synchronized (ShowSetServiceGrpc.class) {
        result = serviceDescriptor;
        if (result == null) {
          serviceDescriptor = result = io.grpc.ServiceDescriptor.newBuilder(SERVICE_NAME)
              .setSchemaDescriptor(new ShowSetServiceFileDescriptorSupplier())
              .addMethod(getShowSetMethod())
              .build();
        }
      }
    }
    return result;
  }
}
