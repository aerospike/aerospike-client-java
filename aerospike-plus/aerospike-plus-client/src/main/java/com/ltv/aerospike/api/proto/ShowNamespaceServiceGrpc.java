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
    comments = "Source: ShowNamespaceServices.proto")
public final class ShowNamespaceServiceGrpc {

  private ShowNamespaceServiceGrpc() {}

  public static final String SERVICE_NAME = "com.ltv.aerospike.api.proto.ShowNamespaceService";

  // Static method descriptors that strictly reflect the proto.
  private static volatile io.grpc.MethodDescriptor<com.ltv.aerospike.api.proto.ShowNamespaceServices.ShowNamespaceRequest,
      com.ltv.aerospike.api.proto.ShowNamespaceServices.ShowNamespaceResponse> getShowNamespaceMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "showNamespace",
      requestType = com.ltv.aerospike.api.proto.ShowNamespaceServices.ShowNamespaceRequest.class,
      responseType = com.ltv.aerospike.api.proto.ShowNamespaceServices.ShowNamespaceResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.ltv.aerospike.api.proto.ShowNamespaceServices.ShowNamespaceRequest,
      com.ltv.aerospike.api.proto.ShowNamespaceServices.ShowNamespaceResponse> getShowNamespaceMethod() {
    io.grpc.MethodDescriptor<com.ltv.aerospike.api.proto.ShowNamespaceServices.ShowNamespaceRequest, com.ltv.aerospike.api.proto.ShowNamespaceServices.ShowNamespaceResponse> getShowNamespaceMethod;
    if ((getShowNamespaceMethod = ShowNamespaceServiceGrpc.getShowNamespaceMethod) == null) {
      synchronized (ShowNamespaceServiceGrpc.class) {
        if ((getShowNamespaceMethod = ShowNamespaceServiceGrpc.getShowNamespaceMethod) == null) {
          ShowNamespaceServiceGrpc.getShowNamespaceMethod = getShowNamespaceMethod =
              io.grpc.MethodDescriptor.<com.ltv.aerospike.api.proto.ShowNamespaceServices.ShowNamespaceRequest, com.ltv.aerospike.api.proto.ShowNamespaceServices.ShowNamespaceResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "showNamespace"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.ltv.aerospike.api.proto.ShowNamespaceServices.ShowNamespaceRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.ltv.aerospike.api.proto.ShowNamespaceServices.ShowNamespaceResponse.getDefaultInstance()))
              .setSchemaDescriptor(new ShowNamespaceServiceMethodDescriptorSupplier("showNamespace"))
              .build();
        }
      }
    }
    return getShowNamespaceMethod;
  }

  /**
   * Creates a new async stub that supports all call types for the service
   */
  public static ShowNamespaceServiceStub newStub(io.grpc.Channel channel) {
    return new ShowNamespaceServiceStub(channel);
  }

  /**
   * Creates a new blocking-style stub that supports unary and streaming output calls on the service
   */
  public static ShowNamespaceServiceBlockingStub newBlockingStub(
      io.grpc.Channel channel) {
    return new ShowNamespaceServiceBlockingStub(channel);
  }

  /**
   * Creates a new ListenableFuture-style stub that supports unary calls on the service
   */
  public static ShowNamespaceServiceFutureStub newFutureStub(
      io.grpc.Channel channel) {
    return new ShowNamespaceServiceFutureStub(channel);
  }

  /**
   */
  public static abstract class ShowNamespaceServiceImplBase implements io.grpc.BindableService {

    /**
     */
    public void showNamespace(com.ltv.aerospike.api.proto.ShowNamespaceServices.ShowNamespaceRequest request,
        io.grpc.stub.StreamObserver<com.ltv.aerospike.api.proto.ShowNamespaceServices.ShowNamespaceResponse> responseObserver) {
      asyncUnimplementedUnaryCall(getShowNamespaceMethod(), responseObserver);
    }

    @java.lang.Override public final io.grpc.ServerServiceDefinition bindService() {
      return io.grpc.ServerServiceDefinition.builder(getServiceDescriptor())
          .addMethod(
            getShowNamespaceMethod(),
            asyncUnaryCall(
              new MethodHandlers<
                com.ltv.aerospike.api.proto.ShowNamespaceServices.ShowNamespaceRequest,
                com.ltv.aerospike.api.proto.ShowNamespaceServices.ShowNamespaceResponse>(
                  this, METHODID_SHOW_NAMESPACE)))
          .build();
    }
  }

  /**
   */
  public static final class ShowNamespaceServiceStub extends io.grpc.stub.AbstractStub<ShowNamespaceServiceStub> {
    private ShowNamespaceServiceStub(io.grpc.Channel channel) {
      super(channel);
    }

    private ShowNamespaceServiceStub(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected ShowNamespaceServiceStub build(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      return new ShowNamespaceServiceStub(channel, callOptions);
    }

    /**
     */
    public void showNamespace(com.ltv.aerospike.api.proto.ShowNamespaceServices.ShowNamespaceRequest request,
        io.grpc.stub.StreamObserver<com.ltv.aerospike.api.proto.ShowNamespaceServices.ShowNamespaceResponse> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(getShowNamespaceMethod(), getCallOptions()), request, responseObserver);
    }
  }

  /**
   */
  public static final class ShowNamespaceServiceBlockingStub extends io.grpc.stub.AbstractStub<ShowNamespaceServiceBlockingStub> {
    private ShowNamespaceServiceBlockingStub(io.grpc.Channel channel) {
      super(channel);
    }

    private ShowNamespaceServiceBlockingStub(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected ShowNamespaceServiceBlockingStub build(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      return new ShowNamespaceServiceBlockingStub(channel, callOptions);
    }

    /**
     */
    public com.ltv.aerospike.api.proto.ShowNamespaceServices.ShowNamespaceResponse showNamespace(com.ltv.aerospike.api.proto.ShowNamespaceServices.ShowNamespaceRequest request) {
      return blockingUnaryCall(
          getChannel(), getShowNamespaceMethod(), getCallOptions(), request);
    }
  }

  /**
   */
  public static final class ShowNamespaceServiceFutureStub extends io.grpc.stub.AbstractStub<ShowNamespaceServiceFutureStub> {
    private ShowNamespaceServiceFutureStub(io.grpc.Channel channel) {
      super(channel);
    }

    private ShowNamespaceServiceFutureStub(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected ShowNamespaceServiceFutureStub build(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      return new ShowNamespaceServiceFutureStub(channel, callOptions);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<com.ltv.aerospike.api.proto.ShowNamespaceServices.ShowNamespaceResponse> showNamespace(
        com.ltv.aerospike.api.proto.ShowNamespaceServices.ShowNamespaceRequest request) {
      return futureUnaryCall(
          getChannel().newCall(getShowNamespaceMethod(), getCallOptions()), request);
    }
  }

  private static final int METHODID_SHOW_NAMESPACE = 0;

  private static final class MethodHandlers<Req, Resp> implements
      io.grpc.stub.ServerCalls.UnaryMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.ServerStreamingMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.ClientStreamingMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.BidiStreamingMethod<Req, Resp> {
    private final ShowNamespaceServiceImplBase serviceImpl;
    private final int methodId;

    MethodHandlers(ShowNamespaceServiceImplBase serviceImpl, int methodId) {
      this.serviceImpl = serviceImpl;
      this.methodId = methodId;
    }

    @java.lang.Override
    @java.lang.SuppressWarnings("unchecked")
    public void invoke(Req request, io.grpc.stub.StreamObserver<Resp> responseObserver) {
      switch (methodId) {
        case METHODID_SHOW_NAMESPACE:
          serviceImpl.showNamespace((com.ltv.aerospike.api.proto.ShowNamespaceServices.ShowNamespaceRequest) request,
              (io.grpc.stub.StreamObserver<com.ltv.aerospike.api.proto.ShowNamespaceServices.ShowNamespaceResponse>) responseObserver);
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

  private static abstract class ShowNamespaceServiceBaseDescriptorSupplier
      implements io.grpc.protobuf.ProtoFileDescriptorSupplier, io.grpc.protobuf.ProtoServiceDescriptorSupplier {
    ShowNamespaceServiceBaseDescriptorSupplier() {}

    @java.lang.Override
    public com.google.protobuf.Descriptors.FileDescriptor getFileDescriptor() {
      return com.ltv.aerospike.api.proto.ShowNamespaceServices.getDescriptor();
    }

    @java.lang.Override
    public com.google.protobuf.Descriptors.ServiceDescriptor getServiceDescriptor() {
      return getFileDescriptor().findServiceByName("ShowNamespaceService");
    }
  }

  private static final class ShowNamespaceServiceFileDescriptorSupplier
      extends ShowNamespaceServiceBaseDescriptorSupplier {
    ShowNamespaceServiceFileDescriptorSupplier() {}
  }

  private static final class ShowNamespaceServiceMethodDescriptorSupplier
      extends ShowNamespaceServiceBaseDescriptorSupplier
      implements io.grpc.protobuf.ProtoMethodDescriptorSupplier {
    private final String methodName;

    ShowNamespaceServiceMethodDescriptorSupplier(String methodName) {
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
      synchronized (ShowNamespaceServiceGrpc.class) {
        result = serviceDescriptor;
        if (result == null) {
          serviceDescriptor = result = io.grpc.ServiceDescriptor.newBuilder(SERVICE_NAME)
              .setSchemaDescriptor(new ShowNamespaceServiceFileDescriptorSupplier())
              .addMethod(getShowNamespaceMethod())
              .build();
        }
      }
    }
    return result;
  }
}
