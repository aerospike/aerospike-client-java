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
    comments = "Source: ShowIndexServices.proto")
public final class ShowIndexServiceGrpc {

  private ShowIndexServiceGrpc() {}

  public static final String SERVICE_NAME = "com.ltv.aerospike.api.proto.ShowIndexService";

  // Static method descriptors that strictly reflect the proto.
  private static volatile io.grpc.MethodDescriptor<com.ltv.aerospike.api.proto.ShowIndexServices.ShowIndexRequest,
      com.ltv.aerospike.api.proto.ShowIndexServices.ShowIndexResponse> getShowIndexMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "showIndex",
      requestType = com.ltv.aerospike.api.proto.ShowIndexServices.ShowIndexRequest.class,
      responseType = com.ltv.aerospike.api.proto.ShowIndexServices.ShowIndexResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.ltv.aerospike.api.proto.ShowIndexServices.ShowIndexRequest,
      com.ltv.aerospike.api.proto.ShowIndexServices.ShowIndexResponse> getShowIndexMethod() {
    io.grpc.MethodDescriptor<com.ltv.aerospike.api.proto.ShowIndexServices.ShowIndexRequest, com.ltv.aerospike.api.proto.ShowIndexServices.ShowIndexResponse> getShowIndexMethod;
    if ((getShowIndexMethod = ShowIndexServiceGrpc.getShowIndexMethod) == null) {
      synchronized (ShowIndexServiceGrpc.class) {
        if ((getShowIndexMethod = ShowIndexServiceGrpc.getShowIndexMethod) == null) {
          ShowIndexServiceGrpc.getShowIndexMethod = getShowIndexMethod =
              io.grpc.MethodDescriptor.<com.ltv.aerospike.api.proto.ShowIndexServices.ShowIndexRequest, com.ltv.aerospike.api.proto.ShowIndexServices.ShowIndexResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "showIndex"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.ltv.aerospike.api.proto.ShowIndexServices.ShowIndexRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.ltv.aerospike.api.proto.ShowIndexServices.ShowIndexResponse.getDefaultInstance()))
              .setSchemaDescriptor(new ShowIndexServiceMethodDescriptorSupplier("showIndex"))
              .build();
        }
      }
    }
    return getShowIndexMethod;
  }

  /**
   * Creates a new async stub that supports all call types for the service
   */
  public static ShowIndexServiceStub newStub(io.grpc.Channel channel) {
    return new ShowIndexServiceStub(channel);
  }

  /**
   * Creates a new blocking-style stub that supports unary and streaming output calls on the service
   */
  public static ShowIndexServiceBlockingStub newBlockingStub(
      io.grpc.Channel channel) {
    return new ShowIndexServiceBlockingStub(channel);
  }

  /**
   * Creates a new ListenableFuture-style stub that supports unary calls on the service
   */
  public static ShowIndexServiceFutureStub newFutureStub(
      io.grpc.Channel channel) {
    return new ShowIndexServiceFutureStub(channel);
  }

  /**
   */
  public static abstract class ShowIndexServiceImplBase implements io.grpc.BindableService {

    /**
     */
    public void showIndex(com.ltv.aerospike.api.proto.ShowIndexServices.ShowIndexRequest request,
        io.grpc.stub.StreamObserver<com.ltv.aerospike.api.proto.ShowIndexServices.ShowIndexResponse> responseObserver) {
      asyncUnimplementedUnaryCall(getShowIndexMethod(), responseObserver);
    }

    @java.lang.Override public final io.grpc.ServerServiceDefinition bindService() {
      return io.grpc.ServerServiceDefinition.builder(getServiceDescriptor())
          .addMethod(
            getShowIndexMethod(),
            asyncUnaryCall(
              new MethodHandlers<
                com.ltv.aerospike.api.proto.ShowIndexServices.ShowIndexRequest,
                com.ltv.aerospike.api.proto.ShowIndexServices.ShowIndexResponse>(
                  this, METHODID_SHOW_INDEX)))
          .build();
    }
  }

  /**
   */
  public static final class ShowIndexServiceStub extends io.grpc.stub.AbstractStub<ShowIndexServiceStub> {
    private ShowIndexServiceStub(io.grpc.Channel channel) {
      super(channel);
    }

    private ShowIndexServiceStub(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected ShowIndexServiceStub build(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      return new ShowIndexServiceStub(channel, callOptions);
    }

    /**
     */
    public void showIndex(com.ltv.aerospike.api.proto.ShowIndexServices.ShowIndexRequest request,
        io.grpc.stub.StreamObserver<com.ltv.aerospike.api.proto.ShowIndexServices.ShowIndexResponse> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(getShowIndexMethod(), getCallOptions()), request, responseObserver);
    }
  }

  /**
   */
  public static final class ShowIndexServiceBlockingStub extends io.grpc.stub.AbstractStub<ShowIndexServiceBlockingStub> {
    private ShowIndexServiceBlockingStub(io.grpc.Channel channel) {
      super(channel);
    }

    private ShowIndexServiceBlockingStub(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected ShowIndexServiceBlockingStub build(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      return new ShowIndexServiceBlockingStub(channel, callOptions);
    }

    /**
     */
    public com.ltv.aerospike.api.proto.ShowIndexServices.ShowIndexResponse showIndex(com.ltv.aerospike.api.proto.ShowIndexServices.ShowIndexRequest request) {
      return blockingUnaryCall(
          getChannel(), getShowIndexMethod(), getCallOptions(), request);
    }
  }

  /**
   */
  public static final class ShowIndexServiceFutureStub extends io.grpc.stub.AbstractStub<ShowIndexServiceFutureStub> {
    private ShowIndexServiceFutureStub(io.grpc.Channel channel) {
      super(channel);
    }

    private ShowIndexServiceFutureStub(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected ShowIndexServiceFutureStub build(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      return new ShowIndexServiceFutureStub(channel, callOptions);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<com.ltv.aerospike.api.proto.ShowIndexServices.ShowIndexResponse> showIndex(
        com.ltv.aerospike.api.proto.ShowIndexServices.ShowIndexRequest request) {
      return futureUnaryCall(
          getChannel().newCall(getShowIndexMethod(), getCallOptions()), request);
    }
  }

  private static final int METHODID_SHOW_INDEX = 0;

  private static final class MethodHandlers<Req, Resp> implements
      io.grpc.stub.ServerCalls.UnaryMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.ServerStreamingMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.ClientStreamingMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.BidiStreamingMethod<Req, Resp> {
    private final ShowIndexServiceImplBase serviceImpl;
    private final int methodId;

    MethodHandlers(ShowIndexServiceImplBase serviceImpl, int methodId) {
      this.serviceImpl = serviceImpl;
      this.methodId = methodId;
    }

    @java.lang.Override
    @java.lang.SuppressWarnings("unchecked")
    public void invoke(Req request, io.grpc.stub.StreamObserver<Resp> responseObserver) {
      switch (methodId) {
        case METHODID_SHOW_INDEX:
          serviceImpl.showIndex((com.ltv.aerospike.api.proto.ShowIndexServices.ShowIndexRequest) request,
              (io.grpc.stub.StreamObserver<com.ltv.aerospike.api.proto.ShowIndexServices.ShowIndexResponse>) responseObserver);
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

  private static abstract class ShowIndexServiceBaseDescriptorSupplier
      implements io.grpc.protobuf.ProtoFileDescriptorSupplier, io.grpc.protobuf.ProtoServiceDescriptorSupplier {
    ShowIndexServiceBaseDescriptorSupplier() {}

    @java.lang.Override
    public com.google.protobuf.Descriptors.FileDescriptor getFileDescriptor() {
      return com.ltv.aerospike.api.proto.ShowIndexServices.getDescriptor();
    }

    @java.lang.Override
    public com.google.protobuf.Descriptors.ServiceDescriptor getServiceDescriptor() {
      return getFileDescriptor().findServiceByName("ShowIndexService");
    }
  }

  private static final class ShowIndexServiceFileDescriptorSupplier
      extends ShowIndexServiceBaseDescriptorSupplier {
    ShowIndexServiceFileDescriptorSupplier() {}
  }

  private static final class ShowIndexServiceMethodDescriptorSupplier
      extends ShowIndexServiceBaseDescriptorSupplier
      implements io.grpc.protobuf.ProtoMethodDescriptorSupplier {
    private final String methodName;

    ShowIndexServiceMethodDescriptorSupplier(String methodName) {
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
      synchronized (ShowIndexServiceGrpc.class) {
        result = serviceDescriptor;
        if (result == null) {
          serviceDescriptor = result = io.grpc.ServiceDescriptor.newBuilder(SERVICE_NAME)
              .setSchemaDescriptor(new ShowIndexServiceFileDescriptorSupplier())
              .addMethod(getShowIndexMethod())
              .build();
        }
      }
    }
    return result;
  }
}
