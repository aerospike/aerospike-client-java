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
    comments = "Source: DeleteUserServices.proto")
public final class DeleteUserServiceGrpc {

  private DeleteUserServiceGrpc() {}

  public static final String SERVICE_NAME = "com.ltv.aerospike.api.proto.DeleteUserService";

  // Static method descriptors that strictly reflect the proto.
  private static volatile io.grpc.MethodDescriptor<com.ltv.aerospike.api.proto.DeleteUserServices.DeleteUserRequest,
      com.ltv.aerospike.api.proto.DeleteUserServices.DeleteUserResponse> getDeleteUserMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "deleteUser",
      requestType = com.ltv.aerospike.api.proto.DeleteUserServices.DeleteUserRequest.class,
      responseType = com.ltv.aerospike.api.proto.DeleteUserServices.DeleteUserResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.ltv.aerospike.api.proto.DeleteUserServices.DeleteUserRequest,
      com.ltv.aerospike.api.proto.DeleteUserServices.DeleteUserResponse> getDeleteUserMethod() {
    io.grpc.MethodDescriptor<com.ltv.aerospike.api.proto.DeleteUserServices.DeleteUserRequest, com.ltv.aerospike.api.proto.DeleteUserServices.DeleteUserResponse> getDeleteUserMethod;
    if ((getDeleteUserMethod = DeleteUserServiceGrpc.getDeleteUserMethod) == null) {
      synchronized (DeleteUserServiceGrpc.class) {
        if ((getDeleteUserMethod = DeleteUserServiceGrpc.getDeleteUserMethod) == null) {
          DeleteUserServiceGrpc.getDeleteUserMethod = getDeleteUserMethod =
              io.grpc.MethodDescriptor.<com.ltv.aerospike.api.proto.DeleteUserServices.DeleteUserRequest, com.ltv.aerospike.api.proto.DeleteUserServices.DeleteUserResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "deleteUser"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.ltv.aerospike.api.proto.DeleteUserServices.DeleteUserRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.ltv.aerospike.api.proto.DeleteUserServices.DeleteUserResponse.getDefaultInstance()))
              .setSchemaDescriptor(new DeleteUserServiceMethodDescriptorSupplier("deleteUser"))
              .build();
        }
      }
    }
    return getDeleteUserMethod;
  }

  /**
   * Creates a new async stub that supports all call types for the service
   */
  public static DeleteUserServiceStub newStub(io.grpc.Channel channel) {
    return new DeleteUserServiceStub(channel);
  }

  /**
   * Creates a new blocking-style stub that supports unary and streaming output calls on the service
   */
  public static DeleteUserServiceBlockingStub newBlockingStub(
      io.grpc.Channel channel) {
    return new DeleteUserServiceBlockingStub(channel);
  }

  /**
   * Creates a new ListenableFuture-style stub that supports unary calls on the service
   */
  public static DeleteUserServiceFutureStub newFutureStub(
      io.grpc.Channel channel) {
    return new DeleteUserServiceFutureStub(channel);
  }

  /**
   */
  public static abstract class DeleteUserServiceImplBase implements io.grpc.BindableService {

    /**
     */
    public void deleteUser(com.ltv.aerospike.api.proto.DeleteUserServices.DeleteUserRequest request,
        io.grpc.stub.StreamObserver<com.ltv.aerospike.api.proto.DeleteUserServices.DeleteUserResponse> responseObserver) {
      asyncUnimplementedUnaryCall(getDeleteUserMethod(), responseObserver);
    }

    @java.lang.Override public final io.grpc.ServerServiceDefinition bindService() {
      return io.grpc.ServerServiceDefinition.builder(getServiceDescriptor())
          .addMethod(
            getDeleteUserMethod(),
            asyncUnaryCall(
              new MethodHandlers<
                com.ltv.aerospike.api.proto.DeleteUserServices.DeleteUserRequest,
                com.ltv.aerospike.api.proto.DeleteUserServices.DeleteUserResponse>(
                  this, METHODID_DELETE_USER)))
          .build();
    }
  }

  /**
   */
  public static final class DeleteUserServiceStub extends io.grpc.stub.AbstractStub<DeleteUserServiceStub> {
    private DeleteUserServiceStub(io.grpc.Channel channel) {
      super(channel);
    }

    private DeleteUserServiceStub(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected DeleteUserServiceStub build(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      return new DeleteUserServiceStub(channel, callOptions);
    }

    /**
     */
    public void deleteUser(com.ltv.aerospike.api.proto.DeleteUserServices.DeleteUserRequest request,
        io.grpc.stub.StreamObserver<com.ltv.aerospike.api.proto.DeleteUserServices.DeleteUserResponse> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(getDeleteUserMethod(), getCallOptions()), request, responseObserver);
    }
  }

  /**
   */
  public static final class DeleteUserServiceBlockingStub extends io.grpc.stub.AbstractStub<DeleteUserServiceBlockingStub> {
    private DeleteUserServiceBlockingStub(io.grpc.Channel channel) {
      super(channel);
    }

    private DeleteUserServiceBlockingStub(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected DeleteUserServiceBlockingStub build(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      return new DeleteUserServiceBlockingStub(channel, callOptions);
    }

    /**
     */
    public com.ltv.aerospike.api.proto.DeleteUserServices.DeleteUserResponse deleteUser(com.ltv.aerospike.api.proto.DeleteUserServices.DeleteUserRequest request) {
      return blockingUnaryCall(
          getChannel(), getDeleteUserMethod(), getCallOptions(), request);
    }
  }

  /**
   */
  public static final class DeleteUserServiceFutureStub extends io.grpc.stub.AbstractStub<DeleteUserServiceFutureStub> {
    private DeleteUserServiceFutureStub(io.grpc.Channel channel) {
      super(channel);
    }

    private DeleteUserServiceFutureStub(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected DeleteUserServiceFutureStub build(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      return new DeleteUserServiceFutureStub(channel, callOptions);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<com.ltv.aerospike.api.proto.DeleteUserServices.DeleteUserResponse> deleteUser(
        com.ltv.aerospike.api.proto.DeleteUserServices.DeleteUserRequest request) {
      return futureUnaryCall(
          getChannel().newCall(getDeleteUserMethod(), getCallOptions()), request);
    }
  }

  private static final int METHODID_DELETE_USER = 0;

  private static final class MethodHandlers<Req, Resp> implements
      io.grpc.stub.ServerCalls.UnaryMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.ServerStreamingMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.ClientStreamingMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.BidiStreamingMethod<Req, Resp> {
    private final DeleteUserServiceImplBase serviceImpl;
    private final int methodId;

    MethodHandlers(DeleteUserServiceImplBase serviceImpl, int methodId) {
      this.serviceImpl = serviceImpl;
      this.methodId = methodId;
    }

    @java.lang.Override
    @java.lang.SuppressWarnings("unchecked")
    public void invoke(Req request, io.grpc.stub.StreamObserver<Resp> responseObserver) {
      switch (methodId) {
        case METHODID_DELETE_USER:
          serviceImpl.deleteUser((com.ltv.aerospike.api.proto.DeleteUserServices.DeleteUserRequest) request,
              (io.grpc.stub.StreamObserver<com.ltv.aerospike.api.proto.DeleteUserServices.DeleteUserResponse>) responseObserver);
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

  private static abstract class DeleteUserServiceBaseDescriptorSupplier
      implements io.grpc.protobuf.ProtoFileDescriptorSupplier, io.grpc.protobuf.ProtoServiceDescriptorSupplier {
    DeleteUserServiceBaseDescriptorSupplier() {}

    @java.lang.Override
    public com.google.protobuf.Descriptors.FileDescriptor getFileDescriptor() {
      return com.ltv.aerospike.api.proto.DeleteUserServices.getDescriptor();
    }

    @java.lang.Override
    public com.google.protobuf.Descriptors.ServiceDescriptor getServiceDescriptor() {
      return getFileDescriptor().findServiceByName("DeleteUserService");
    }
  }

  private static final class DeleteUserServiceFileDescriptorSupplier
      extends DeleteUserServiceBaseDescriptorSupplier {
    DeleteUserServiceFileDescriptorSupplier() {}
  }

  private static final class DeleteUserServiceMethodDescriptorSupplier
      extends DeleteUserServiceBaseDescriptorSupplier
      implements io.grpc.protobuf.ProtoMethodDescriptorSupplier {
    private final String methodName;

    DeleteUserServiceMethodDescriptorSupplier(String methodName) {
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
      synchronized (DeleteUserServiceGrpc.class) {
        result = serviceDescriptor;
        if (result == null) {
          serviceDescriptor = result = io.grpc.ServiceDescriptor.newBuilder(SERVICE_NAME)
              .setSchemaDescriptor(new DeleteUserServiceFileDescriptorSupplier())
              .addMethod(getDeleteUserMethod())
              .build();
        }
      }
    }
    return result;
  }
}
