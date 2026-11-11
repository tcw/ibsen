package github.com.tcw.ibsen.client;

import static io.grpc.MethodDescriptor.generateFullMethodName;

/**
 */
@javax.annotation.Generated(
    value = "by gRPC proto compiler (version 1.68.1)",
    comments = "Source: ibsen.proto")
@io.grpc.stub.annotations.GrpcGenerated
public final class IbsenGrpc {

  private IbsenGrpc() {}

  public static final java.lang.String SERVICE_NAME = "Ibsen";

  // Static method descriptors that strictly reflect the proto.
  private static volatile io.grpc.MethodDescriptor<github.com.tcw.ibsen.client.InputEntries,
      github.com.tcw.ibsen.client.WriteStatus> getWriteMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "write",
      requestType = github.com.tcw.ibsen.client.InputEntries.class,
      responseType = github.com.tcw.ibsen.client.WriteStatus.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<github.com.tcw.ibsen.client.InputEntries,
      github.com.tcw.ibsen.client.WriteStatus> getWriteMethod() {
    io.grpc.MethodDescriptor<github.com.tcw.ibsen.client.InputEntries, github.com.tcw.ibsen.client.WriteStatus> getWriteMethod;
    if ((getWriteMethod = IbsenGrpc.getWriteMethod) == null) {
      synchronized (IbsenGrpc.class) {
        if ((getWriteMethod = IbsenGrpc.getWriteMethod) == null) {
          IbsenGrpc.getWriteMethod = getWriteMethod =
              io.grpc.MethodDescriptor.<github.com.tcw.ibsen.client.InputEntries, github.com.tcw.ibsen.client.WriteStatus>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "write"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  github.com.tcw.ibsen.client.InputEntries.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  github.com.tcw.ibsen.client.WriteStatus.getDefaultInstance()))
              .setSchemaDescriptor(new IbsenMethodDescriptorSupplier("write"))
              .build();
        }
      }
    }
    return getWriteMethod;
  }

  private static volatile io.grpc.MethodDescriptor<github.com.tcw.ibsen.client.ReadParams,
      github.com.tcw.ibsen.client.OutputEntries> getReadMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "read",
      requestType = github.com.tcw.ibsen.client.ReadParams.class,
      responseType = github.com.tcw.ibsen.client.OutputEntries.class,
      methodType = io.grpc.MethodDescriptor.MethodType.SERVER_STREAMING)
  public static io.grpc.MethodDescriptor<github.com.tcw.ibsen.client.ReadParams,
      github.com.tcw.ibsen.client.OutputEntries> getReadMethod() {
    io.grpc.MethodDescriptor<github.com.tcw.ibsen.client.ReadParams, github.com.tcw.ibsen.client.OutputEntries> getReadMethod;
    if ((getReadMethod = IbsenGrpc.getReadMethod) == null) {
      synchronized (IbsenGrpc.class) {
        if ((getReadMethod = IbsenGrpc.getReadMethod) == null) {
          IbsenGrpc.getReadMethod = getReadMethod =
              io.grpc.MethodDescriptor.<github.com.tcw.ibsen.client.ReadParams, github.com.tcw.ibsen.client.OutputEntries>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.SERVER_STREAMING)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "read"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  github.com.tcw.ibsen.client.ReadParams.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  github.com.tcw.ibsen.client.OutputEntries.getDefaultInstance()))
              .setSchemaDescriptor(new IbsenMethodDescriptorSupplier("read"))
              .build();
        }
      }
    }
    return getReadMethod;
  }

  private static volatile io.grpc.MethodDescriptor<github.com.tcw.ibsen.client.EmptyArgs,
      github.com.tcw.ibsen.client.TopicList> getListMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "list",
      requestType = github.com.tcw.ibsen.client.EmptyArgs.class,
      responseType = github.com.tcw.ibsen.client.TopicList.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<github.com.tcw.ibsen.client.EmptyArgs,
      github.com.tcw.ibsen.client.TopicList> getListMethod() {
    io.grpc.MethodDescriptor<github.com.tcw.ibsen.client.EmptyArgs, github.com.tcw.ibsen.client.TopicList> getListMethod;
    if ((getListMethod = IbsenGrpc.getListMethod) == null) {
      synchronized (IbsenGrpc.class) {
        if ((getListMethod = IbsenGrpc.getListMethod) == null) {
          IbsenGrpc.getListMethod = getListMethod =
              io.grpc.MethodDescriptor.<github.com.tcw.ibsen.client.EmptyArgs, github.com.tcw.ibsen.client.TopicList>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "list"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  github.com.tcw.ibsen.client.EmptyArgs.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  github.com.tcw.ibsen.client.TopicList.getDefaultInstance()))
              .setSchemaDescriptor(new IbsenMethodDescriptorSupplier("list"))
              .build();
        }
      }
    }
    return getListMethod;
  }

  /**
   * Creates a new async stub that supports all call types for the service
   */
  public static IbsenStub newStub(io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<IbsenStub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<IbsenStub>() {
        @java.lang.Override
        public IbsenStub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new IbsenStub(channel, callOptions);
        }
      };
    return IbsenStub.newStub(factory, channel);
  }

  /**
   * Creates a new blocking-style stub that supports unary and streaming output calls on the service
   */
  public static IbsenBlockingStub newBlockingStub(
      io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<IbsenBlockingStub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<IbsenBlockingStub>() {
        @java.lang.Override
        public IbsenBlockingStub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new IbsenBlockingStub(channel, callOptions);
        }
      };
    return IbsenBlockingStub.newStub(factory, channel);
  }

  /**
   * Creates a new ListenableFuture-style stub that supports unary calls on the service
   */
  public static IbsenFutureStub newFutureStub(
      io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<IbsenFutureStub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<IbsenFutureStub>() {
        @java.lang.Override
        public IbsenFutureStub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new IbsenFutureStub(channel, callOptions);
        }
      };
    return IbsenFutureStub.newStub(factory, channel);
  }

  /**
   */
  public interface AsyncService {

    /**
     */
    default void write(github.com.tcw.ibsen.client.InputEntries request,
        io.grpc.stub.StreamObserver<github.com.tcw.ibsen.client.WriteStatus> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getWriteMethod(), responseObserver);
    }

    /**
     */
    default void read(github.com.tcw.ibsen.client.ReadParams request,
        io.grpc.stub.StreamObserver<github.com.tcw.ibsen.client.OutputEntries> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getReadMethod(), responseObserver);
    }

    /**
     */
    default void list(github.com.tcw.ibsen.client.EmptyArgs request,
        io.grpc.stub.StreamObserver<github.com.tcw.ibsen.client.TopicList> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getListMethod(), responseObserver);
    }
  }

  /**
   * Base class for the server implementation of the service Ibsen.
   */
  public static abstract class IbsenImplBase
      implements io.grpc.BindableService, AsyncService {

    @java.lang.Override public final io.grpc.ServerServiceDefinition bindService() {
      return IbsenGrpc.bindService(this);
    }
  }

  /**
   * A stub to allow clients to do asynchronous rpc calls to service Ibsen.
   */
  public static final class IbsenStub
      extends io.grpc.stub.AbstractAsyncStub<IbsenStub> {
    private IbsenStub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected IbsenStub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new IbsenStub(channel, callOptions);
    }

    /**
     */
    public void write(github.com.tcw.ibsen.client.InputEntries request,
        io.grpc.stub.StreamObserver<github.com.tcw.ibsen.client.WriteStatus> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getWriteMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void read(github.com.tcw.ibsen.client.ReadParams request,
        io.grpc.stub.StreamObserver<github.com.tcw.ibsen.client.OutputEntries> responseObserver) {
      io.grpc.stub.ClientCalls.asyncServerStreamingCall(
          getChannel().newCall(getReadMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void list(github.com.tcw.ibsen.client.EmptyArgs request,
        io.grpc.stub.StreamObserver<github.com.tcw.ibsen.client.TopicList> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getListMethod(), getCallOptions()), request, responseObserver);
    }
  }

  /**
   * A stub to allow clients to do synchronous rpc calls to service Ibsen.
   */
  public static final class IbsenBlockingStub
      extends io.grpc.stub.AbstractBlockingStub<IbsenBlockingStub> {
    private IbsenBlockingStub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected IbsenBlockingStub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new IbsenBlockingStub(channel, callOptions);
    }

    /**
     */
    public github.com.tcw.ibsen.client.WriteStatus write(github.com.tcw.ibsen.client.InputEntries request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getWriteMethod(), getCallOptions(), request);
    }

    /**
     */
    public java.util.Iterator<github.com.tcw.ibsen.client.OutputEntries> read(
        github.com.tcw.ibsen.client.ReadParams request) {
      return io.grpc.stub.ClientCalls.blockingServerStreamingCall(
          getChannel(), getReadMethod(), getCallOptions(), request);
    }

    /**
     */
    public github.com.tcw.ibsen.client.TopicList list(github.com.tcw.ibsen.client.EmptyArgs request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getListMethod(), getCallOptions(), request);
    }
  }

  /**
   * A stub to allow clients to do ListenableFuture-style rpc calls to service Ibsen.
   */
  public static final class IbsenFutureStub
      extends io.grpc.stub.AbstractFutureStub<IbsenFutureStub> {
    private IbsenFutureStub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected IbsenFutureStub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new IbsenFutureStub(channel, callOptions);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<github.com.tcw.ibsen.client.WriteStatus> write(
        github.com.tcw.ibsen.client.InputEntries request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getWriteMethod(), getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<github.com.tcw.ibsen.client.TopicList> list(
        github.com.tcw.ibsen.client.EmptyArgs request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getListMethod(), getCallOptions()), request);
    }
  }

  private static final int METHODID_WRITE = 0;
  private static final int METHODID_READ = 1;
  private static final int METHODID_LIST = 2;

  private static final class MethodHandlers<Req, Resp> implements
      io.grpc.stub.ServerCalls.UnaryMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.ServerStreamingMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.ClientStreamingMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.BidiStreamingMethod<Req, Resp> {
    private final AsyncService serviceImpl;
    private final int methodId;

    MethodHandlers(AsyncService serviceImpl, int methodId) {
      this.serviceImpl = serviceImpl;
      this.methodId = methodId;
    }

    @java.lang.Override
    @java.lang.SuppressWarnings("unchecked")
    public void invoke(Req request, io.grpc.stub.StreamObserver<Resp> responseObserver) {
      switch (methodId) {
        case METHODID_WRITE:
          serviceImpl.write((github.com.tcw.ibsen.client.InputEntries) request,
              (io.grpc.stub.StreamObserver<github.com.tcw.ibsen.client.WriteStatus>) responseObserver);
          break;
        case METHODID_READ:
          serviceImpl.read((github.com.tcw.ibsen.client.ReadParams) request,
              (io.grpc.stub.StreamObserver<github.com.tcw.ibsen.client.OutputEntries>) responseObserver);
          break;
        case METHODID_LIST:
          serviceImpl.list((github.com.tcw.ibsen.client.EmptyArgs) request,
              (io.grpc.stub.StreamObserver<github.com.tcw.ibsen.client.TopicList>) responseObserver);
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

  public static final io.grpc.ServerServiceDefinition bindService(AsyncService service) {
    return io.grpc.ServerServiceDefinition.builder(getServiceDescriptor())
        .addMethod(
          getWriteMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              github.com.tcw.ibsen.client.InputEntries,
              github.com.tcw.ibsen.client.WriteStatus>(
                service, METHODID_WRITE)))
        .addMethod(
          getReadMethod(),
          io.grpc.stub.ServerCalls.asyncServerStreamingCall(
            new MethodHandlers<
              github.com.tcw.ibsen.client.ReadParams,
              github.com.tcw.ibsen.client.OutputEntries>(
                service, METHODID_READ)))
        .addMethod(
          getListMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              github.com.tcw.ibsen.client.EmptyArgs,
              github.com.tcw.ibsen.client.TopicList>(
                service, METHODID_LIST)))
        .build();
  }

  private static abstract class IbsenBaseDescriptorSupplier
      implements io.grpc.protobuf.ProtoFileDescriptorSupplier, io.grpc.protobuf.ProtoServiceDescriptorSupplier {
    IbsenBaseDescriptorSupplier() {}

    @java.lang.Override
    public com.google.protobuf.Descriptors.FileDescriptor getFileDescriptor() {
      return github.com.tcw.ibsen.client.IbsenProto.getDescriptor();
    }

    @java.lang.Override
    public com.google.protobuf.Descriptors.ServiceDescriptor getServiceDescriptor() {
      return getFileDescriptor().findServiceByName("Ibsen");
    }
  }

  private static final class IbsenFileDescriptorSupplier
      extends IbsenBaseDescriptorSupplier {
    IbsenFileDescriptorSupplier() {}
  }

  private static final class IbsenMethodDescriptorSupplier
      extends IbsenBaseDescriptorSupplier
      implements io.grpc.protobuf.ProtoMethodDescriptorSupplier {
    private final java.lang.String methodName;

    IbsenMethodDescriptorSupplier(java.lang.String methodName) {
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
      synchronized (IbsenGrpc.class) {
        result = serviceDescriptor;
        if (result == null) {
          serviceDescriptor = result = io.grpc.ServiceDescriptor.newBuilder(SERVICE_NAME)
              .setSchemaDescriptor(new IbsenFileDescriptorSupplier())
              .addMethod(getWriteMethod())
              .addMethod(getReadMethod())
              .addMethod(getListMethod())
              .build();
        }
      }
    }
    return result;
  }
}
