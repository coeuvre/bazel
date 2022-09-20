package com.google.devtools.build.lib.remote;

import com.google.devtools.build.lib.profiler.Profiler;
import com.google.devtools.build.lib.profiler.Profiler.ActiveProfileTask;
import com.google.devtools.build.lib.profiler.ProfilerTask;
import io.grpc.CallOptions;
import io.grpc.Channel;
import io.grpc.ClientCall;
import io.grpc.ClientInterceptor;
import io.grpc.ForwardingClientCall.SimpleForwardingClientCall;
import io.grpc.ForwardingClientCallListener.SimpleForwardingClientCallListener;
import io.grpc.Metadata;
import io.grpc.MethodDescriptor;
import io.grpc.Status;
import java.util.NoSuchElementException;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.atomic.AtomicLong;

public class GrpcProfilingInterceptor implements ClientInterceptor {

  @Override
  public <ReqT, RespT> ClientCall<ReqT, RespT> interceptCall(
      MethodDescriptor<ReqT, RespT> methodDescriptor, CallOptions callOptions, Channel channel) {
    ClientCall<ReqT, RespT> clientCall = channel.newCall(methodDescriptor, callOptions);
    return new GrpcProfilingCall<>(clientCall, methodDescriptor.getBareMethodName());
  }

  private final ConcurrentLinkedDeque<Long> freeFakeThreadIds = new ConcurrentLinkedDeque<>();
  // Pick random large enough value that won't overlap with real thread id as the base.
  private static final long FAKE_THREAD_ID_BASE = 1000000000000L;
  private final AtomicLong nextFakeThreadId = new AtomicLong(FAKE_THREAD_ID_BASE);

  private long acquireNextFakeThreadId() {
    try {
      return freeFakeThreadIds.removeFirst();
    } catch (NoSuchElementException ignored) {
      long fakeThreadId = nextFakeThreadId.getAndIncrement();
      Profiler.instance()
          .defineThread(
              fakeThreadId,
              "remote-execution-" + (fakeThreadId - FAKE_THREAD_ID_BASE),
              String.valueOf(FAKE_THREAD_ID_BASE));
      return fakeThreadId;
    }
  }

  private void releaseFakeThreadId(long threadId) {
    freeFakeThreadIds.addFirst(threadId);
  }

  private class GrpcProfilingCall<ReqT, RespT> extends SimpleForwardingClientCall<ReqT, RespT> {
    private final String desc;

    protected GrpcProfilingCall(ClientCall<ReqT, RespT> delegate, String desc) {
      super(delegate);
      this.desc = desc;
    }

    @Override
    public void start(Listener<RespT> responseListener, Metadata headers) {
      ActiveProfileTask s = Profiler.instance().profile(ProfilerTask.REMOTE_NETWORK, desc);
      long fakeThreadId = acquireNextFakeThreadId();
      s.setThreadId(fakeThreadId);

      super.start(
          new SimpleForwardingClientCallListener<RespT>(responseListener) {
            @Override
            public void onClose(Status status, Metadata trailers) {
              try {
                super.onClose(status, trailers);
              } finally {
                s.close();
                releaseFakeThreadId(fakeThreadId);
              }
            }
          },
          headers);
    }
  }
}
