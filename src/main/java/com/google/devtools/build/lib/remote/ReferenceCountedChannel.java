// Copyright 2018 The Bazel Authors. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package com.google.devtools.build.lib.remote;

import com.google.devtools.build.lib.remote.grpc.ChannelConnectionFactory;
import com.google.devtools.build.lib.remote.grpc.ChannelConnectionFactory.ChannelConnection;
import com.google.devtools.build.lib.remote.grpc.DynamicConnectionPool;
import com.google.devtools.build.lib.remote.grpc.SharedConnectionFactory.SharedConnection;
import io.grpc.*;
import io.netty.util.AbstractReferenceCounted;
import io.netty.util.ReferenceCounted;

import java.io.IOException;

/**
 * A wrapper around a {@link io.grpc.ManagedChannel} exposing a reference count. When instantiated
 * the reference count is 1. {@link ManagedChannel#shutdown()} will be called on the wrapped channel
 * when the reference count reaches 0.
 *
 * <p>See {@link ReferenceCounted} for more information about reference counting.
 */
public class ReferenceCountedChannel extends Channel implements ReferenceCounted {
  private final DynamicConnectionPool dynamicConnectionPool;
  private final AbstractReferenceCounted referenceCounted =
      new AbstractReferenceCounted() {
        @Override
        protected void deallocate() {
          try {
            dynamicConnectionPool.close();
          } catch (IOException e) {
            throw new AssertionError(e.getMessage(), e);
          }
        }

        @Override
        public ReferenceCounted touch(Object o) {
          return this;
        }
      };

  protected ReferenceCountedChannel(ChannelConnectionFactory connectionFactory) {
    this.dynamicConnectionPool =
        new DynamicConnectionPool(connectionFactory, connectionFactory.maxConcurrency());
  }

  static class ConnectionCleanupCall<ReqT, RespT>
      extends ForwardingClientCall.SimpleForwardingClientCall<ReqT, RespT> {
    private final SharedConnection connection;

    protected ConnectionCleanupCall(ClientCall<ReqT, RespT> delegate, SharedConnection connection) {
      super(delegate);
      this.connection = connection;
    }

    @Override
    public void start(Listener<RespT> responseListener, Metadata headers) {
      super.start(
          new ForwardingClientCallListener.SimpleForwardingClientCallListener<RespT>(
              responseListener) {
            @Override
            public void onClose(Status status, Metadata trailers) {
              super.onClose(status, trailers);
              try {
                connection.close();
              } catch (IOException e) {
                throw new AssertionError(e.getMessage(), e);
              }
            }
          },
          headers);
    }
  }

  @Override
  public <RequestT, ResponseT> ClientCall<RequestT, ResponseT> newCall(
      MethodDescriptor<RequestT, ResponseT> methodDescriptor, CallOptions callOptions) {
    SharedConnection sharedConnection = dynamicConnectionPool.create().blockingGet();
    ChannelConnection connection = (ChannelConnection) sharedConnection.getUnderlyingConnection();
    return new ConnectionCleanupCall<>(
        connection.getChannel().newCall(methodDescriptor, callOptions), sharedConnection);
  }

  @Override
  public String authority() {
    SharedConnection sharedConnection = dynamicConnectionPool.create().blockingGet();
    ChannelConnection connection = (ChannelConnection) sharedConnection.getUnderlyingConnection();
    return connection.getChannel().authority();
  }

  @Override
  public int refCnt() {
    return referenceCounted.refCnt();
  }

  @Override
  public ReferenceCountedChannel retain() {
    referenceCounted.retain();
    return this;
  }

  @Override
  public ReferenceCountedChannel retain(int increment) {
    referenceCounted.retain(increment);
    return this;
  }

  @Override
  public ReferenceCounted touch() {
    referenceCounted.touch();
    return this;
  }

  @Override
  public ReferenceCounted touch(Object hint) {
    referenceCounted.touch(hint);
    return this;
  }

  @Override
  public boolean release() {
    return referenceCounted.release();
  }

  @Override
  public boolean release(int decrement) {
    return referenceCounted.release(decrement);
  }
}
