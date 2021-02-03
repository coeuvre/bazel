package com.google.devtools.build.lib.remote.grpc;

import java.io.Closeable;
import java.io.IOException;

public interface PooledConnectionFactory extends ConnectionFactory, Closeable {
  @Override
  void close() throws IOException;
}
