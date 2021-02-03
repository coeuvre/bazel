package com.google.devtools.build.lib.remote.grpc;

import com.google.auto.value.AutoValue;
import com.google.devtools.build.lib.authandtls.AuthAndTLSOptions;
import io.grpc.ClientInterceptor;

import java.util.List;

@AutoValue
public abstract class ConnectionFactoryOptions {
  abstract String target();
  abstract String proxy();
  abstract AuthAndTLSOptions options();
  abstract List<ClientInterceptor> interceptors();
}
