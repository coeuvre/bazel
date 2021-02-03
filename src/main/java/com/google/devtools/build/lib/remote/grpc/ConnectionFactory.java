package com.google.devtools.build.lib.remote.grpc;

import io.reactivex.rxjava3.core.Single;

/**
 * A {@link ConnectionFactory} represents a resource factory for channel creation. It may create
 * channels by itself, wrap a {@link ConnectionFactory}, or apply channel pooling on top of a {@link
 * ConnectionFactory}.
 *
 * <p>A {@link ConnectionFactory} uses deferred initialization and should initiate connection
 * resource allocation after subscription.
 *
 * <p>Connection creation must be cancellable. Canceling connection creation must release (“close”)
 * the connection and all associated resources.
 */
public interface ConnectionFactory {
  Single<? extends Connection> create();
}
