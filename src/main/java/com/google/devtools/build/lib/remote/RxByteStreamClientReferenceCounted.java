package com.google.devtools.build.lib.remote;

import io.netty.util.ReferenceCounted;

/**
 * An {@link RxByteStreamClient} which is {@link ReferenceCounted}.
 */
public interface RxByteStreamClientReferenceCounted extends RxByteStreamClient, ReferenceCounted {
  @Override
  RxByteStreamClientReferenceCounted retain();
}
