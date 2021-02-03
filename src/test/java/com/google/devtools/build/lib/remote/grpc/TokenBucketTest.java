package com.google.devtools.build.lib.remote.grpc;

import static com.google.common.truth.Truth.assertThat;

import io.reactivex.rxjava3.observers.TestObserver;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.io.IOException;

/** Tests for {@link TokenBucket}. */
@RunWith(JUnit4.class)
public class TokenBucketTest {

  @Test
  public void acquireToken_smoke() {
    TokenBucket<Integer> bucket = new TokenBucket<>();
    assertThat(bucket.size()).isEqualTo(0);
    bucket.addToken(0);
    assertThat(bucket.size()).isEqualTo(1);

    TestObserver<Integer> observer = bucket.acquireToken().test();

    observer.assertValue(0).assertComplete();
    assertThat(bucket.size()).isEqualTo(0);
  }

  @Test
  public void acquireToken_releaseTokenToPreviousObserver() {
    TokenBucket<Integer> bucket = new TokenBucket<>();
    TestObserver<Integer> observer = bucket.acquireToken().test();
    observer.assertEmpty();

    bucket.addToken(0);

    observer.assertValue(0).assertComplete();
    assertThat(bucket.size()).isEqualTo(0);
  }

  @Test
  public void acquireToken_notReleaseTokenToDisposedObserver() {
    TokenBucket<Integer> bucket = new TokenBucket<>();
    TestObserver<Integer> observer = bucket.acquireToken().test();

    observer.dispose();
    bucket.addToken(0);

    observer.assertEmpty();
    assertThat(bucket.size()).isEqualTo(1);
  }

  @Test
  public void acquireToken_disposeAfterTokenAcquired() {
    TokenBucket<Integer> bucket = new TokenBucket<>();
    TestObserver<Integer> observer = bucket.acquireToken().test();

    bucket.addToken(0);
    bucket.addToken(1);

    observer.assertValue(0).assertComplete();
    assertThat(bucket.size()).isEqualTo(1);
  }

  @Test
  public void acquireToken_multipleObservers_onlyOneCanAcquire() {
    TokenBucket<Integer> bucket = new TokenBucket<>();
    TestObserver<Integer> observer1 = bucket.acquireToken().test();
    TestObserver<Integer> observer2 = bucket.acquireToken().test();

    bucket.addToken(0);

    if (observer1.values().size() != 0) {
      observer1.assertValue(0).assertComplete();
      observer2.assertEmpty();

      bucket.addToken(1);
      observer2.assertValue(1).assertComplete();
    } else {
      observer1.assertEmpty();
      observer2.assertValue(0).assertComplete();

      bucket.addToken(1);
      observer1.assertValue(1).assertComplete();
    }
  }

  @Test
  public void close_errorAfterClose() throws IOException {
    TokenBucket<Integer> bucket = new TokenBucket<>();
    bucket.close();

    TestObserver<Integer> observer = bucket.acquireToken().test();

    observer.assertError(e -> e.getMessage().contains("closed"));
  }

  @Test
  public void close_errorPreviousObservers() throws IOException {
    TokenBucket<Integer> bucket = new TokenBucket<>();
    TestObserver<Integer> observer = bucket.acquireToken().test();

    bucket.close();

    observer.assertError(e -> e.getMessage().contains("closed"));
  }
}
