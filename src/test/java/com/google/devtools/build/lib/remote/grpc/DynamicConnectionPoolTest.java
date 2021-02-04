package com.google.devtools.build.lib.remote.grpc;

import com.google.devtools.build.lib.remote.grpc.SharedConnectionFactory.SharedConnection;
import io.reactivex.rxjava3.core.Single;
import io.reactivex.rxjava3.observers.TestObserver;
import io.reactivex.rxjava3.plugins.RxJavaPlugins;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.Mock;

import java.io.IOException;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static com.google.common.truth.Truth.assertThat;
import static org.mockito.Mockito.*;
import static org.mockito.Mockito.times;
import static org.mockito.MockitoAnnotations.initMocks;

/** Tests for {@link DynamicConnectionPool}. */
@RunWith(JUnit4.class)
public class DynamicConnectionPoolTest {
  private final AtomicReference<Throwable> rxGlobalThrowable = new AtomicReference<>(null);

  @Mock private Connection connection0;
  @Mock private Connection connection1;
  @Mock private ConnectionFactory connectionFactory;
  private final AtomicInteger connectionFactoryCreateTimes = new AtomicInteger(0);

  @Before
  public void setUp() {
    RxJavaPlugins.setErrorHandler(rxGlobalThrowable::set);

    initMocks(this);

    when(connectionFactory.create()).thenAnswer(invocation -> {
      int times = connectionFactoryCreateTimes.getAndIncrement();
      if (times == 0) {
        return Single.just(connection0);
      } else {
        return Single.just(connection1);
      }
    });
  }

  @After
  public void tearDown() throws Throwable {
    // Make sure rxjava didn't receive global errors
    Throwable t = rxGlobalThrowable.getAndSet(null);
    if (t != null) {
      throw t;
    }
  }

  @Test
  public void create_smoke() {
    DynamicConnectionPool pool = new DynamicConnectionPool(connectionFactory, 1);

    TestObserver<SharedConnection> observer = pool.create().test();

    observer.assertValue(conn -> conn.getUnderlyingConnection() == connection0).assertComplete();
    assertThat(connectionFactoryCreateTimes.get()).isEqualTo(1);
  }

  @Test
  public void create_exceedingMaxConcurrent_createNewConnection() {
    DynamicConnectionPool pool = new DynamicConnectionPool(connectionFactory, 1);

    TestObserver<SharedConnection> observer0 = pool.create().test();
    TestObserver<SharedConnection> observer1 = pool.create().test();

    observer0.assertValue(conn -> conn.getUnderlyingConnection() == connection0).assertComplete();
    observer1.assertValue(conn -> conn.getUnderlyingConnection() == connection1).assertComplete();
    assertThat(connectionFactoryCreateTimes.get()).isEqualTo(2);
  }

  @Test
  public void create_pendingConnectionCreationAndExceedingMaxConcurrent_createNewConnection() {
    AtomicBoolean terminated = new AtomicBoolean(false);
    ConnectionFactory connectionFactory = mock(ConnectionFactory.class);
    when(connectionFactory.create())
        .thenAnswer(
            invocation -> {
              if (connectionFactoryCreateTimes.getAndIncrement() == 0) {
                return Single.create(emitter -> {
                  Thread t = new Thread(
                      () -> {
                        try {
                          Thread.sleep(Integer.MAX_VALUE);
                          emitter.onSuccess(connection0);
                        } catch (InterruptedException e) {
                          emitter.onError(e);
                        }
                        terminated.set(true);
                      });
                  t.start();
                });
              } else {
                return Single.just(connection1);
              }
            });
    DynamicConnectionPool pool = new DynamicConnectionPool(connectionFactory, 1);

    TestObserver<SharedConnection> observer0 = pool.create().test();
    TestObserver<SharedConnection> observer1 = pool.create().test();

    assertThat(terminated.get()).isFalse();
    observer0.assertEmpty();
    observer1.assertValue(conn -> conn.getUnderlyingConnection() == connection1).assertComplete();
    assertThat(connectionFactoryCreateTimes.get()).isEqualTo(2);
  }

  @Test
  public void create_belowMaxConcurrency_shareConnections() {
    DynamicConnectionPool pool = new DynamicConnectionPool(connectionFactory, 2);

    TestObserver<SharedConnection> observer0 = pool.create().test();
    TestObserver<SharedConnection> observer1 = pool.create().test();

    observer0.assertValue(conn -> conn.getUnderlyingConnection() == connection0).assertComplete();
    observer1.assertValue(conn -> conn.getUnderlyingConnection() == connection0).assertComplete();
    assertThat(connectionFactoryCreateTimes.get()).isEqualTo(1);
  }

  @Test
  public void create_afterConnectionClosed_shareConnections() throws IOException {
    DynamicConnectionPool pool = new DynamicConnectionPool(connectionFactory, 1);
    TestObserver<SharedConnection> observer0 = pool.create().test();
    observer0.assertValue(conn -> conn.getUnderlyingConnection() == connection0).assertComplete();
    observer0.values().get(0).close();

    TestObserver<SharedConnection> observer1 = pool.create().test();

    observer1.assertValue(conn -> conn.getUnderlyingConnection() == connection0).assertComplete();
    assertThat(connectionFactoryCreateTimes.get()).isEqualTo(1);
  }

  @Test
  public void closePool_noNewConnectionAllowed() throws IOException {
    DynamicConnectionPool pool = new DynamicConnectionPool(connectionFactory, 1);
    pool.close();

    TestObserver<SharedConnection> observer = pool.create().test();

    observer.assertError(e -> e instanceof IllegalStateException && e.getMessage().contains("closed"));
  }

  @Test
  public void closePool_closeUnderlyingConnection() throws IOException {
    DynamicConnectionPool pool = new DynamicConnectionPool(connectionFactory, 1);
    TestObserver<SharedConnection> observer = pool.create().test();
    observer.assertComplete();

    pool.close();

    verify(connection0, times(1)).close();
  }

  @Test
  public void closePool_pendingConnectionCreation_closedError() throws IOException, InterruptedException {
    AtomicBoolean canceled = new AtomicBoolean(false);
    AtomicBoolean finished = new AtomicBoolean(false);
    Semaphore terminated = new Semaphore(0);
    ConnectionFactory connectionFactory = mock(ConnectionFactory.class);
    when(connectionFactory.create())
        .thenAnswer(
            invocation ->
                Single.create(
                    emitter -> {
                      Thread t =
                          new Thread(
                              () -> {
                                try {
                                  Thread.sleep(Integer.MAX_VALUE);
                                  finished.set(true);
                                  emitter.onSuccess(connection0);
                                } catch (InterruptedException ignored) {
                                  /* no-op */
                                }

                                terminated.release();
                              });
                      t.start();

                      emitter.setCancellable(t::interrupt);
                    })
                    .doOnDispose(() -> canceled.set(true)));
    DynamicConnectionPool pool = new DynamicConnectionPool(connectionFactory, 1);
    TestObserver<SharedConnection> observer = pool.create().test();
    observer.assertEmpty();

    assertThat(canceled.get()).isFalse();
    pool.close();

    terminated.acquire();
    observer.assertError(e -> e instanceof IllegalStateException && e.getMessage().contains("closed"));
    assertThat(canceled.get()).isTrue();
    assertThat(finished.get()).isFalse();
  }
}
