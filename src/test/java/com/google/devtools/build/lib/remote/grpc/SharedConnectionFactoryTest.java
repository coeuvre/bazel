package com.google.devtools.build.lib.remote.grpc;

import static com.google.common.truth.Truth.assertThat;
import static org.mockito.Mockito.*;
import static org.mockito.MockitoAnnotations.initMocks;

import com.google.devtools.build.lib.remote.grpc.SharedConnectionFactory.SharedConnection;
import io.reactivex.rxjava3.core.Single;
import io.reactivex.rxjava3.observers.TestObserver;
import io.reactivex.rxjava3.plugins.RxJavaPlugins;
import io.reactivex.rxjava3.schedulers.Schedulers;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.Mock;

import java.io.IOException;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/** Tests for {@link SharedConnectionFactory}. */
@RunWith(JUnit4.class)
public class SharedConnectionFactoryTest {
  private final AtomicReference<Throwable> rxGlobalThrowable = new AtomicReference<>(null);

  @Mock private Connection connection;
  @Mock private ConnectionFactory connectionFactory;

  @Before
  public void setUp() {
    RxJavaPlugins.setErrorHandler(rxGlobalThrowable::set);

    initMocks(this);

    when(connectionFactory.create()).thenAnswer(invocation -> Single.just(connection));
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
    SharedConnectionFactory factory = new SharedConnectionFactory(connectionFactory, 1);
    assertThat(factory.numAvailableConnections()).isEqualTo(1);

    TestObserver<SharedConnection> observer = factory.create().test();

    observer.assertValue(conn -> conn.getUnderlyingConnection() == connection).assertComplete();
    assertThat(factory.numAvailableConnections()).isEqualTo(0);
  }

  @Test
  public void create_noConnectionCreationBeforeSubscription() {
    SharedConnectionFactory factory = new SharedConnectionFactory(connectionFactory, 1);

    factory.create();

    verify(connectionFactory, times(0)).create();
  }

  @Test
  public void create_pendingWhenExceedingMaxConcurrency() {
    SharedConnectionFactory factory = new SharedConnectionFactory(connectionFactory, 1);
    TestObserver<SharedConnection> observer1 = factory.create().test();
    assertThat(factory.numAvailableConnections()).isEqualTo(0);
    observer1.assertValue(conn -> conn.getUnderlyingConnection() == connection).assertComplete();

    TestObserver<SharedConnection> observer2 = factory.create().test();
    observer2.assertEmpty();
  }

  @Test
  public void create_afterConnectionClosed_shareConnections() throws IOException {
    SharedConnectionFactory factory = new SharedConnectionFactory(connectionFactory, 1);
    TestObserver<SharedConnection> observer1 = factory.create().test();
    assertThat(factory.numAvailableConnections()).isEqualTo(0);
    observer1.assertValue(conn -> conn.getUnderlyingConnection() == connection).assertComplete();
    TestObserver<SharedConnection> observer2 = factory.create().test();

    observer1.values().get(0).close();

    observer2.assertValue(conn -> conn.getUnderlyingConnection() == connection).assertComplete();
    assertThat(factory.numAvailableConnections()).isEqualTo(0);
  }

  @Test
  public void create_belowMaxConcurrency_shareConnections() {
    SharedConnectionFactory factory = new SharedConnectionFactory(connectionFactory, 2);

    TestObserver<SharedConnection> observer1 = factory.create().test();
    assertThat(factory.numAvailableConnections()).isEqualTo(1);
    observer1.assertValue(conn -> conn.getUnderlyingConnection() == connection).assertComplete();

    TestObserver<SharedConnection> observer2 = factory.create().test();
    observer2.assertValue(conn -> conn.getUnderlyingConnection() == connection).assertComplete();
    assertThat(factory.numAvailableConnections()).isEqualTo(0);
  }

  @Test
  public void create_concurrentCreate_shareConnections() throws InterruptedException {
    SharedConnectionFactory factory = new SharedConnectionFactory(connectionFactory, 2);
    Semaphore semaphore = new Semaphore(0);
    AtomicBoolean finished = new AtomicBoolean(false);
    Thread t =
        new Thread(
            () -> {
              factory
                  .create()
                  .doOnSuccess(
                      conn -> {
                        assertThat(conn.getUnderlyingConnection()).isEqualTo(connection);
                        semaphore.release();
                        Thread.sleep(Integer.MAX_VALUE);
                        finished.set(true);
                      })
                  .blockingSubscribe();

              finished.set(true);
            });
    t.start();
    semaphore.acquire();

    TestObserver<SharedConnection> observer = factory.create().test();

    observer.assertValue(conn -> conn.getUnderlyingConnection() == connection).assertComplete();
    assertThat(finished.get()).isFalse();
  }

  @Test
  public void create_dispose_cancelCreation() {
    AtomicBoolean disposed = new AtomicBoolean(false);
    AtomicBoolean finished = new AtomicBoolean(false);
    ConnectionFactory connectionFactory = mock(ConnectionFactory.class);
    when(connectionFactory.create())
        .thenAnswer(
            invocation ->
                Single.create(
                        emitter ->
                            new Thread(
                                    () -> {
                                      try {
                                        Thread.sleep(Integer.MAX_VALUE);
                                        finished.set(true);
                                        emitter.onSuccess(connectionFactory);
                                      } catch (InterruptedException e) {
                                        emitter.onError(e);
                                      }
                                    })
                                .start())
                    .doOnDispose(() -> disposed.set(true)));
    SharedConnectionFactory factory = new SharedConnectionFactory(connectionFactory, 1);
    TestObserver<SharedConnection> observer = factory.create().test();

    observer.assertEmpty().dispose();

    assertThat(disposed.get()).isTrue();
    assertThat(finished.get()).isFalse();
  }

  @Test
  public void create_interrupt_terminate() throws InterruptedException {
    AtomicBoolean finished = new AtomicBoolean(false);
    AtomicBoolean interrupted = new AtomicBoolean(true);
    Semaphore threadTerminatedSemaphore = new Semaphore(0);
    ConnectionFactory connectionFactory = mock(ConnectionFactory.class);
    when(connectionFactory.create())
        .thenAnswer(
            invocation ->
                Single.create(
                    emitter ->
                        new Thread(
                                () -> {
                                  try {
                                    Thread.sleep(Integer.MAX_VALUE);
                                    finished.set(true);
                                    emitter.onSuccess(connectionFactory);
                                  } catch (InterruptedException e) {
                                    emitter.onError(e);
                                  }
                                })
                            .start()));
    SharedConnectionFactory factory = new SharedConnectionFactory(connectionFactory, 2);
    factory.create().test().assertEmpty();
    Thread t =
        new Thread(
            () -> {
              try {
                SharedConnection ignored = factory.create().blockingGet();
              } catch (RuntimeException e) {
                if (e.getCause() instanceof InterruptedException) {
                  interrupted.set(true);
                } else {
                  throw e;
                }
              }

              threadTerminatedSemaphore.release();
            });
    t.start();

    // busy wait
    while (true) {
      if (factory.getConnectionLock().getQueueLength() > 0) {
        break;
      }
    }
    t.interrupt();
    threadTerminatedSemaphore.acquire();

    assertThat(finished.get()).isFalse();
    assertThat(interrupted.get()).isTrue();
  }

  @Test
  public void closeConnection_connectionBecomeAvailable() throws IOException {
    SharedConnectionFactory factory = new SharedConnectionFactory(connectionFactory, 1);
    TestObserver<SharedConnection> observer = factory.create().test();
    observer.assertComplete();
    SharedConnection conn = observer.values().get(0);
    assertThat(factory.numAvailableConnections()).isEqualTo(0);

    conn.close();

    assertThat(factory.numAvailableConnections()).isEqualTo(1);
    verify(connection, times(0)).close();
  }

  @Test
  public void closeFactory_closeUnderlyingConnection() throws IOException {
    SharedConnectionFactory factory = new SharedConnectionFactory(connectionFactory, 1);
    TestObserver<SharedConnection> observer = factory.create().test();
    observer.assertComplete();

    factory.close();

    verify(connection, times(1)).close();
  }

  @Test
  public void closeFactory_noNewConnectionAllowed() throws IOException {
    SharedConnectionFactory factory = new SharedConnectionFactory(connectionFactory, 1);
    factory.close();

    TestObserver<SharedConnection> observer = factory.create().test();

    observer.assertError(error -> error.getMessage().contains("closed"));
  }
}
