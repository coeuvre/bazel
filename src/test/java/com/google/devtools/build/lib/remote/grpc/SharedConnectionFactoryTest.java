package com.google.devtools.build.lib.remote.grpc;

import static com.google.common.truth.Truth.assertThat;
import static org.mockito.Mockito.*;
import static org.mockito.MockitoAnnotations.initMocks;

import com.google.devtools.build.lib.remote.grpc.SharedConnectionFactory.SharedConnection;
import io.reactivex.rxjava3.core.Single;
import io.reactivex.rxjava3.observers.TestObserver;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.Mock;

import java.io.IOException;

/** Tests for {@link SharedConnectionFactory}. */
@RunWith(JUnit4.class)
public class SharedConnectionFactoryTest {

  @Mock private Connection connection;
  @Mock private ConnectionFactory connectionFactory;

  @Before
  public void setUp() {
    initMocks(this);

    when(connectionFactory.create()).thenAnswer(invocation -> Single.just(connection));
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
