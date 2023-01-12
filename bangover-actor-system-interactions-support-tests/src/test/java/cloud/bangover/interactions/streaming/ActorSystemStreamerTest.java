package cloud.bangover.interactions.streaming;

import cloud.bangover.MockHistory;
import cloud.bangover.actors.ActorSystem;
import cloud.bangover.actors.Actors;
import cloud.bangover.actors.Actors.SystemConfiguration;
import cloud.bangover.actors.Actors.SystemConfigurer;
import cloud.bangover.actors.CorrelationKey;
import cloud.bangover.actors.EventLoop.Dispatcher;
import cloud.bangover.actors.SingleThreadDispatcher;
import cloud.bangover.generators.StubGenerator;
import cloud.bangover.interactions.streaming.Stream.Stat;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class ActorSystemStreamerTest {
  private static final CorrelationKey GENERATED_KEY = CorrelationKey.wrap("CORRELATED_KEY");

  private ActorSystem actorSyetem;

  @Before
  public void setUp() {
    Dispatcher dispatcher = new SingleThreadDispatcher();
    this.actorSyetem = Actors.create(new Actors.SystemInitializer() {
      @Override
      public SystemConfiguration initializeSystem(SystemConfigurer configurer) {
        return configurer
            .withDispatcher(dispatcher)
            .withCorrelationKeyGenerator(new StubGenerator<CorrelationKey>(GENERATED_KEY))
            .configure();
      }
    });
    this.actorSyetem.start();
  }

  @Test
  public void shouldTransferDataFromEverySource() throws Throwable {
    // Given
    Streamer streamer = new ActorSystemStreamer(actorSyetem);
    MockSource<Integer> source = new MockSource<Integer>();
    MockDestination<Integer> destination = new MockDestination<Integer>();
    source.configureReadingIterations()
      .withNextEntry(new SubmitIteation<Integer>(1, 1))
      .withNextEntry(new SubmitIteation<Integer>(2, 1));
    Stream<Integer> stream = streamer.createStream(source, destination);
    // When
    Stat status = stream.start().get(1000L);
    // Then
    MockHistory<SubmitIteation<Integer>> submitIterations = destination.getHistory();
    Assert.assertTrue(destination.isReleased());
    Assert.assertTrue(submitIterations.hasEntry(0, new SubmitIteation<Integer>(1, 1)));
    Assert.assertTrue(submitIterations.hasEntry(1, new SubmitIteation<Integer>(2, 1)));
    Assert.assertEquals((Long) 2L, status.getSize());
  }
  
  @After
  public void tearDown() {
    actorSyetem.shutdown();
  }
}
