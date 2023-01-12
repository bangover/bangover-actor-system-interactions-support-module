package cloud.bangover.interactions.interactor;

import cloud.bangover.actors.Actor;
import cloud.bangover.actors.Actor.Context;
import cloud.bangover.actors.ActorAddress;
import cloud.bangover.actors.ActorName;
import cloud.bangover.actors.ActorSystem;
import cloud.bangover.actors.Actors;
import cloud.bangover.actors.Actors.SystemConfiguration;
import cloud.bangover.actors.Actors.SystemConfigurer;
import cloud.bangover.actors.Actors.SystemInitializer;
import cloud.bangover.actors.CorrelationKey;
import cloud.bangover.actors.EventLoop.Dispatcher;
import cloud.bangover.actors.FixedMessagesWaitingDispatcher;
import cloud.bangover.actors.Message;
import cloud.bangover.async.promises.MockErrorHandler;
import cloud.bangover.async.promises.MockResponseHandler;
import cloud.bangover.async.timer.Timeout;
import cloud.bangover.async.timer.TimeoutException;
import cloud.bangover.async.timer.Timer;
import cloud.bangover.generators.StubGenerator;
import cloud.bangover.interactions.interactor.RequestReplyInteractor.Factory;
import lombok.NonNull;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class ActorSystemInteractorTest {
  private static final Timeout LONG_TIMEOUT = Timeout.ofSeconds(10L);
  private static final Timeout SHORT_TIMEOUT = Timeout.ofMilliseconds(10L);
  private static final ActorName PARSER_ACTOR_NAME = ActorName.wrap("LONG_PARSER");
  private static final ActorName STRING_PROVIDER_ACTOR_NAME = ActorName.wrap("STRING_PROVIDER");
  private static final CorrelationKey GENERATED_KEY = CorrelationKey.wrap("CORRELATED_KEY");

  @Test
  public void shouldIterationBeCompletedSuccessfully() throws Exception {
    // Given
    FixedMessagesWaitingDispatcher dispatcher =
        FixedMessagesWaitingDispatcher.singleThreadDispatcher(3);
    ActorSystem actorSystem = createActorSystem(dispatcher);
    actorSystem.start();
    ActorAddress actorAddress = actorSystem.actorOf(PARSER_ACTOR_NAME, new Actor.Factory<String>() {
      @Override
      public Actor<String> createActor(Context context) {
        return new LongParserActor(context);
      }
    });
    RequestReplyInteractor<String, Long> interactor =
        createInteractor(actorSystem, actorAddress, String.class, Long.class, LONG_TIMEOUT);
    MockResponseHandler<Long> resultListener = new MockResponseHandler<Long>();

    // When
    interactor.invoke("100").then(resultListener).await();
    actorSystem.shutdown();

    // Then
    resultListener.getHistory().hasEntry(0, 100L);
  }

  @Test
  public void shouldCompleteInteractionWithTimeoutError() throws Exception {
    // Given
    FixedMessagesWaitingDispatcher dispatcher =
        FixedMessagesWaitingDispatcher.singleThreadDispatcher(2);
    ActorSystem actorSystem = createActorSystem(dispatcher);
    actorSystem.start();
    ActorAddress actorAddress = actorSystem.actorOf(PARSER_ACTOR_NAME, new Actor.Factory<String>() {
      @Override
      public Actor<String> createActor(Context context) {
        return new LongParserActor(context);
      }
    });
    RequestReplyInteractor<String, Long> interactor =
        createInteractor(actorSystem, actorAddress, String.class, Long.class, SHORT_TIMEOUT);
    MockErrorHandler<Throwable> errorListener = new MockErrorHandler<Throwable>();
    // When
    interactor.invoke("100").error(errorListener).await();
    actorSystem.shutdown();
    // Then
    Assert.assertTrue(errorListener.getHistory().getEntry(0) instanceof TimeoutException);
  }

  @Test
  public void shouldCompleteIterationWithWrongRequestTypeError() throws Exception {
    // Given
    FixedMessagesWaitingDispatcher dispatcher =
        FixedMessagesWaitingDispatcher.singleThreadDispatcher(1);
    ActorSystem actorSystem = createActorSystem(dispatcher);
    actorSystem.start();
    ActorAddress actorAddress = actorSystem.actorOf(PARSER_ACTOR_NAME, new Actor.Factory<String>() {
      @Override
      public Actor<String> createActor(Context context) {
        return new LongParserActor(context);
      }
    });
    @SuppressWarnings("unchecked")
    RequestReplyInteractor<Long, Long> interactor =
        (RequestReplyInteractor<Long, Long>) ((Object) createInteractor(actorSystem, actorAddress, String.class,
            Long.class, LONG_TIMEOUT));
    MockErrorHandler<Throwable> errorListener = new MockErrorHandler<Throwable>();

    // When
    interactor.invoke(100L).error(errorListener).await();
    actorSystem.shutdown();

    // Then
    Assert.assertTrue(errorListener.getHistory().getEntry(0) instanceof WrongRequestTypeException);
  }

  @Test
  public void shouldCompleteIterationWithWrongResponseTypeError() throws Exception {
    // Given
    FixedMessagesWaitingDispatcher dispatcher =
        FixedMessagesWaitingDispatcher.singleThreadDispatcher(1);
    ActorSystem actorSystem = createActorSystem(dispatcher);
    actorSystem.start();
    ActorAddress actorAddress = actorSystem.actorOf(PARSER_ACTOR_NAME, new Actor.Factory<String>() {
      @Override
      public Actor<String> createActor(Context context) {
        return new LongParserActor(context);
      }
    });
    RequestReplyInteractor<String, String> interactor =
        createInteractor(actorSystem, actorAddress, String.class, String.class, LONG_TIMEOUT);
    MockErrorHandler<Throwable> errorListener = new MockErrorHandler<Throwable>();

    // When
    interactor.invoke("100").error(errorListener).await();
    actorSystem.shutdown();

    // Then
    Assert.assertTrue(errorListener.getHistory().getEntry(0) instanceof WrongResponseTypeException);
  }
  
  @Test
  public void shouldProvideString() throws Exception {
    // Given
    FixedMessagesWaitingDispatcher dispatcher =
        FixedMessagesWaitingDispatcher.singleThreadDispatcher(5);
    ActorSystem actorSystem = createActorSystem(dispatcher);
    actorSystem.start();
    ActorAddress actorAddress = actorSystem.actorOf(STRING_PROVIDER_ACTOR_NAME, new Actor.Factory<Object>() {
      @Override
      public Actor<Object> createActor(Context context) {
        return new StringProviderActor(context);
      }
    });
    ReplyOnlyInteractor<String> interactor =
    		createReplyOnlyInteractor(actorSystem, actorAddress, String.class, LONG_TIMEOUT);
    MockResponseHandler<String> responseHandler = new MockResponseHandler<String>();

    // When
    interactor.invoke().then(responseHandler).await();
    actorSystem.shutdown();

    // Then
    Assert.assertEquals("HELLO", responseHandler.getHistory().getEntry(0));
  }

  private ActorSystem createActorSystem(Dispatcher dispatcher) {
    return Actors.create(new SystemInitializer() {
      @Override
      public SystemConfiguration initializeSystem(SystemConfigurer configurer) {
        return configurer.withDispatcher(dispatcher)
            .withCorrelationKeyGenerator(new StubGenerator<CorrelationKey>(GENERATED_KEY))
            .configure();
      }
    });
  }

  private <Q, S> RequestReplyInteractor<Q, S> createInteractor(ActorSystem actorSystem,
      ActorAddress targetActor, Class<Q> requestType, Class<S> responseType, Timeout timeout) {
    Factory interactorFactory = ActorSystemInteractor.requestReplyFactory(actorSystem);
    TargetAddress targetAddress = TargetAddress.ofUrn(targetActor.toString());
    return interactorFactory.createInteractor(targetAddress, requestType, responseType, timeout);
  }
  
  private <S> ReplyOnlyInteractor<S> createReplyOnlyInteractor(ActorSystem actorSystem,
      ActorAddress targetActor, Class<S> responseType, Timeout timeout) {
	  ReplyOnlyInteractor.Factory interactorFactory = ActorSystemInteractor.replyOnlyFactory(actorSystem);
	  TargetAddress targetAddress = TargetAddress.ofUrn(targetActor.toString());
	  return interactorFactory.createInteractor(targetAddress, responseType, timeout);  
  }
  
  private static class StringProviderActor extends Actor<Object> {
    public StringProviderActor(@NonNull Context context) {
	  super(context);
    }
    
	@Override
    protected void receive(Message<Object> message) throws Throwable {
      tell(message.replyWith("HELLO"));
    }  
  }

  private static class LongParserActor extends Actor<String> {
    public LongParserActor(@NonNull Context context) {
      super(context);
    }

    @Override
    protected void receive(Message<String> message) throws Throwable {
      message.whenIsMatchedTo(String.class, new Message.MessageHandleFunction<String>() {
        @Override
        public void receive(String value) {
          Timer.sleep(100L);
          tell(message.replyWith(Long.parseLong(value)));
        }
      });
    }
  }
}
