package cloud.bangover.interactions.interactor;

import cloud.bangover.actors.Actor;
import cloud.bangover.actors.ActorAddress;
import cloud.bangover.actors.ActorName;
import cloud.bangover.actors.ActorSystem;
import cloud.bangover.actors.CorrelationKey;
import cloud.bangover.actors.Message;
import cloud.bangover.async.promises.Deferred;
import cloud.bangover.async.promises.Promise;
import cloud.bangover.async.promises.Promises;
import cloud.bangover.async.timer.Timeout;
import cloud.bangover.async.timer.TimeoutException;
import cloud.bangover.async.timer.TimeoutSupervisor;
import cloud.bangover.async.timer.Timer;

import java.util.Optional;
import java.util.UUID;

import lombok.AccessLevel;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;

/**
 * This class is the intractor implementation for communicating with the random actor, registered in
 * the actor system. It will register intermediate actor into the actor system and communicate with
 * another actors via messages {@link Message}. Requests and responses is correlated using
 * correlation key.
 *
 * @author Dmitry Mikhaylenko
 *
 * @param <Q> The request type name
 * @param <S> The response type name
 */
@RequiredArgsConstructor(access = AccessLevel.PRIVATE)
public final class ActorSystemInteractor<Q, S> implements RequestReplyInteractor<Q, S> {
  private final ActorSystem actorSystem;
  private final Class<Q> requestType;
  private final Class<S> repsponseType;
  private final TargetAddress target;
  private final Timeout timeout;

  /**
   * Get the actor system request-reply interactor factory.
   *
   * @param actorSystem The actor system
   * @return The interactor factory
   */
  public static final RequestReplyInteractor.Factory requestReplyFactory(
      @NonNull ActorSystem actorSystem) {
    return new ActorRequestReplyInteractorFactory(actorSystem);
  }

  /**
   * Get the actor system reply-only interactor factory.
   *
   * @param actorSystem The actor system
   * @return The interactor factory
   */
  public static final ReplyOnlyInteractor.Factory replyOnlyFactory(
      @NonNull ActorSystem actorSystem) {
    RequestReplyInteractor.Factory requestReplyFactory = requestReplyFactory(actorSystem);
    return new ReplyOnlyInteractor.Factory() {
      @Override
      public <S> ReplyOnlyInteractor<S> createInteractor(TargetAddress target,
          Class<S> responseType, Timeout timeout) {
        RequestReplyInteractor<Void, S> requestReplyInteractor =
            requestReplyFactory.createInteractor(target, Void.class, responseType, timeout);
        return new ReplyOnlyInteractor<S>() {
          @Override
          public Promise<S> invoke() {
            return requestReplyInteractor.invoke(null);
          }
        };
      }
    };
  }

  @Override
  public final Promise<S> invoke(Q request) {
    return Promises.of(resolver -> {
      ActorAddress interactorActor = actorSystem.actorOf(generateActorName(),
          context -> new InteractorActor(context, createTargetAddress(target), timeout, resolver));
      actorSystem.tell(Message.createFor(interactorActor, request));
    });
  }

  private ActorAddress createTargetAddress(TargetAddress target) {
    return ActorAddress.ofUrn(target.toString());
  }

  private ActorName generateActorName() {
    return ActorName.wrap(String.format("INTERACTION--%s", UUID.randomUUID()));
  }

  @RequiredArgsConstructor
  private static class ActorRequestReplyInteractorFactory
      implements RequestReplyInteractor.Factory {
    @NonNull
    private final ActorSystem actorSystem;

    @Override
    public <Q, S> RequestReplyInteractor<Q, S> createInteractor(TargetAddress target,
        Class<Q> requestType, Class<S> responseType, Timeout timeout) {
      return new ActorSystemInteractor<Q, S>(actorSystem, requestType, responseType, target,
          timeout);
    }
  }

  private class InteractorActor extends Actor<Object> {
    private final TimeoutSupervisor timeoutSupervisor;
    private final ActorAddress targetAddress;
    private CorrelationKey correlationKey;
    private final Deferred<S> resolver;
    private Stage stage;

    public InteractorActor(@NonNull Context context, ActorAddress targetAddress, Timeout timeout,
        Deferred<S> resolver) {
      super(context);
      this.timeoutSupervisor = Timer.supervisor(timeout, () -> {
        TimeoutException error = new TimeoutException(timeout);
        tell(Message.createFor(self(), self(), error));
      });
      this.correlationKey = CorrelationKey.UNCORRELATED;
      this.targetAddress = targetAddress;
      this.stage = Stage.REQUEST_WAITING;
      this.resolver = resolver;
    }

    @Override
    protected void receive(Message<Object> message) throws Throwable {
      message.whenIsMatchedTo(body -> stage.isRequestWaitingStage(),
          body -> processRequest(message), v -> {
            message.whenIsMatchedTo(body -> stage.isResponseWaitingStage(), body -> {
              if (body instanceof Throwable) {
                throw (Throwable) body;
              }
              processResponse(message);
            });
          });
    }

    @Override
    protected FaultResolver<Object> getFaultResover() {
      return new FaultResolver<Object>() {
        @Override
        public void resolveError(LifecycleController lifecycle, Message<Object> message,
            Throwable error) {
          resolver.reject(error);
          lifecycle.stop();
        }
      };
    }

    @SuppressWarnings("unchecked")
    private void processRequest(Message<Object> requestMessage) throws Throwable {
      requestMessage.whenIsMatchedTo(requestType, requestBody -> {
        handleRequest(requestMessage.map(v -> requestBody));
      }, requestBody -> {
        if (requestBody == null && requestType == Void.class) {
          handleRequest(requestMessage.map(v -> (Q) requestBody));
        } else {
          throw new WrongRequestTypeException(repsponseType, requestBody);
        }
      });
    }

    @SuppressWarnings("unchecked")
    private void processResponse(final Message<Object> responseMessage) throws Throwable {
      responseMessage.whenIsMatchedTo(body -> {
        return Optional.ofNullable(body)
            .map(v -> repsponseType.isInstance(v))
            .orElseGet(() -> {
              return repsponseType == Void.class;
            });
      }, body -> {
        handleResponse(responseMessage.map(v -> (S) body));
        completeInteraction();
      }, responseBody -> {
        throw new WrongResponseTypeException(repsponseType, responseBody);
      });
    }

    private void handleRequest(Message<Q> requestMessage) {
      this.stage = Stage.RESPONSE_WAITING;
      this.correlationKey = requestMessage.getCorrelationKey();
      timeoutSupervisor.startSupervision();
      tell(requestMessage.withDestination(targetAddress).withSender(self()));
    }

    private void handleResponse(Message<S> responseMessage) throws Throwable {
      responseMessage.whenIsMatchedTo(v -> isResponseCorrelatedToRequest(responseMessage),
          responseBody -> {
            timeoutSupervisor.stopSupervision();
            resolver.resolve(responseBody);
          });
    }

    private void completeInteraction() {
      stop(self());
    }

    private boolean isResponseCorrelatedToRequest(Message<S> responseMessage) {
      return responseMessage.getCorrelationKey().equals(correlationKey);
    }
  }

  private enum Stage {
    REQUEST_WAITING,
    RESPONSE_WAITING;

    public boolean isRequestWaitingStage() {
      return this == REQUEST_WAITING;
    }

    public boolean isResponseWaitingStage() {
      return this == RESPONSE_WAITING;
    }
  }
}
