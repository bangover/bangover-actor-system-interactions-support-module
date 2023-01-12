package cloud.bangover.interactions.streaming;

import cloud.bangover.actors.Actor;
import cloud.bangover.actors.ActorAddress;
import cloud.bangover.actors.ActorName;
import cloud.bangover.actors.ActorSystem;
import cloud.bangover.actors.Message;
import cloud.bangover.async.promises.Deferred;
import cloud.bangover.async.promises.Promise;
import cloud.bangover.async.promises.Promises;
import cloud.bangover.interactions.streaming.Destination.SourceConnection;
import cloud.bangover.interactions.streaming.Source.DestinationConnection;
import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.atomic.AtomicLong;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;

/**
 * This class implements the streaming mechanism over actors system.
 *
 * @author Dmitry Mikhaylenko
 *
 */
@RequiredArgsConstructor
public class ActorSystemStreamer implements Streamer {
  private final ActorSystem actorSystem;
  private final AtomicLong sequence = new AtomicLong(0L);

  @Override
  public <T> Stream<T> createStream(Source<T> source, Destination<T> destination) {
    return new ActorSystemStream<T>(source, destination);
  }

  private interface StreamCommand<T> {
    public void apply(Transmitter<T> transmitter, Message<Object> message);
  }

  private interface Transmitter<T> {
    public void start(Message<Object> message);

    public void receive(Message<Object> message);

    public void submit(Message<Object> message, T data, int size);

    public void complete(Message<Object> message, Long count);

    public void fail(Message<Object> message, Throwable error);
  }

  @RequiredArgsConstructor
  private final class ActorSystemStream<T> implements Stream<T> {
    @NonNull
    private final Source<T> source;
    @NonNull
    private final Destination<T> destination;

    private final Collection<StatusObserver> statusObservers = new ArrayList<>();

    @Override
    public Stream<T> observeStatus(StatusObserver statusObserver) {
      this.statusObservers.add(statusObserver);
      return this;
    }

    @Override
    public Promise<Stat> start() {
      return Promises.of(resolver -> {
        ActorAddress streamActorAddress = actorSystem.actorOf(generateActorName(),
            context -> new StreamingActor(context, source, destination, resolver));
        actorSystem.tell(Message.createFor(streamActorAddress, new Start()));
      });
    }

    private ActorName generateActorName() {
      return ActorName.wrap(String.format("STREAM--%S", sequence.incrementAndGet()));
    }

    @Getter
    @EqualsAndHashCode
    @RequiredArgsConstructor
    private class CurrentStatus implements Stat {
      private final Long size;
    }

    private final class Start implements StreamCommand<T> {
      @Override
      public void apply(Transmitter<T> transmitter, Message<Object> message) {
        transmitter.start(message);
      }
    }

    private final class Receive implements StreamCommand<T> {
      @Override
      public void apply(Transmitter<T> transmitter, Message<Object> message) {
        transmitter.receive(message);
      }
    }

    @RequiredArgsConstructor
    private final class Submit implements StreamCommand<T> {
      private final T data;
      private final int size;

      @Override
      public void apply(Transmitter<T> transmitter, Message<Object> message) {
        transmitter.submit(message, data, size);
      }
    }

    @RequiredArgsConstructor
    private final class Complete implements StreamCommand<T> {
      private final Long count;

      @Override
      public void apply(Transmitter<T> transmitter, Message<Object> message) {
        transmitter.complete(message, count);
      }
    }

    @RequiredArgsConstructor
    private final class Fail implements StreamCommand<T> {
      private final Throwable error;

      @Override
      public void apply(Transmitter<T> transmitter, Message<Object> message) {
        transmitter.fail(message, error);
      }
    }

    private final class StreamingActor extends Actor<Object> {
      private final Transmitter<T> transmitter;

      public StreamingActor(@NonNull Context context, @NonNull Source<T> source,
          @NonNull Destination<T> destination, @NonNull Deferred<Stat> resolver) {
        super(context);
        this.transmitter = new StreamingTranmitter(source, destination, resolver);
      }

      @Override
      @SuppressWarnings("unchecked")
      protected void receive(Message<Object> message) throws Throwable {
        message.whenIsMatchedTo(StreamCommand.class,
            command -> command.apply(transmitter, message));
      }

      @Override
      protected FaultResolver<Object> getFaultResover() {
        FaultResolver<Object> original = super.getFaultResover();
        return new FaultResolver<Object>() {
          @Override
          public void resolveError(LifecycleController lifecycle, Message<Object> message,
              Throwable error) {
            tell(message.map(body -> new Fail(error)));
            original.resolveError(lifecycle, message, error);
          }
        };
      }

      @RequiredArgsConstructor
      private class StreamingTranmitter implements Transmitter<T> {
        private final Source<T> source;
        private final Destination<T> destination;
        private final Deferred<Stat> resolver;
        private Long totalSize = 0L;

        @Override
        public void start(Message<Object> message) {
          tell(message.map(body -> new Receive()));
        }

        @Override
        public void receive(Message<Object> message) {
          source.read(createDestinationConnection(message));
        }

        @Override
        public void submit(Message<Object> message, T data, int size) {
          destination.write(createSourceConnection(message), data, size);
          updateTotalSize(size);
        }

        @Override
        public void complete(Message<Object> message, Long count) {
          destination.release();
          source.release();
          resolver.resolve(new CurrentStatus(totalSize));
          stop(self());
        }

        @Override
        public void fail(Message<Object> message, Throwable error) {
          destination.release();
          source.release();
          resolver.reject(error);
          stop(self());
        }

        private void updateTotalSize(int transmittedSize) {
          this.totalSize += transmittedSize;
          notifyStatusObservers();
        }

        private void notifyStatusObservers() {
          statusObservers
              .forEach(observer -> observer.onStatusChange(new CurrentStatus(totalSize)));
        }

        private DestinationConnection<T> createDestinationConnection(Message<Object> message) {
          return new DestinationConnection<T>() {
            @Override
            public void submit(T data, Integer size) {
              tell(message.map(body -> new Submit(data, size)));
            }

            @Override
            public void complete() {
              tell(message.map(body -> new Complete(totalSize)));
            }
          };
        }

        private SourceConnection createSourceConnection(Message<Object> message) {
          return new SourceConnection() {
            @Override
            public void receive() {
              tell(message.map(body -> new Receive()));
            }
          };
        }
      }
    }
  }
}
