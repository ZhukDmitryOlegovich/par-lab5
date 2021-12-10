import akka.actor.AbstractActor;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

public class ActorCache extends AbstractActor {
    private final Map<String, Long> cache = new HashMap<>();

    @Override
    public Receive createReceive() {
        return receiveBuilder()
                .match(
                        MessageGetResult.class,
                        message -> sender().tell(
                                Optional.ofNullable(cache.get(message.getUrl())),
                                self()
                        )
                )
                .match(
                        MessageCacheResult.class,
                        message -> cache.put(message.getUrl(), message.getResponseTime())
                )
                .build();
    }
}
