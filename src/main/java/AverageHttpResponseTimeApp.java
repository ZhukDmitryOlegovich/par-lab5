import akka.NotUsed;
import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Props;
import akka.http.javadsl.ConnectHttp;
import akka.http.javadsl.Http;
import akka.http.javadsl.ServerBinding;
import akka.http.javadsl.model.HttpRequest;
import akka.http.javadsl.model.HttpResponse;
import akka.http.javadsl.model.Query;

import akka.japi.Pair;

import akka.pattern.Patterns;
import akka.stream.ActorMaterializer;
import akka.stream.javadsl.Flow;
import akka.stream.javadsl.Keep;
import akka.stream.javadsl.Sink;
import akka.stream.javadsl.Source;
import org.asynchttpclient.Dsl;

import org.asynchttpclient.Request;
import org.asynchttpclient.Response;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

public class AverageHttpResponseTimeApp {
    private static final String ACTOR_NAME = "routes";

    private static final String URL_TEST_URL = "testUrl";
    private static final String URL_COUNT = "count";

    private static final int TIMEOUT_MILLISECONDS = 5000;
    private static final int MAP_PARALLELISM = 1;

    private static final String HOST = "localhost";
    private static final int PORT = 8080;

    private static final String MESSAGE_SERVER_WAIT = "Wait, the server is starting...";
    private static final String FORMAT_SERVER_START = "Server online at http://%s:%d/\nPress ENTER to stop...\n";
    private static final String FORMAT_RESULT = "%4d %s\n";

    public static void main(String[] args) throws IOException {
        System.out.println(MESSAGE_SERVER_WAIT);
        ActorSystem system = ActorSystem.create(ACTOR_NAME);
        ActorRef actor = system.actorOf(Props.create(ActorCache.class));

        final Http http = Http.get(system);
        final ActorMaterializer materializer = ActorMaterializer.create(system);
        final Flow<HttpRequest, HttpResponse, NotUsed> routeFlow = flowHttpRequest(materializer, actor);
        final CompletionStage<ServerBinding> binding = http.bindAndHandle(
                routeFlow,
                ConnectHttp.toHost(HOST, PORT),
                materializer
        );
        System.out.printf(
                FORMAT_SERVER_START,
                HOST,
                PORT
        );
        System.in.read();
        binding
                .thenCompose(ServerBinding::unbind)
                .thenAccept(unbound -> system.terminate());
    }

    private static Flow<HttpRequest, HttpResponse, NotUsed> flowHttpRequest(
            ActorMaterializer materializer, ActorRef actor
    ) {
        return Flow.of(HttpRequest.class)
                .map(req -> {
                    Query query = req.getUri().query();
                    return new Pair<>(
                            query.get(URL_TEST_URL).get(),
                            Integer.parseInt(query.get(URL_COUNT).get())
                    );
                })
                .mapAsync(MAP_PARALLELISM, req -> Patterns.ask(
                                actor,
                                new MessageGetResult(req.first()),
                                java.time.Duration.ofMillis(TIMEOUT_MILLISECONDS))
                        .thenCompose(res -> {
                            Optional<Long> optionalRes = (Optional<Long>) res;
                            if (optionalRes.isPresent()) {
                                return CompletableFuture.completedFuture(
                                        new Pair<>(req.first(), optionalRes.get())
                                );
                            }
                            Sink<Pair<String, Integer>, CompletionStage<Long>> sink = Flow
                                    .<Pair<String, Integer>>create()
                                    .mapConcat(r -> new ArrayList<>(Collections.nCopies(r.second(), r.first())))
                                    .mapAsync(req.second(), url -> {
                                        long start = System.currentTimeMillis();
                                        return Dsl.asyncHttpClient()
                                                .executeRequest(Dsl.get(url).build())
                                                .toCompletableFuture()
                                                .thenCompose(response -> CompletableFuture.completedFuture(
                                                        System.currentTimeMillis() - start
                                                ));
                                    })
                                    .toMat(Sink.fold(0L, Long::sum), Keep.right());
                            return Source.from(Collections.singletonList(req))
                                    .toMat(sink, Keep.right())
                                    .run(materializer)
                                    .thenApply(sum -> new Pair<>(req.first(), sum / req.second()));
                        })
                )
                .map(res -> {
                    actor.tell(
                            new MessageCacheResult(res.first(), res.second()),
                            ActorRef.noSender()
                    );
                    String message = String.format(
                            FORMAT_RESULT,
                            res.second(),
                            res.first()
                    );
                    System.out.print(message);
                    return HttpResponse.create().withEntity(message);
                });
    }
}
