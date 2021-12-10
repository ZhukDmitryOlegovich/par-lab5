import akka.NotUsed;
import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Props;
import akka.http.javadsl.ConnectHttp;
import akka.http.javadsl.Http;
import akka.http.javadsl.ServerBinding;
import akka.http.javadsl.model.Query;
import akka.japi.Function2;
import akka.pattern.Patterns;
import akka.stream.ActorMaterializer;
import akka.stream.javadsl.Flow;

import akka.http.javadsl.model.HttpRequest;
import akka.http.javadsl.model.HttpResponse;
import akka.japi.Pair;
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
import java.util.regex.Pattern;

public class AverageHttpResponseTimeApp {
    public static void main(String[] args) throws IOException {
        System.out.println("start!");
        ActorSystem system = ActorSystem.create("routes");
        ActorRef actor = system.actorOf(Props.create(ActorCache.class));

        final Http http = Http.get(system);
        final ActorMaterializer materializer = ActorMaterializer.create(system);
        final Flow<HttpRequest, HttpResponse, NotUsed> routeFlow = null;// <вызов метода которому передаем Http, ActorSystem и ActorMaterializer>;
        final CompletionStage<ServerBinding> binding = http.bindAndHandle(
                routeFlow,
                ConnectHttp.toHost("localhost", 8080),
                materializer
        );
        System.out.println("Server online at http://localhost:8080/\nPress RETURN to stop...");
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
                    String url = query.get("testUrl").get();
                    int count = Integer.parseInt(query.get("count").get());
                    return new Pair<>(url, count);
                })
                .mapAsync(1, req -> Patterns
                        .ask(
                                actor,
                                new MessageGetResult(req.first()),
                                java.time.Duration.ofMillis(5000)
                        )
                        .thenCompose(res -> {
                            if (((Optional<Long>) res).isPresent()) {
                                return CompletableFuture.completedFuture(new Pair<>(req.first(), ((Optional<Long>) res).get()));
                            } else {
                                Sink<Integer, CompletionStage<Long>> fold = Sink.fold(0L, (Function2<Long, Integer, Long>) Long::sum);
                                Sink<Pair<String, Integer>, CompletionStage<Long>> sink = Flow
                                        .<Pair<String, Integer>>create()
                                        .mapConcat(r -> new ArrayList<>(Collections.nCopies(r.second(), r.first())))
                                        .mapAsync(req.second(), url -> {
                                            long start = System.currentTimeMillis();
                                            Request request = Dsl.get(url).build();
                                            CompletableFuture<Response> whenResponse = Dsl.asyncHttpClient().executeRequest(request).toCompletableFuture();
                                            return whenResponse.thenCompose( response -> {
                                                int duration = (int) (System.currentTimeMillis() - start);
                                                return CompletableFuture.completedFuture(duration);
                                            });
                                        })
                                        .toMat(fold, Keep.right());
                                return Source.from(Collections.singletonList(req))
                                        .toMat(sink, Keep.right())
                                        .run(materializer)
                                        .thenApply(sum -> new Pair<>(req.first(), sum / req.second()));
                            }
                        })
                ))
    }
}
