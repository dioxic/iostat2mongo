package uk.dioxic.iostat2mongo;

import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Hooks;

import java.net.URISyntaxException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Objects;

import static uk.dioxic.iostat2mongo.Application.fromPath;

public class FluxBufferTest {

    @Test
    public void bigFileSize() throws URISyntaxException {
        Path file =Paths.get(Objects.requireNonNull(Thread.currentThread().getContextClassLoader().getResource("iostat-big.log")).toURI());
        IostatParser parser = IostatParser.builder().build();

        Bucketer bucketer  = Bucketer.builder()
                .bucketLevel(ChronoUnit.MINUTES)
                .resolution(ChronoUnit.SECONDS)
                .dimensionFields(List.of("machine","type","metric","device"))
                .factField("value")
                .build();

        Hooks.onOperatorDebug();

        fromPath(file)
//                .doOnNext(System.out::println)
//                .subscribeOn(Schedulers.elastic())
//                .windowUntil(DateUtil::isDate, true)
                .bufferUntil(DateUtil::isDate, true)
//                .log()
//                .flatMap(a -> Flux.just("a","b"))
                .flatMap(this::parseList)
//                .flatMap(lines ->  Flux.fromIterable(parser.parse(lines)))
//                .log()
//                .filter(doc -> doc.get("value", Double.class) > 0)
//                .map(bucketer::bucket)
//                .doOnNext(System.out::println)
//                .limitRate(1)
                .buffer(100)
                .flatMap(a -> Flux.just("a","b"))
//                .log()
                .delayElements(Duration.ofMillis(1000))
                //.doOnNext(i -> System.out.println("processing batch"))
                .blockLast();
    }

    private Flux<String> something(Flux<List<Flux<String>>> thing) {
        return null;
    }

    private Flux<String> parseList(List<String> lines) {
        return Flux.fromIterable(lines);
    }

    private Flux<String> parseFlux(Flux<String> lines) {
        List<String> someLines = lines.collectList().block();
        return Flux.fromIterable(someLines);
    }

}
