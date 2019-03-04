package uk.dioxic.iostat2mongo;

import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Hooks;
import reactor.core.scheduler.Schedulers;

import java.net.URISyntaxException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;

public class FluxBufferTest {

    private Logger logger = LoggerFactory.getLogger(getClass());

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

        AtomicInteger counter = new AtomicInteger(0);

        parser.generatorParse(file)
//        fromPath(file)
                .doOnSubscribe(sub -> logger.info("Starting processing"))
                .doOnComplete(() -> logger.info("Processing complete"))
//                .map(IostatParser.State::toDocument)
                .flatMap(IostatParser.State::toDocumentList)
//                .doOnNext(doc -> logger.info(DocumentUtil.toJson(doc)))
//                .subscribeOn(Schedulers.elastic())
//                .windowUntil(DateUtil::isDate, true)
//                .flatMap(a -> Flux.just("a","b"))
//                .flatMap(this::parseList)
//                .flatMap(lines ->  Flux.defer(() -> Flux.fromIterable(parser.generatorParse(lines))),1,1)
//                .filter(doc -> doc.get("value", Double.class) > 0)
//                .map(bucketer::bucket)
//                .doOnNext(System.out::println)
//                .limitRate(1)
                .buffer(10)
                .publishOn(Schedulers.elastic())
                .delayElements(Duration.ofMillis(100))
                //.flatMap(list -> Flux.just(list.size()), 1, 1)
                .doOnNext(docs -> logger.info("writing {} documents", docs.size()))
                .map(Collection::size)
                .reduce(0, Integer::sum)
                .doOnNext(total -> logger.info("total documents: {}", total))
                .block();
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
