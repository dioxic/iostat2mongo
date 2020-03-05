package uk.dioxic.iostat2mongo;

import com.mongodb.bulk.BulkWriteResult;
import com.mongodb.client.model.BulkWriteOptions;
import com.mongodb.client.model.InsertOneModel;
import lombok.extern.slf4j.Slf4j;
import org.bson.Document;
import reactor.core.publisher.Flux;
import reactor.core.scheduler.Schedulers;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.stream.BaseStream;

@Slf4j
public class Application {

    private static CliOptions cli;

    public static void main(String[] args) {
        try {
            cli = new CliOptions(args);
            Application application = new Application();
            application.run();
        } catch (Throwable e) {
            e.printStackTrace();
        } finally {
            log.info("cleaning up resources");
            cli.cleanup();
            log.info("exiting");
        }

    }

    private void run() throws IOException {
//        Hooks.onOperatorDebug();

        // drop collection
//        Mono.from(cli.getCollection().drop()).block();

        // create index
//        Mono.from(cli.getCollection().createIndex(Indexes.ascending("machine", "type", "metric", "ts"))).block();

        IostatParser parser = IostatParser.builder()
            .attributes(cli.getAttributes())
            .filters(cli.getFilters())
            .build();

        Bucketer bucketer  = Bucketer.builder()
                .bucketLevel(ChronoUnit.MINUTES)
                .resolution(ChronoUnit.SECONDS)
                .dimensionFields(List.of("machine","type","metric","device"))
                .factField("value")
                .includeValues(false)
                .includeAvg(true)
                .build();

        BulkWriteOptions options = new BulkWriteOptions().ordered(false);

        Runtime runtime = Runtime.getRuntime();

        cli.getFiles().forEach(file -> parser.generatorParse(file)
                .doOnSubscribe(sub -> log.info("Starting processing"))
                .doOnComplete(() -> log.info("Processing complete"))
                .windowUntil(state -> bucketer.splitOn(state.ts), true)
                //.parallel()
                .publishOn(Schedulers.elastic())
                .flatMap(window -> window.flatMap(IostatParser.State::toDocumentList)
                        .groupBy(bucketer::dimensionKey)
                        .flatMap(g -> g.reduce(new Document(), bucketer::combine))
                )
                .doOnNext(doc -> doc.remove(bucketer.getSumField()))
//                .log()
//                .doOnNext(doc -> logger.info(DocumentUtil.toJson(doc)))
//                .doOnNext(cli::log)
                .map(InsertOneModel::new)
                .buffer(cli.getBatchSize())
//                .parallel()
//                .runOn(Schedulers.parallel())
//                .doOnNext(e -> logger.info("writing batch"))
                .flatMap(models -> cli.getCollection().bulkWrite(models, options))
                .doOnNext(result -> log.info(result.toString()))
                .doOnError(System.err::println)
//                .sequential()
                .reduce(new Result(), Result::sum)
                .flatMapMany(result -> Flux.just(
                        result + " from " + file.getFileName(),
                        String.format("Memory in use while reading: %dMB\n", (runtime.totalMemory() - runtime.freeMemory()) / (1024 * 1024))
                ))
                .doOnNext(log::info)
                .blockLast());
    }

    public static Flux<String> fromPath(Path file) {
        log.info("Reading file {}", file.getFileName());

        return Flux.using(() -> Files.lines(file),
                Flux::fromStream,
                BaseStream::close
        );
    }

    static class Result {
        int inserted = 0;
        int modified = 0;
        int deleted = 0;
        int matched = 0;

        static Result sum(Result x, BulkWriteResult y) {
            return x.add(y);
        }

        private Result add(BulkWriteResult bulkWriteResult) {
            inserted += bulkWriteResult.getInsertedCount();
            modified += bulkWriteResult.getModifiedCount();
            deleted += bulkWriteResult.getDeletedCount();
            matched += bulkWriteResult.getMatchedCount();
            inserted += bulkWriteResult.getUpserts().size();
            return this;
        }

        @Override
        public String toString() {
            return "Result{" +
                    "inserted=" + inserted +
                    ", modified=" + modified +
                    ", deleted=" + deleted +
                    ", matched=" + matched +
                    '}';
        }
    }

}
