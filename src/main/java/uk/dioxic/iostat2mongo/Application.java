package uk.dioxic.iostat2mongo;

import com.mongodb.bulk.BulkWriteResult;
import com.mongodb.client.model.BulkWriteOptions;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Hooks;
import reactor.core.publisher.Mono;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.stream.BaseStream;

public class Application {

    private static CliOptions cli;
    private static Logger logger = LoggerFactory.getLogger(Application.class);

    public static void main(String[] args) {
        try {
            cli = new CliOptions(args);
            Application application = new Application();
            application.run();
        } catch (Throwable e) {
            e.printStackTrace();
        } finally {
            logger.info("cleaning up resources");
            cli.cleanup();
            logger.info("exiting");
        }

    }

    private void run() throws IOException {
        Hooks.onOperatorDebug();

        // drop collection
        Mono.from(cli.getCollection().drop()).block();

        // create index
        Document index = new Document();
        index.append("machine", 1);
        index.append("type", 1);
        index.append("metric", 1);
        index.append("ts", 1);

        Mono.from(cli.getCollection().createIndex(index)).block();

        IostatParser parser = IostatParser.builder()
            .attributes(cli.getAttributes())
            .filters(cli.getFilters())
            .build();

        Bucketer bucketer  = Bucketer.builder()
                .bucketLevel(ChronoUnit.MINUTES)
                .resolution(ChronoUnit.SECONDS)
                .dimensionFields(List.of("machine","type","metric","device"))
                .factField("value")
                .build();

        BulkWriteOptions options = new BulkWriteOptions().ordered(false);

        Runtime runtime = Runtime.getRuntime();

        cli.getFiles().forEach(file -> fromPath(file)
                .bufferUntil(DateUtil::isDate, true)
//                .flatMap(lines ->  Flux.fromIterable(parser.parse(lines)))
                .log()
                .flatMap(lines -> Flux.fromIterable(parser.parse(lines)))
                .log()
                .filter(doc -> doc.get("value", Double.class) > 0)
                .map(bucketer::bucket)
//                .doOnNext(System.out::println)
//                .map(InsertOneModel::new)
                .buffer(cli.getBatchSize())
                .delayElements(Duration.ofMillis(500))
//                .log()
//                .parallel(cli.getThreads())
//                .runOn(Schedulers.parallel())
//                .doOnNext(DocumentUtil::print)
//                .map(cli.getCollection()::insertOne)
                //.doOnNext(b -> System.out.println("executing bulk write op"))
                .flatMap(models -> cli.getCollection().bulkWrite(models, options))
                .log()
                .doOnError(System.err::println)
//                .blockLast());
//                .sequential()
                //.map(BulkWriteResult::getModifiedCount)
                //.log()
                //.doOnNext(System.out::println)
//                .map(success -> (success.toString().isEmpty()) ? ZERO : ONE)
                .reduce(new Result(), Result::sum)
                .flatMapMany(result -> Flux.just(
                        result + " from " + file.getFileName(),
                        String.format("Memory in use while reading: %dMB\n", (runtime.totalMemory() - runtime.freeMemory()) / (1024 * 1024))
                ))
                .doOnNext(logger::info)
                .blockLast());
    }

    public static Flux<String> fromPath(Path file) {
        logger.info("Reading file {}", file.getFileName());
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
