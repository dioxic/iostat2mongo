package uk.dioxic.iostat2mongo;

import com.mongodb.bulk.BulkWriteResult;
import com.mongodb.client.model.BulkWriteOptions;
import com.mongodb.client.model.InsertOneModel;
import org.bson.Document;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.stream.BaseStream;

public class Application {

    private static CliOptions cli;

    public static void main(String[] args) throws IOException {
        cli = new CliOptions(args);

        Mono.from(cli.getCollection().drop()).block();
        cli.getCollection().createIndex(new Document("ts", 1));

        cli.getFiles().forEach(file -> writeResults(parseFile(file))
                .map(BulkWriteResult::getInsertedCount)
                .reduce(0, Integer::sum)
                .doOnNext(i -> System.out.println("Inserted " + i + " from " + file.getFileName()))
                .doOnError(System.err::println)
                .block());

        cli.cleanup();
    }

    public static Flux<String> fromPath(Path path) {
        return Flux.using(() -> Files.lines(path),
                Flux::fromStream,
                BaseStream::close
        );
    }

    public static Flux<Document> parseFile(Path path) {
        IostatParser parser = new IostatParser(cli.getAttributes(), List.of("r/s", "w/s"));

        return fromPath(path)
                .bufferUntil(DateUtil::isDate, true)
                .flatMap(docs ->  Flux.fromIterable(parser.parse(docs)));
    }

    public static Flux<BulkWriteResult> writeResults(Flux<Document> documents) {
        BulkWriteOptions options = new BulkWriteOptions().ordered(false);

        return documents
                .map(InsertOneModel::new)
                .buffer(cli.getBatchSize())
                .parallel(cli.getThreads())
                .runOn(Schedulers.parallel())
                .flatMap(models -> cli.getCollection().bulkWrite(models, options))
                .sequential();
    }

}
