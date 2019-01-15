package uk.dioxic.iostat2mongo;

import org.bson.Document;
import reactor.core.publisher.Flux;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.regex.Pattern;
import java.util.stream.BaseStream;

public class Application {

    private static Pattern datePattern = Pattern.compile("\\d{1,2}/\\d{1,2}/\\d{1,2}\\s\\d{2}:\\d{2}:\\d{2}"); // 12/16/18 15:00:10

    public static void main(String[] args) {
        BookReader reader = new BookReader();

        Path iostatPath = Paths.get("/Users/mmunton/Documents/Clients/barclays/sizing/gbrpsr000003291_iostat/gbrpsr000003291-1544972409.iostat.log");
        Path bookPath = Paths.get("/Users/mmunton/Downloads/bookshelf.txt");
// anastasia

        Flux book = bookFlux(bookPath);

        book.doOnNext(System.out::println)
                .blockLast();

        Flux iostat = iostatFlux(iostatPath);

        iostat.doOnNext(System.out::println)
                .blockLast();

    }

    static Flux<List<Document>> iostatFlux(Path path) {
        final Runtime runtime = Runtime.getRuntime();

        return fromPath(path)
                .windowWhile(Application::isDate)
                .collectList()
                .flatMap(Application::parseIostat);

    }

    static boolean isDate(String s) {
        return datePattern.matcher(s).matches();
    }

    static Flux<Document> parseIostat(List<String> s) {
        System.out.println(s);
        return null;
    }

    static Flux<String> bookFlux(Path path) {
        final Runtime runtime = Runtime.getRuntime();

        return fromPath(path)
                .filter(s -> s.startsWith("Title: ") || s.startsWith("Author: ")
                        || s.equalsIgnoreCase("##BOOKSHELF##"))
                .map(s -> s.replaceFirst("Title: ", ""))
                .map(s -> s.replaceFirst("Author: ", " by "))
                .windowWhile(s -> !s.contains("##"))
                .flatMap(bookshelf -> bookshelf
                        .window(2)
                        .flatMap(bookInfo -> bookInfo.reduce(String::concat))
                        .collectList()
                        .doOnNext(s -> System.gc())
                        .flatMapMany(bookList -> Flux.just(
                                "\n\nFound new Bookshelf of " + bookList.size() + " books:",
                                bookList.toString(),
                                String.format("Memory in use while reading: %dMB\n", (runtime.totalMemory() - runtime.freeMemory()) / (1024 * 1024))
                        )));
    }

    private static Flux<String> fromPath(Path path) {
        return Flux.using(() -> Files.lines(path),
                Flux::fromStream,
                BaseStream::close
        );
    }
}
