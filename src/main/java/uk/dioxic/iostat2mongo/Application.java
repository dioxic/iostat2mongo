package uk.dioxic.iostat2mongo;

import org.bson.BSON;
import org.bson.Document;
import org.bson.codecs.BsonValueCodecProvider;
import org.bson.codecs.DocumentCodec;
import org.bson.codecs.DocumentCodecProvider;
import org.bson.codecs.ValueCodecProvider;
import org.bson.codecs.configuration.CodecRegistries;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.json.JsonWriterSettings;
import reactor.core.publisher.Flux;

import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;
import java.util.stream.BaseStream;
import java.util.stream.Collectors;

import static java.util.Arrays.asList;
import static org.bson.codecs.configuration.CodecRegistries.fromProviders;

public class Application {

    private static Pattern datePattern = Pattern.compile("\\d{1,2}/\\d{1,2}/\\d{1,2}\\s\\d{2}:\\d{2}:\\d{2}"); // 12/16/18 15:00:10
    private static DateTimeFormatter df = DateTimeFormatter.ofPattern("MM/dd/yy HH:mm:ss");
    private static final CodecRegistry DEFAULT_REGISTRY = fromProviders(asList(new ValueCodecProvider(),
            new BsonValueCodecProvider(),
            new ExtendedCodecProvider(),
            new DocumentCodecProvider()));
    private static JsonWriterSettings jws = JsonWriterSettings.builder()
            .indent(true)
            .build();
    private static DocumentCodec documentCodec = new DocumentCodec(DEFAULT_REGISTRY);

    public static void main(String[] args) throws URISyntaxException {
        Path iostatPath = Paths.get(Thread.currentThread().getContextClassLoader().getResource("iostat.log").toURI());
        Flux<Document> iostat = iostatFlux(iostatPath);

        iostat.doOnNext(Application::printDocument)
                .blockLast();

    }

    private static Flux<String> fromPath(Path path) {
        return Flux.using(() -> Files.lines(path),
                Flux::fromStream,
                BaseStream::close
        );
    }

    private static void printDocument(Document doc) {

        System.out.println(doc.toJson(jws, documentCodec));
    }

    private static Flux<Document> iostatFlux(Path path) {
        return fromPath(path)
                .bufferUntil(Application::isDate, true)
                .flatMap(docs ->  Flux.fromIterable(parseIostat(docs)));
    }

    static boolean isDate(String s) {
        return datePattern.matcher(s).matches();
    }

    private static List<Document> parseIostat(List<String> s) {
        boolean cpu = false, device = false;
        String[] fields = null;
        List<Document> docs = new ArrayList<>();
        Document doc = null;

        if (!s.isEmpty() && isDate(s.get(0))) {
            LocalDateTime date = LocalDateTime.parse(s.get(0), df);

            for (String line : s) {
                if (line.isEmpty()) {
                    continue;
                }
                if (line.startsWith("avg-cpu:")) {
                    cpu = true;
                    device = false;
                    fields = line.split("\\s+");
                    doc = new Document("ts", date);
                    doc.append("type", "cpu");
                    docs.add(doc);
                    continue;
                }
                if (line.startsWith("Device:")) {
                    cpu = false;
                    device = true;
                    fields = line.split("\\s+");
                    doc = new Document("ts", date);
                    doc.append("type", "device");
                    docs.add(doc);
                    continue;
                }

                if (cpu) {
                    String[] values = line.split("\\s+");
                    for (int i=1; i<fields.length; i++) {
                        doc.put(fields[i], values[i]);
                    }
                }

                if (device) {
                    Document deviceDoc = new Document();
                    String[] values = line.split("\\s+");
                    for (int i=1; i<fields.length; i++) {
                        deviceDoc.put(fields[i], values[i]);
                    }
                    doc.append(values[0], deviceDoc);
                }
            }
        }

        return docs;
    }

}
