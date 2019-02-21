package uk.dioxic.iostat2mongo;

import lombok.Builder;
import lombok.Singular;
import org.bson.Document;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.Exceptions;
import reactor.core.publisher.Flux;
import reactor.core.publisher.SynchronousSink;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static uk.dioxic.iostat2mongo.DateUtil.isDate;

@Builder
public class IostatParser {
    private static final Pattern machinePattern = Pattern.compile("\\((.+?)\\)");
    private static final Map<String, String> FIELD_MAPPING = Map.of("Device:", "device", "avg-cpu:", "cpu");

    @Singular
    private List<String> filters;

    @Singular
    private Map<String,Object> attributes;

    private final Logger logger = LoggerFactory.getLogger(getClass());

    public List<Document> parse(List<String> s) {
        boolean cpu = false, device = false;
        String[] fields = null;
        List<Document> docs = new ArrayList<>();
        Document doc = null;

        if (!s.isEmpty() && isDate(s.get(0))) {
            LocalDateTime date = DateUtil.parse(s.get(0));

            for (String line : s) {
                if (line.isEmpty()) {
                    continue;
                }
                if (line.startsWith("avg-cpu:")) {
                    cpu = true;
                    device = false;
                    fields = line.split("\\s+");
                    doc = new Document("ts", date);
                    doc.putAll(attributes);
                    doc.put("type", "cpu");
                    continue;
                }
                if (line.startsWith("Device:")) {
                    cpu = false;
                    device = true;
                    fields = line.split("\\s+");
                    doc = new Document("ts", date);
                    doc.putAll(attributes);
                    doc.put("type", "device");
                    continue;
                }

                if (cpu) {
                    String[] values = line.split("\\s+");
                    for (int i=1; i<fields.length; i++) {
                        Document cpuDoc = new Document();
                        doc.forEach(cpuDoc::put);
                        cpuDoc.put("metric", fields[i]);
                        cpuDoc.put("value", Double.parseDouble(values[i]));
                        docs.add(cpuDoc);
                    }
                }

                if (device) {
                    String[] values = line.split("\\s+");
                    for (int i = 1; i < fields.length; i++) {
                        if (filters == null || filters.contains(fields[i])) {
                            Document deviceDoc = new Document();
                            doc.forEach(deviceDoc::put);
                            deviceDoc.put("device", values[0]);
                            deviceDoc.put("metric", fields[i]);
                            deviceDoc.put("value", Double.parseDouble(values[i]));
                            docs.add(deviceDoc);
                        }
                    }
                }
            }
        }

        return docs;
    }

    public static String getMachine(String line) {
        Matcher matcher = machinePattern.matcher(line);
        return matcher.find() ? matcher.group(1) : null;
    }

    public static class State implements Cloneable {
        String[] headers;
        String[] values;
        String machine;
        LocalDateTime ts;

        public Document toDocument() {
            Document document = new Document();

            String type = headers[0];
            type = FIELD_MAPPING.getOrDefault(type, type);

            document.append("machine", machine)
                    .append("ts", ts)
                    .append("type", type);

            for (int i=1; i< Math.min(headers.length, values.length); i++) {
                document.append(headers[i], Double.valueOf(values[i]));
            }

            if (values[0] != null && !values[0].isBlank()) {
                document.append(type, values[0]);
            }

            return document;
        }

        public Flux<Document> toDocumentList() {
            List<Document> docs = new ArrayList<>();

            for (int i=1; i< Math.min(headers.length, values.length); i++) {
                Document document = new Document();

                String type = headers[0];
                type = FIELD_MAPPING.getOrDefault(type, type);

                document.append("machine", machine)
                        .append("ts", ts)
                        .append("type", type);


                document.append("metric", headers[i]);
                document.append("value", Double.valueOf(values[i]));

                if (values[0] != null && !values[0].isBlank()) {
                    document.append(type, values[0]);
                }
                docs.add(document);
            }

            return Flux.fromIterable(docs);
        }

        public State clone() {
            State clone = new State();
            clone.headers = headers;
            clone.values = values;
            clone.machine = machine;
            clone.ts = ts;

            return clone;
        }

        @Override
        public String toString() {
            return "State{" +
                    "headers=" + Arrays.toString(headers) +
                    ", values=" + Arrays.toString(values) +
                    ", machine='" + machine + '\'' +
                    ", ts=" + ts +
                    '}';
        }
    }

    public Flux<State> generatorParse(Path file)  {
        try {
            final BufferedReader br = Files.newBufferedReader(file);
            return Flux.generate(
                    State::new,
                    (state, sink) -> getNext(state,sink,br)
            );
        } catch (IOException e) {
            throw Exceptions.propagate(e);
        }
    }

    public Publisher<Document> pub = new Publisher<Document>() {
        @Override
        public void subscribe(Subscriber<? super Document> s) {

        }
    };

    private State getNext(State state, SynchronousSink<State> sink, BufferedReader br) {
        while (true) {
            try {
                String line = br.readLine();
                if (line == null) {
                    if (sink != null)
                        sink.complete();
                    br.close();
                    break;
                }
                if (line.startsWith("Linux")) {
                    state.machine = getMachine(line);
                }
                else if (isDate(line)) {
                    state.ts = DateUtil.parse(line);
                }
                else if (!line.isBlank()) {
                    String[] tokens = line.split("\\s+");
                    if (tokens.length > 0 && tokens[0].endsWith(":")) {
                        state.headers = tokens;
                    } else {
                        logger.debug("stat emitted");
                        state.values = tokens;
                        if (sink != null)
                            sink.next(state.clone());
                        break;
                    }
                }
            }
            catch (IOException e) {
                throw Exceptions.propagate(e);
            }
        }
        return state;
    }

    @Override
    public String toString() {
        return "IostatParser{" +
                "filter=" + filters +
                ", additionalAttrs=" + attributes +
                '}';
    }
}
