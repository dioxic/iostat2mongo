package uk.dioxic.iostat2mongo;

import org.bson.Document;

import java.time.LocalDateTime;
import java.util.*;

import static uk.dioxic.iostat2mongo.DateUtil.isDate;

public class IostatParser {

    private List<String> filter;
    private Map<String,Object> additionalAttrs = Collections.emptyMap();

    public IostatParser(Document attributes) {
       this(attributes, null);
    }

    public IostatParser(Document attributes, List<String> filter) {
        if (attributes != null) {
            this.additionalAttrs = new HashMap<>();
            attributes.forEach((key, value) -> additionalAttrs.put(key, value));
        }
        this.filter = filter;
    }

    public void setFilter(List<String> filter) {
        this.filter = filter;
    }

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
                    doc.putAll(additionalAttrs);
                    doc.append("type", "cpu");
                    docs.add(doc);
                    continue;
                }
                if (line.startsWith("Device:")) {
                    cpu = false;
                    device = true;
                    fields = line.split("\\s+");
                    doc = new Document("ts", date);
                    doc.putAll(additionalAttrs);
                    doc.append("type", "device");
                    docs.add(doc);
                    continue;
                }

                if (cpu) {
                    String[] values = line.split("\\s+");
                    for (int i=1; i<fields.length; i++) {
                        doc.put(fields[i], Double.parseDouble(values[i]));
                    }
                }

                if (device) {
                    Document deviceDoc = new Document();
                    String[] values = line.split("\\s+");
                    if (filter == null || filter.contains(fields[0])) {
                        for (int i = 1; i < fields.length; i++) {
                            if (filter == null || filter.contains(fields[i])) {
                                deviceDoc.put(fields[i], Double.parseDouble(values[i]));
                            }
                        }
                        doc.append(values[0], deviceDoc);
                    }
                }
            }
        }

        return docs;
    }
}
