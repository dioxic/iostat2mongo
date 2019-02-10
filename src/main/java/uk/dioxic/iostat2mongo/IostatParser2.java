package uk.dioxic.iostat2mongo;

import lombok.Builder;
import lombok.Singular;
import org.bson.Document;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static uk.dioxic.iostat2mongo.DateUtil.isDate;

@Builder
public class IostatParser2 {
    @Singular
    private List<String> filters = Collections.emptyList();

    @Singular
    private Map<String,Object> attributes = Collections.emptyMap();

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

    @Override
    public String toString() {
        return "IostatParser{" +
                "filter=" + filters +
                ", additionalAttrs=" + attributes +
                '}';
    }

}
