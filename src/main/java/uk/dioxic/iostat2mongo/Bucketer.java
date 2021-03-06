package uk.dioxic.iostat2mongo;

import com.mongodb.client.model.UpdateOneModel;
import com.mongodb.client.model.UpdateOptions;
import lombok.Builder;
import lombok.Getter;
import lombok.NonNull;
import lombok.Singular;
import org.bson.Document;

import java.time.Duration;
import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.stream.Collectors;

@Builder
@Getter
public class Bucketer {

    private static final int HOURS_PER_YEAR = 365 * 24;
    private static final int HOURS_PER_MONTH = HOURS_PER_YEAR;
    private static final UpdateOptions upsertOption;

    static {
        upsertOption = new UpdateOptions();
        upsertOption.upsert(true);
    }

    @Builder.Default private UpdateOptions options = upsertOption;
    @NonNull private ChronoUnit bucketLevel;
    @NonNull private ChronoUnit resolution;
    @NonNull @Singular private List<String> dimensionFields;
    @NonNull @Singular private List<String> factFields;
    @Builder.Default private String timestampField = "ts";
    @Builder.Default private String maxField = "max";
    @Builder.Default private String minField = "min";
    @Builder.Default private String avgField = "avg";
    @Builder.Default private String sumField = "sum";
    @Builder.Default private String countField = "count";
    @Builder.Default private String valueField = "values";
    @Builder.Default private boolean includeMax = true;
    @Builder.Default private boolean includeMin = true;
    @Builder.Default private boolean includeAvg = true;
    @Builder.Default private boolean includeCount = true;
    @Builder.Default private boolean includeValues = true;

    public UpdateOneModel<Document> bucket(Document document) {
        LocalDateTime ts = document.get(timestampField, LocalDateTime.class);
        LocalDateTime bucketTs = truncate(ts);
        int bucketOffset = offset(bucketTs, ts);

        Document filter = new Document();
        filter.put(timestampField, bucketTs);

        dimensionFields.forEach(dim -> {
            if (document.containsKey(dim))
                filter.put(dim, document.get(dim));
        });

        Document update = new Document();
        Object value = document.get("value");

        Document setDoc = new Document();
        final Document maxDoc = new Document();
        final Document minDoc = new Document();
        final Document countDoc  = new Document();

        if (includeMax)
            update.put("$max", maxDoc);

        if (includeMin)
            update.put("$min", minDoc);

        if (includeCount)
            update.put("$inc", countDoc);

        if (factFields.size() == 1) {
            Object fact = document.get(factFields.get(0));
            setDoc.put(valueField + "." + bucketOffset, fact);
            if (includeMax)
                maxDoc.put(maxField, fact);
            if (includeMin)
                minDoc.put(minField, fact);
            if (includeCount)
                countDoc.put(countField, 1);
        }
        else {
            factFields.forEach(fact -> {
                if (document.containsKey(fact)) {
                    if (includeCount || includeMax || includeMin) {
                        setDoc.put(fact + "." + valueField + "." + bucketOffset, document.get(fact));
                    } else {
                        setDoc.put(fact + "." + bucketOffset, document.get(fact));
                    }
                    if (includeMax)
                        maxDoc.put(fact+ "." + maxField, fact);
                    if (includeMin)
                        minDoc.put(fact+ "." + minField, fact);
                    if (includeCount)
                        countDoc.put(fact+ "." + countField, fact);
                }
            });
        }

        update.put("$set", setDoc);

        return new UpdateOneModel<>(filter, update, options);
    }

    public Document combine(Document x, Document y) {
        Document doc = new Document();

        LocalDateTime ts = y.get(timestampField, LocalDateTime.class);
        LocalDateTime bucketTs = truncate(ts);
        int bucketOffset = offset(bucketTs, ts);

        doc.put(timestampField, bucketTs);

        dimensionFields.forEach(dim -> {
            if (y.containsKey(dim))
                doc.put(dim, y.get(dim));
        });

        if (includeValues){
            Document values = new Document();
            values.putAll(x.get("values", new Document()));
            values.put(Integer.toString(bucketOffset), y.getDouble("value"));
            doc.put("values", values);
        }

        if (includeMax)
            doc.put(maxField, Math.max(x.get(maxField, Double.MIN_VALUE), y.getDouble("value")));

        if (includeMin)
            doc.put(minField, Math.min(x.get(minField, Double.MAX_VALUE), y.getDouble("value")));

        if (includeCount || includeAvg)
            doc.put(countField, x.getInteger(countField, 0)+1);

        if (includeAvg) {
            double sum = x.get(sumField, 0d) + y.getDouble("value");
            doc.put(sumField, sum);
            doc.put(avgField, sum / doc.getInteger(countField).doubleValue());
        }

        return doc;
    }

    public boolean splitOn(LocalDateTime ts) {
        return ts.equals(truncate(ts));
    }

    public String dimensionKey(Document doc) {
        return doc.keySet().stream()
                .filter(dimensionFields::contains)
                .map(doc::get)
                .map(Object::toString)
                .collect(Collectors.joining());
    }

    private int offset(LocalDateTime bucketTs, LocalDateTime ts) {
        Duration duration = Duration.between(bucketTs, ts);
        switch (resolution) {
            case YEARS:
                return ts.getYear() - bucketTs.getYear();
            case MONTHS:
                return ts.getMonthValue() - bucketTs.getMonthValue()+1;
            case DAYS:
                return (int)duration.toDays()+1;
            case HOURS:
                return (int)duration.toHours();
            case MINUTES:
                return (int)duration.toMinutes();
            case SECONDS:
                return (int)duration.toSeconds();
            default:
                return 0;
        }
    }

    public LocalDateTime truncate(LocalDateTime ts) {
        switch (bucketLevel) {
            case YEARS:
                ts = ts.withMonth(1);
            case MONTHS:
                ts = ts.withDayOfMonth(1);
            case DAYS:
                ts = ts.withHour(0);
            case HOURS:
                ts = ts.withMinute(0);
            case MINUTES:
                ts = ts.withSecond(0);
        }
        return ts;
    }

}
