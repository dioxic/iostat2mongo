package uk.dioxic.iostat2mongo;

import com.mongodb.client.model.UpdateOneModel;
import org.bson.BsonDocument;
import org.bson.BsonDouble;
import org.bson.Document;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;

import static org.assertj.core.api.Assertions.assertThat;

public class BucketerTest {

    private static Document document;
    private static LocalDateTime ts;
    private static LocalDateTime bucketYear;
    private static LocalDateTime bucketMonth;
    private static LocalDateTime bucketDay;
    private static LocalDateTime bucketHour;
    private static LocalDateTime bucketMinute;

    private Bucketer.BucketerBuilder builder;

    @BeforeAll
    public static void beforeAll() {
        ts = LocalDateTime.of(2019,2,2,1,59,59);
        bucketYear = LocalDateTime.of(2019,1,1,0,0,0);
        bucketMonth = LocalDateTime.of(2019,2,1,0,0,0);
        bucketDay = LocalDateTime.of(2019,2,2,0,0,0);
        bucketHour = LocalDateTime.of(2019,2,2,1,0,0);
        bucketMinute = LocalDateTime.of(2019,2,2,1,59,0);

        document = new Document();
        document.put("ts", ts);
        document.put("machine", "machine123");
        document.put("type", "device");
        document.put("metric", "r/s");
        document.put("value", 29.5);
    }

    @BeforeEach
    public void before() {
        builder = new Bucketer.BucketerBuilder();
        builder.dimensionField("machine")
                .dimensionField("type")
                .dimensionField("metric")
                .factField("value")
                .timestampField("ts");
    }

    @Test
    public void bucket_minuteLevel_secondResolution() {
        Bucketer bucketer = builder.bucketLevel(ChronoUnit.MINUTES).resolution(ChronoUnit.SECONDS).build();
        assertTs(bucketer.bucket(document), bucketMinute, 59);
    }

    @Test
    public void bucket_hourLevel_secondResolution() {
        Bucketer bucketer = builder.bucketLevel(ChronoUnit.HOURS).resolution(ChronoUnit.SECONDS).build();
        assertTs(bucketer.bucket(document), bucketHour, 3599);
    }

    @Test
    public void bucket_dayLevel_secondResolution() {
        Bucketer bucketer = builder.bucketLevel(ChronoUnit.DAYS).resolution(ChronoUnit.SECONDS).build();
        assertTs(bucketer.bucket(document), bucketDay, 7199);
    }

    @Test
    public void bucket_monthLevel_secondResolution() {
        Bucketer bucketer = builder.bucketLevel(ChronoUnit.MONTHS).resolution(ChronoUnit.SECONDS).build();
        long offset = ts.toEpochSecond(ZoneOffset.UTC) - bucketMonth.toEpochSecond(ZoneOffset.UTC);
        assertTs(bucketer.bucket(document), bucketMonth, offset);
    }

    @Test
    public void bucket_hourLevel_minuteResolution() {
        Bucketer bucketer = builder.bucketLevel(ChronoUnit.HOURS).resolution(ChronoUnit.MINUTES).build();
        assertTs(bucketer.bucket(document), bucketHour, 59);
    }

    @Test
    public void bucket_dayLevel_minuteResolution() {
        Bucketer bucketer = builder.bucketLevel(ChronoUnit.DAYS).resolution(ChronoUnit.MINUTES).build();
        assertTs(bucketer.bucket(document), bucketDay, 119);
    }

    @Test
    public void bucket_monthLevel_minuteResolution() {
        Bucketer bucketer = builder.bucketLevel(ChronoUnit.MONTHS).resolution(ChronoUnit.MINUTES).build();
        long offset = ts.toEpochSecond(ZoneOffset.UTC) - bucketMonth.toEpochSecond(ZoneOffset.UTC);
        assertTs(bucketer.bucket(document), bucketMonth, offset / 60);
    }

    @Test
    public void bucket_dayLevel_hourResolution() {
        Bucketer bucketer = builder.bucketLevel(ChronoUnit.DAYS).resolution(ChronoUnit.HOURS).build();
        assertTs(bucketer.bucket(document), bucketDay, 1);
    }

    @Test
    public void bucket_monthLevel_hourResolution() {
        Bucketer bucketer = builder.bucketLevel(ChronoUnit.MONTHS).resolution(ChronoUnit.HOURS).build();
        assertTs(bucketer.bucket(document), bucketMonth, 25);
    }

    @Test
    public void bucket_monthLevel_dayResolution() {
        Bucketer bucketer = builder.bucketLevel(ChronoUnit.MONTHS).resolution(ChronoUnit.DAYS).build();
        assertTs(bucketer.bucket(document), bucketMonth, 2);
    }

    @Test
    public void bucket_yearLevel_hourResolution() {
        Bucketer bucketer = builder.bucketLevel(ChronoUnit.YEARS).resolution(ChronoUnit.HOURS).build();
        assertTs(bucketer.bucket(document), bucketYear, 769);
    }

    @Test
    public void bucket_yearLevel_dayResolution() {
        Bucketer bucketer = builder.bucketLevel(ChronoUnit.YEARS).resolution(ChronoUnit.DAYS).build();
        assertTs(bucketer.bucket(document), bucketYear, 33);
    }

    @Test
    public void bucket_yearLevel_monthResolution() {
        Bucketer bucketer = builder.bucketLevel(ChronoUnit.YEARS).resolution(ChronoUnit.MONTHS).build();
        assertTs(bucketer.bucket(document), bucketYear, 2);
    }

    private void assertTs(UpdateOneModel<Document> bucket, LocalDateTime bucketTs, long bucketOffset) {
        BsonDocument filter = bucket.getFilter().toBsonDocument(Document.class, DocumentUtil.getCodecRegistry());

        assertThat(filter).as("filter keys").containsKeys("ts", "machine", "type", "metric");
        assertThat(filter.get("ts").isDateTime()).as("timestamp field is BsonDateTime").isTrue();
        assertThat(LocalDateTime.ofInstant(Instant.ofEpochMilli(filter.get("ts").asDateTime().getValue()), ZoneOffset.UTC))
                .as("bucket timestamp")
                .isEqualTo(bucketTs);


        BsonDocument update = bucket.getUpdate().toBsonDocument(Document.class, DocumentUtil.getCodecRegistry());

        assertThat(update).as("update keys").containsKeys("$set", "$max", "$min", "$inc");
        assertThat(update.get("$set").asDocument()).as("update $set key").containsKeys("values." + bucketOffset);
        assertThat(update.get("$set").asDocument().get("values." + bucketOffset)).as("update $set value").isEqualTo(new BsonDouble(29.5));
    }
}
