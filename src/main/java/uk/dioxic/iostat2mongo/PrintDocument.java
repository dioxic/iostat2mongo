package uk.dioxic.iostat2mongo;

import org.bson.Document;
import org.bson.codecs.BsonValueCodecProvider;
import org.bson.codecs.DocumentCodec;
import org.bson.codecs.DocumentCodecProvider;
import org.bson.codecs.ValueCodecProvider;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.codecs.jsr310.LocalDateTimeCodec;
import org.bson.json.JsonWriterSettings;

import static java.util.Arrays.asList;
import static org.bson.codecs.configuration.CodecRegistries.fromProviders;

public class PrintDocument {

    private static final CodecRegistry DEFAULT_REGISTRY = fromProviders(asList(new ValueCodecProvider(),
            new BsonValueCodecProvider(),
            new ExtendedCodecProvider(new LocalDateTimeCodec()),
            new DocumentCodecProvider()));
    private static final JsonWriterSettings jws = JsonWriterSettings.builder()
            .indent(true)
            .build();
    private static final DocumentCodec documentCodec = new DocumentCodec(DEFAULT_REGISTRY);

    public static void print(Document doc) {
        System.out.println(doc.toJson(jws, documentCodec));
    }

}
