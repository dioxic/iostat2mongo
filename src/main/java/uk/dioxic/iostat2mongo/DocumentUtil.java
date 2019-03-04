package uk.dioxic.iostat2mongo;

import org.bson.Document;
import org.bson.codecs.*;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.codecs.jsr310.LocalDateTimeCodec;
import org.bson.json.JsonWriterSettings;

import static java.util.Arrays.asList;
import static org.bson.codecs.configuration.CodecRegistries.fromProviders;

public class DocumentUtil {

    private static final CodecRegistry DEFAULT_REGISTRY = fromProviders(asList(new ValueCodecProvider(),
            new BsonValueCodecProvider(),
            new ExtendedCodecProvider(new LocalDateTimeCodec()),
            new DocumentCodecProvider()));
    private static final JsonWriterSettings jws = JsonWriterSettings.builder()
            .indent(true)
            .build();
    private static final DocumentCodec documentCodec = new DocumentCodec(DEFAULT_REGISTRY);

    public static String toJson(Document doc) {
        return doc.toJson(jws, documentCodec);
    }

    public static DocumentCodec getDocumentCodec() {
        return documentCodec;
    }

    public static CodecRegistry getCodecRegistry() {
        return DEFAULT_REGISTRY;
    }

}
