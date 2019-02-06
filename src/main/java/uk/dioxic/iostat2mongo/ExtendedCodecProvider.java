package uk.dioxic.iostat2mongo;

import org.bson.codecs.Codec;
import org.bson.codecs.configuration.CodecProvider;
import org.bson.codecs.configuration.CodecRegistry;

import java.util.HashMap;
import java.util.Map;

public class ExtendedCodecProvider implements CodecProvider {

    private final Map<Class<?>, Codec<?>> codecs = new HashMap<>();

    /**
     * A provider of Codecs for extended types.
     */
    public ExtendedCodecProvider(Codec... codecs) {
        for (Codec codec : codecs) {
            addCodec(codec);
        }
    }

    public ExtendedCodecProvider() {};

    @Override
    @SuppressWarnings("unchecked")
    public <T> Codec<T> get(Class<T> clazz, CodecRegistry registry) {
        return (Codec<T>) codecs.get(clazz);
    }

    private <T> void addCodec(final Codec<T> codec) {
        codecs.put(codec.getEncoderClass(), codec);
    }
}
