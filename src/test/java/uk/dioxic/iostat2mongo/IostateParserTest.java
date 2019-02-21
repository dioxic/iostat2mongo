package uk.dioxic.iostat2mongo;

import org.junit.jupiter.api.Test;
import reactor.test.StepVerifier;

import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.Objects;

public class IostateParserTest {

    @Test
    public void fluxParserTest() throws URISyntaxException, IOException {
        Path file = Paths.get(Objects.requireNonNull(Thread.currentThread().getContextClassLoader().getResource("iostat-test.log")).toURI());
        IostatParser parser = IostatParser.builder().build();

        StepVerifier.create(parser.generatorParse(file).delayElements(Duration.ofSeconds(1)))
                .expectNextCount(10)
            .verifyComplete();

    }
}
