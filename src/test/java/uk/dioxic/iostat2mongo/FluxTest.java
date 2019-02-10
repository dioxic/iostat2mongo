package uk.dioxic.iostat2mongo;

import org.junit.jupiter.api.Test;

import java.util.List;

public class FluxTest {

    @Test
    public void testSplit() {
        String mFilter = "";
        List<String> filter = List.of(mFilter.split("\\s*,\\s*"));

        System.out.println(filter);
    }

}
