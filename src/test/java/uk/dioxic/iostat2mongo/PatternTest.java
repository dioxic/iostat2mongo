package uk.dioxic.iostat2mongo;

import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.*;

public class PatternTest {

    @Test
    void testDatePattern() {
        assertThat(Application.isDate("12/16/18 15:00:10")).isTrue();
    }
}
