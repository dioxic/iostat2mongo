package uk.dioxic.iostat2mongo;

import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.*;
import static uk.dioxic.iostat2mongo.DateUtil.isDate;

public class PatternTest {

    @Test
    void testDatePattern() {
        assertThat(isDate("12/16/18 15:00:10")).isTrue();
    }

    @Test
    void testMachinePattern() {
        String line = "Linux 2.6.32-754.2.1.el6.x86_64 (gbrpsr000003291) \t12/16/18 \t_x86_64_\t(24 CPU)";

        assertThat(IostatParser.getMachine(line)).isEqualTo("gbrpsr000003291");
    }
}
