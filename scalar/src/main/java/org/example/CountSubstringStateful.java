package org.example;

import org.apache.flink.table.functions.AggregateFunction;

import java.util.regex.Pattern;
import java.util.stream.StreamSupport;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class CountSubstringStateful extends AggregateFunction<Long, CountSubstringStateful.CountAccumulator> {

    private static final Logger LOGGER = LogManager.getLogger();

    public static class CountAccumulator {
        public Long value;

        public CountAccumulator(Long value) {
            this.value = value;
        }
    }

    public static final String NAME = "COUNT_SUBSTRING";

    // mandatory
    public void accumulate(CountAccumulator acc, String string, String substring) {
        if (string == null || substring == null) {
            return;
        }
        long counted = countSubstring(string, substring);
        LOGGER.debug("accumulating count of {} in {}. count before: {}, adding {}", substring, string, acc.value, counted);
        acc.value += counted;
    }

    public static long countSubstring(String str, String substring) {
        if (str.isEmpty() || substring.isEmpty()) {
            return 0;
        }
        return Pattern.compile(Pattern.quote(substring))
                .matcher(str)
                .results()
                .count();
    }

    // merge is mandatory for bounded aggregations as well as
    // session or hop window aggregations
    // accumulators are joined if a row is observed that 'connects' their sessions
    public void merge(CountAccumulator acc, Iterable<CountAccumulator> accumulators) {
        long reduced = StreamSupport.stream(accumulators.spliterator(), false)
                .map(a -> a.value)
                .reduce(0L, Long::sum);
        LOGGER.debug("merging. count of merged: {}, adding to {}", reduced, acc.value);

        // The logic here should be to add the merged values to the current accumulator.
        acc.value += reduced;
    }

    // retract is necessary for aggregations on OVER windows
    public void retract(CountAccumulator acc, String string, String substring) {
        if (string == null || substring == null) {
            return;
        }
        long counted = countSubstring(string, substring);
        LOGGER.debug("retracting. subtracting {} from {}", counted, acc.value);
        acc.value -= counted;
    }

    @Override
    public Long getValue(CountAccumulator o) {
        return o.value;
    }

    @Override
    public CountAccumulator createAccumulator() {
        return new CountAccumulator(0L);
    }
}
