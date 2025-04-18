package org.example;

import org.apache.flink.table.functions.AggregateFunction;

// recommended for large state, but unclear how to use
// import org.apache.flink.table.api.dataview.MapView;

import java.util.regex.Pattern;
import java.util.stream.StreamSupport;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class CountSubstringStateful extends AggregateFunction<Integer, CountSubstringStateful.CountAccumulator> {

    private static final Logger LOGGER = LogManager.getLogger();

    public static class CountAccumulator {
        public Integer value;

        public CountAccumulator(Integer value){
            this.value = value;
        }
    }

    public static final String NAME = "COUNT_SUBSTRING";

    // mandatory
    public void accumulate(CountAccumulator acc, String string, String substring) {
        long counted = countSubstring(string, substring);
        LOGGER.debug("accumulating count of {} in {}. count before: {}, adding {}", substring, string, acc.value, counted);
        acc.value += Math.toIntExact(counted);
    }

    public static long countSubstring(String str, String substring) {
        return Pattern.compile(Pattern.quote(substring))
                .matcher(str)
                .results()
                .count();
    }

    // merge is mandatory for bounded aggregations as well as
    // session or hop window aggregations
    // accumulators are joined if a row is observed that 'connects' their sessions
    public void merge(CountAccumulator acc, Iterable<CountAccumulator> accumulators) {

        Integer reduced = StreamSupport.stream(accumulators.spliterator(), false).map(a -> a.value)
                .reduce(0, Integer::sum);
        LOGGER.debug("merging. count of merged: {}, adding to {}", reduced, acc.value);

        // TODO - replace or add? I assume add
        acc.value += reduced;
    }

    // retract is necessary for aggregations on OVER windows
    public void retract(CountAccumulator acc, Long value) {
        LOGGER.debug("retracting. substracting {} from {}", value, acc.value);
        acc.value -= value.intValue();
    }

    @Override
    public Integer getValue(CountAccumulator o) {
        return o.value;
    }

    @Override
    public CountAccumulator createAccumulator() {
        return new CountAccumulator(0);
    }
}
