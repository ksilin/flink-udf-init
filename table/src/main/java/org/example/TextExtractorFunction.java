package org.example;

import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

@FunctionHint(output = @DataTypeHint("ROW<word STRING, length INT>"))
public class TextExtractorFunction extends TableFunction<Row> {

    private static final Logger LOGGER = LogManager.getLogger();

    public void eval(String input) {
        if (input == null || input.isEmpty()) {
            LOGGER.debug("Input is null or empty");
            return;
        }
        String[] words = input.replaceAll("[^a-zA-Z0-9\\s]", "").split("\\s+");
        LOGGER.debug("Extracted {} words from {} : {} ", words.length,  input, words);
        for (String word : words) {
            collect(Row.of(word, word.length()));
        }
    }

    public void eval(String input, String regexString) {
        if (input == null || input.isEmpty()) {
            LOGGER.debug("Input is null or empty");
            return;
        }
        String[] words = input.replaceAll("[^a-zA-Z0-9\\s]", "").split(regexString);
        LOGGER.debug("Extracted {} words from {} : {} ", words.length,  input, words);
        for (String word : words) {
            collect(Row.of(word, word.length()));
        }
    }
}