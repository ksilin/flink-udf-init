package org.example;

import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

@FunctionHint(output = @DataTypeHint("ROW<word STRING>"))
public class StringLoggingTableFunction extends TableFunction<Row> {

    private static final Logger LOGGER = LogManager.getLogger();

    public void eval(String input) {
        if (input == null || input.isEmpty()) {
            LOGGER.warn("Input is null or empty");
            return;
        }
        LOGGER.trace(" TRACE: input {} ", input);
        LOGGER.debug(" DEBUG: input {} ", input);
        LOGGER.info(" INFO: input {} ", input);
        LOGGER.warn(" WARN: input {} ", input);
        LOGGER.error(" ERROR: input {} ", input);
        LOGGER.fatal(" FATAL: input {} ", input);
        collect(Row.of(input));
    }
}