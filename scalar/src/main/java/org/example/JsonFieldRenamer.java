package org.example;

import org.apache.flink.table.functions.ScalarFunction;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class JsonFieldRenamer extends ScalarFunction {
    public static final String NAME = "RENAME_JSON_FIELD";

    private static final Logger LOGGER = LogManager.getLogger();

    public String eval(String json, String oldName, String newName) {

        LOGGER.info("Renaming field {} to {} in JSON: {}", oldName, newName, json);

        String replaced = json.replace("\"" + oldName + "\":", "\"" + newName + "\":");

        LOGGER.info("Renaming result: {}", replaced);

        return replaced;
    }
}
