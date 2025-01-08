package org.example;

import org.apache.flink.table.functions.ScalarFunction;

public class JsonArrayRenamer  extends ScalarFunction {
    public static final String NAME = "RENAME_JSON_ARRAY";

    public String eval(String json, String oldName, String newName) {
        return json.replace("\"" + oldName + "\":", "\"" + newName + "\":");
    }
}
