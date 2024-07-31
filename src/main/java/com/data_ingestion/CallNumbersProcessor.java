package com.data_ingestion.dataingestion;

import org.json.JSONObject;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public class CallNumbersProcessor {

    public void process(JSONObject jsonObject, DataIngestionClient dbClient) throws SQLException {
        List<Object> params = new ArrayList<>();
        params.add(jsonObject.getString("id"));
        params.add(jsonObject.getJSONObject("item").getJSONArray("call_number").getString(0));

        // Insert data into CallNumbers table
        dbClient.insertData("INSERT INTO CallNumbers (id, callnumber) VALUES (?, ?)", params);
    }
}
