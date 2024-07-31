package com.data_ingestion.dataingestion;

import org.json.JSONObject;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public class ItemsProcessor {

    public void process(JSONObject jsonObject, DataIngestionClient dbClient) throws SQLException {
        List<Object> params = new ArrayList<>();
        JSONObject item = jsonObject.getJSONObject("item");

        params.add(item.getJSONArray("call_number").getString(0));
        params.add(item.optString("created_published", null));
        params.add(item.optString("date", null));
        params.add(item.optString("notes", null));
        params.add(item.optString("source_collection", null));
        params.add(jsonObject.getString("title"));
        params.add(jsonObject.getString("id"));

        // Insert data into Items table
        dbClient.insertData("INSERT INTO Items (callnumber, created_published, date, notes, sourcecollection, title, externalid) VALUES (?, ?, ?, ?, ?, ?, ?)", params);
    }
}
