package com.data_ingestion;

import org.json.JSONObject;
import org.postgresql.util.PSQLException;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public class ItemsProcessor {

    public void process(JSONObject jsonObject, DataIngestionClient dbClient) {
        List<Object> params = new ArrayList<>();
        JSONObject item = jsonObject.getJSONObject("item");

        params.add(item.getJSONArray("call_number").getString(0));
        params.add(item.optString("created_published", null));
        params.add(item.optString("date", null));
        params.add(item.optString("notes", null));
        params.add(item.optString("source_collection", null));
        params.add(jsonObject.getString("title"));
        params.add(jsonObject.getString("id"));

        try {
            // Insert data into Items table
            dbClient.insertData("INSERT INTO Items (call_number, created_published, date, notes, source_collection, title, external_id) VALUES (?, ?, ?, ?, ?, ?, ?)", params);
        } catch (PSQLException e) {
            if (e.getSQLState().equals("23505")) { // 23505 is the SQL state for unique violation
                System.err.println("Duplicate entry for Items with external_id: " + jsonObject.getString("id"));
            } else {
                e.printStackTrace();
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}
