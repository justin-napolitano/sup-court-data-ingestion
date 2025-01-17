package com.data_ingestion;

import org.json.JSONArray;
import org.json.JSONObject;
import org.postgresql.util.PSQLException;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public class ResourcesProcessor {

    public void process(JSONObject jsonObject, DataIngestionClient dbClient) {
        JSONArray resourcesArray = jsonObject.getJSONArray("resources");
        for (int i = 0; i < resourcesArray.length(); i++) {
            JSONObject resource = resourcesArray.getJSONObject(i);
            List<Object> params = new ArrayList<>();
            params.add(jsonObject.getString("id"));
            params.add(resource.optString("image", null));
            params.add(resource.optString("pdf", null));

            try {
                // Insert data into Resources table
                dbClient.insertData("INSERT INTO Resources (external_id, image, pdf) VALUES (?, ?, ?)", params);
            } catch (PSQLException e) {
                if (e.getSQLState().equals("23505")) { // 23505 is the SQL state for unique violation
                    System.err.println("Duplicate entry for Resources with external_id: " + jsonObject.getString("id"));
                } else {
                    e.printStackTrace();
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }
}
