package com.data_ingestion.dataingestion;

import org.json.JSONArray;
import org.json.JSONObject;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public class ResourcesProcessor {

    public void process(JSONObject jsonObject, DataIngestionClient dbClient) throws SQLException {
        JSONArray resourcesArray = jsonObject.getJSONArray("resources");
        for (int i = 0; i < resourcesArray.length(); i++) {
            JSONObject resource = resourcesArray.getJSONObject(i);
            List<Object> params = new ArrayList<>();
            params.add(jsonObject.getString("id"));
            params.add(jsonObject.getString("id"));
            params.add(resource.optString("image", null));
            params.add(resource.optString("pdf", null));

            // Insert data into Resources table
            dbClient.insertData("INSERT INTO Resources (id, external_id, image, pdf) VALUES (?, ?, ?, ?)", params);
        }
    }
}
