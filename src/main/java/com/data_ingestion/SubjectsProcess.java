package com.data_ingestion.dataingestion;

import org.json.JSONArray;
import org.json.JSONObject;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public class SubjectsProcessor {

    public void process(JSONObject jsonObject, DataIngestionClient dbClient) throws SQLException {
        JSONArray subjectsArray = jsonObject.getJSONArray("subject");
        for (int i = 0; i < subjectsArray.length(); i++) {
            List<Object> params = new ArrayList<>();
            params.add(jsonObject.getString("id"));
            params.add(jsonObject.getString("id"));
            params.add(subjectsArray.getString(i));

            // Insert data into Subjects table
            dbClient.insertData("INSERT INTO Subjects (id, external_id, subject) VALUES (?, ?, ?)", params);
        }
    }
}
