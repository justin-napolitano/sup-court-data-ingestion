package com.data_ingestion;

import org.json.JSONArray;
import org.json.JSONObject;
import org.postgresql.util.PSQLException;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public class SubjectsProcessor {

    public void process(JSONObject jsonObject, DataIngestionClient dbClient) {
        JSONArray subjectsArray = jsonObject.getJSONArray("subject");
        for (int i = 0; i < subjectsArray.length(); i++) {
            List<Object> params = new ArrayList<>();
            params.add(jsonObject.getString("id"));
            params.add(subjectsArray.getString(i));

            try {
                // Insert data into Subjects table
                dbClient.insertData("INSERT INTO Subjects (external_id, subject) VALUES (?, ?, ?)", params);
            } catch (PSQLException e) {
                if (e.getSQLState().equals("23505")) { // 23505 is the SQL state for unique violation
                    System.err.println("Duplicate entry for Subjects with external_id: " + jsonObject.getString("id"));
                } else {
                    e.printStackTrace();
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }
}
