package com.data_ingestion;

import org.json.JSONObject;
import org.postgresql.util.PSQLException;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public class ContributorsProcessor {

    public void process(JSONObject jsonObject, DataIngestionClient dbClient) {
        List<Object> params = new ArrayList<>();
        params.add(jsonObject.getString("id"));
        params.add(jsonObject.getJSONArray("contributor").getString(0));

        try {
            // Insert data into Contributors table
            dbClient.insertData("INSERT INTO Contributors (external_id, contributor) VALUES (?, ?)", params);
        } catch (PSQLException e) {
            if (e.getSQLState().equals("23505")) { // 23505 is the SQL state for unique violation
                System.err.println("Duplicate entry for Contributors with external_id: " + jsonObject.getString("id"));
            } else {
                e.printStackTrace();
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}
