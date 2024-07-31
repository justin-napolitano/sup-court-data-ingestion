package com.data_ingestion.dataingestion;

import org.json.JSONObject;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public class ContributorsProcessor {

    public void process(JSONObject jsonObject, DataIngestionClient dbClient) throws SQLException {
        List<Object> params = new ArrayList<>();
        params.add(jsonObject.getString("id"));
        params.add(jsonObject.getJSONArray("contributor").getString(0));

        // Insert data into Contributors table
        dbClient.insertData("INSERT INTO Contributors (id, contributor) VALUES (?, ?)", params);
    }
}
