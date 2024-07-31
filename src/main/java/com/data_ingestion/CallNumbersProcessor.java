package com.data_ingestion;

import org.json.JSONObject;
import org.postgresql.util.PSQLException;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public class CallNumbersProcessor {

    public void process(JSONObject jsonObject, DataIngestionClient dbClient) {
        List<Object> params = new ArrayList<>();
        params.add(jsonObject.getString("id"));
        params.add(jsonObject.getJSONObject("item").getJSONArray("call_number").getString(0));

        try {
            // Insert data into CallNumbers table
            dbClient.insertData("INSERT INTO CallNumbers (external_id, call_number) VALUES (?, ?)", params);
        } catch (PSQLException e) {
            if (e.getSQLState().equals("23505")) { // 23505 is the SQL state for unique violation
                System.err.println("Duplicate entry for CallNumbers with external_id: " + jsonObject.getString("id"));
            } else {
                e.printStackTrace();
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}
