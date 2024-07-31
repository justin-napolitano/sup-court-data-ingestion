package com.data_ingestion.dataingestion;

import org.json.JSONArray;
import org.json.JSONObject;

import java.io.IOException;
import java.sql.SQLException;
import java.util.List;

public class DataIngestionMain {
    public static void main(String[] args) {
        String dbName = "supreme-court";
        String dbUrl = "jdbc:postgresql://localhost:5432/" + dbName;
        String user = "example";
        String password = "example";
        String projectId = "smart-axis-421517";
        String credentialsPath = "path/to/your/service-account-key.json";  // Update this to the path of your JSON key
        String bucket_name = "processed_results"

        try {
            DataIngestionClient dbClient = new DataIngestionClient(dbUrl, user, password);
            GCSClient gcsClient = new GCSClient(projectId, credentialsPath);

            // List all objects in the bucket
            List<String> objectNames = gcsClient.listObjects(processed_results);

            for (String objectName : objectNames) {
                // Download JSON data
                String jsonData = gcsClient.downloadJson(processed_results, objectName);

                // Parse JSON data
                JSONObject jsonObject = new JSONObject(jsonData);
                JSONArray resultsArray = jsonObject.getJSONObject("content").getJSONArray("results");

                // Process each result
                for (int i = 0; i < resultsArray.length(); i++) {
                    JSONObject result = resultsArray.getJSONObject(i);

                    // Process CallNumbers
                    CallNumbersProcessor callNumbersProcessor = new CallNumbersProcessor();
                    callNumbersProcessor.process(result, dbClient);

                    // Process Contributors
                    ContributorsProcessor contributorsProcessor = new ContributorsProcessor();
                    contributorsProcessor.process(result, dbClient);

                    // Process Items
                    ItemsProcessor itemsProcessor = new ItemsProcessor();
                    itemsProcessor.process(result, dbClient);

                    // Process Resources
                    ResourcesProcessor resourcesProcessor = new ResourcesProcessor();
                    resourcesProcessor.process(result, dbClient);

                    // Process Subjects
                    SubjectsProcessor subjectsProcessor = new SubjectsProcessor();
                    subjectsProcessor.process(result, dbClient);
                }
            }

            dbClient.close();
        } catch (SQLException | IOException e) {
            e.printStackTrace();
        }
    }
}
