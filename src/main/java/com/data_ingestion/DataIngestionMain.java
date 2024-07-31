package com.data_ingestion;

import org.json.JSONArray;
import org.json.JSONObject;

import java.io.IOException;
import java.sql.SQLException;
import java.util.List;

public class DataIngestionMain {
    public static void main(String[] args) {
        String dbName = "supreme_court";
        String dbUrl = "jdbc:postgresql://localhost:5432/" + dbName;
        String user = "example";
        String password = "example";
        String projectId = "smart-axis-421517";
        String credentialsPath = "/home/cobra/Repos/justin-napolitano/sup-court-data-ingestion/resources/secret.json";  // Update this to the path of your JSON key
        String bucketName = "processed_results";

        try {
            System.out.println("Initializing Database Client...");
            DataIngestionClient dbClient = new DataIngestionClient(dbUrl, user, password);
            System.out.println("Database Client initialized.");

            System.out.println("Initializing GCS Client...");
            GCSClient gcsClient = new GCSClient(projectId, credentialsPath);
            System.out.println("GCS Client initialized.");

            System.out.println("Listing objects in the bucket: " + bucketName);
            List<String> objectNames = gcsClient.listObjects(bucketName);
            System.out.println("Total objects found: " + objectNames.size());

            for (String objectName : objectNames) {
                System.out.println("Processing object: " + objectName);

                // Download JSON data
                String jsonData = gcsClient.downloadJson(bucketName, objectName);
                System.out.println("Downloaded JSON data for object: " + objectName);

                // Parse JSON data
                JSONObject jsonObject = new JSONObject(jsonData);
                JSONArray resultsArray = jsonObject.getJSONObject("content").getJSONArray("results");
                System.out.println("Parsed JSON data. Total results: " + resultsArray.length());
                

                // Process each result
                for (int i = 0; i < resultsArray.length(); i++) {
                    JSONObject result = resultsArray.getJSONObject(i);
                    System.out.println("Processing result " + (i + 1) + " of " + resultsArray.length());

                    // Process CallNumbers
                    CallNumbersProcessor callNumbersProcessor = new CallNumbersProcessor();
                    callNumbersProcessor.process(result, dbClient);
                    System.out.println("Processed CallNumbers for result " + (i + 1));

                    // Process Contributors
                    ContributorsProcessor contributorsProcessor = new ContributorsProcessor();
                    contributorsProcessor.process(result, dbClient);
                    System.out.println("Processed Contributors for result " + (i + 1));

                    // Process Items
                    ItemsProcessor itemsProcessor = new ItemsProcessor();
                    itemsProcessor.process(result, dbClient);
                    System.out.println("Processed Items for result " + (i + 1));

                    // Process Resources
                    ResourcesProcessor resourcesProcessor = new ResourcesProcessor();
                    resourcesProcessor.process(result, dbClient);
                    System.out.println("Processed Resources for result " + (i + 1));

                    // Process Subjects
                    SubjectsProcessor subjectsProcessor = new SubjectsProcessor();
                    subjectsProcessor.process(result, dbClient);
                    System.out.println("Processed Subjects for result " + (i + 1));
                    System.exit(0);
                }
            }

            dbClient.close();
            System.out.println("Database Client connection closed.");
        } catch (SQLException | IOException e) {
            e.printStackTrace();
        }
    }
}
