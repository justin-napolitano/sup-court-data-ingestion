

# Setting Up a Data Ingestion Workflow with Java and Google Cloud Storage

## Introduction

In this readme, we will walk through setting up a data ingestion workflow using Java. The workflow will download JSON data from a Google Cloud Storage (GCS) bucket, parse it, and insert it into a PostgreSQL database. We will also handle unique constraint violations gracefully.

## Prerequisites

* Java 11 or higher installed
* Maven installed
* PostgreSQL running locally (preferably in a Docker container)
* Google Cloud Storage bucket with JSON files
* Service account key for Google Cloud Storage
  
## Setting Up the Project

### 1. Project Structure
Create the following directory structure for your project:

```css
sup-court-data-ingestion/
└── src/
    └── main/
        └── java/
            └── com/
                └── data_ingestion/
                    ├── GCSClient.java
                    ├── DataIngestionClient.java
                    ├── CallNumbersProcessor.java
                    ├── ContributorsProcessor.java
                    ├── ItemsProcessor.java
                    ├── ResourcesProcessor.java
                    ├── SubjectsProcessor.java
                    └── DataIngestionMain.java
```

### 2. pom.xml
Ensure your pom.xml includes the necessary dependencies:

```xml

<project xmlns="http://maven.apache.org/POM/4.0.0" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
  xsi:schemaLocation="http://maven.apache.org/POM/4.0.0 http://maven.apache.org/maven-v4_0_0.xsd">
  <modelVersion>4.0.0</modelVersion>
  <groupId>com.data_ingestion</groupId>
  <artifactId>sup-court-data-ingestion</artifactId>
  <packaging>jar</packaging>
  <version>1.0-SNAPSHOT</version>
  <name>sup-court-data-ingestion</name>
  <url>http://maven.apache.org</url>

  <dependencies>
    <dependency>
      <groupId>org.postgresql</groupId>
      <artifactId>postgresql</artifactId>
      <version>42.2.23</version>
    </dependency>
    <dependency>
      <groupId>com.google.cloud</groupId>
      <artifactId>google-cloud-storage</artifactId>
      <version>2.1.4</version>
    </dependency>
    <dependency>
      <groupId>org.json</groupId>
      <artifactId>json</artifactId>
      <version>20210307</version>
    </dependency>
  </dependencies>

  <build>
    <plugins>
      <plugin>
        <groupId>org.apache.maven.plugins</groupId>
        <artifactId>maven-compiler-plugin</artifactId>
        <version>3.8.1</version>
        <configuration>
          <source>11</source>
          <target>11</target>
        </configuration>
      </plugin>
      <plugin>
        <groupId>org.codehaus.mojo</groupId>
        <artifactId>exec-maven-plugin</artifactId>
        <version>3.0.0</version>
        <configuration>
          <mainClass>com.data_ingestion.DataIngestionMain</mainClass>
        </configuration>
      </plugin>
    </plugins>
  </build>
</project>
``` 

## Implementing the Java Classes

### 1. GCSClient.java

```java
package com.data_ingestion;

import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.storage.*;

import java.io.FileInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import com.google.api.gax.paging.Page;

public class GCSClient {
    private Storage storage;

    public GCSClient(String projectId, String credentialsPath) throws IOException {
        GoogleCredentials credentials = GoogleCredentials.fromStream(new FileInputStream(credentialsPath))
                .createScoped(List.of("https://www.googleapis.com/auth/cloud-platform"));
        storage = StorageOptions.newBuilder().setProjectId(projectId).setCredentials(credentials).build().getService();
    }

    public String downloadJson(String bucketName, String objectName) {
        Blob blob = storage.get(BlobId.of(bucketName, objectName));
        if (blob == null) {
            throw new RuntimeException("No such object");
        }
        return new String(blob.getContent(), StandardCharsets.UTF_8);
    }

    public List<String> listObjects(String bucketName) {
        List<String> objectNames = new ArrayList<>();
        Page<Blob> blobs = storage.list(bucketName);
        for (Blob blob : blobs.iterateAll()) {
            objectNames.add(blob.getName());
        }
        return objectNames;
    }
}
```

### 2. DataIngestionClient.java

```java

package com.data_ingestion;

import java.sql.*;
import java.util.List;

public class DataIngestionClient {
    private Connection connection;

    public DataIngestionClient(String url, String user, String password) throws SQLException {
        connection = DriverManager.getConnection(url, user, password);
    }

    public void insertData(String insertSQL, List<Object> parameters) throws SQLException {
        try (PreparedStatement pstmt = connection.prepareStatement(insertSQL)) {
            for (int i = 0; i < parameters.size(); i++) {
                pstmt.setObject(i + 1, parameters.get(i));
            }
            pstmt.executeUpdate();
        }
    }

    public void close() throws SQLException {
        if (connection != null && !connection.isClosed()) {
            connection.close();
        }
    }
}
```

### 3. Processors with Exception Handling

#### CallNumbersProcessor.java

```java
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
```

#### ContributorsProcessor.java

```java

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
            dbClient.insertData("INSERT INTO Contributors (external_id, contributor) VALUES (?, ?)", params);
        } catch (PSQLException e) {
            if (e.getSQLState().equals("23505")) {
                System.err.println("Duplicate entry for Contributors with external_id: " + jsonObject.getString("id"));
            } else {
                e.printStackTrace();
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}
```

#### ItemsProcessor.java

```java

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
            dbClient.insertData("INSERT INTO Items (call_number, created_published, date, notes, source_collection, title, external_id) VALUES (?, ?, ?, ?, ?, ?, ?)", params);
        } catch (PSQLException e) {
            if (e.getSQLState().equals("23505")) {
                System.err.println("Duplicate entry for Items with external_id: " + jsonObject.getString("id"));
            } else {
                e.printStackTrace();
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}

```

#### ResourcesProcessor.java

```java

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
                dbClient.insertData("INSERT INTO Resources (external_id, image, pdf) VALUES (?, ?, ?)", params);
            } catch (PSQLException e) {
                if (e.getSQLState().equals("23505")) {
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

```

#### SubjectsProcessor.java

```java

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
                dbClient.insertData("INSERT INTO Subjects (external_id, subject) VALUES (?, ?, ?)", params);
            } catch (PSQLException e) {
                if (e.getSQLState().equals("23505")) {
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

``` 

## The Main File

### DataIngestionMain.java


```java

package com.data_ingestion;

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
        String projectId = "strategic-kite-431518-d8";
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
                }
            }

            dbClient.close();
            System.out.println("Database Client connection closed.");
        } catch (SQLException | IOException e) {
            e.printStackTrace();
        }
    }
}
```

## Running the Project

### Navigate to the Project Directory:

```bash
cd /path/to/sup-court-data-ingestion
Compile the Project:
```

```bash

mvn compile
Execute the Main Class:
```

```bash

mvn exec:java -Dexec.mainClass="com.data_ingestion.DataIngestionMain"
```

## Conclusion
In this blog post, we walked through setting up a data ingestion workflow using Java and Google Cloud Storage. We covered how to handle unique constraint violations and ensure our data is correctly ingested into the PostgreSQL database. By following these steps, you should be able to set up a robust data ingestion workflow for your own use case.Copy code