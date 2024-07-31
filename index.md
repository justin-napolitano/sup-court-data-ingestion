+++
title =  "PostGreSQL Java: Data Ingestion"
description = "Learning java with DBs
tags = ['java', "postgresql","cdp"]
images = ["images/feature-image.png"]
date = "2024-07-31T14:25:13-05:00"
categories = ["projects"]
series = ["Java"]

# How to Set Up a PostgreSQL Database and Tables Using Java and Maven

I've been working on this supreme court case thing. I recently played with DataShare to see if I could use something out of the box for some analysis. It was an okay tool... but not really powerful enough for my use case. I want to create automated workflows at some scale and build out applications that are more versatile. So I am starting over with Java.. which I haven't really used in about 15 years.  

## Prerequisites

- Basic knowledge of Java and Maven
- Docker installed on your machine
- PostgreSQL JDBC Driver

## Step 1: Set Up PostgreSQL with Docker

First, let's create a Docker container for PostgreSQL and Adminer, a web-based database management tool.

Create a `docker-compose.yml` file with the following content:

```yaml
version: '3.9'

services:
  db:
    image: postgres:latest
    restart: always
    shm_size: 128mb
    environment:
      POSTGRES_USER: example
      POSTGRES_PASSWORD: example
      POSTGRES_DB: example
    ports:
      - "5432:5432"
    volumes:
      - pgdata:/var/lib/postgresql/data

  adminer:
    image: adminer:latest
    restart: always
    ports:
      - "8080:8080"

volumes:
  pgdata:
```

To start the services, run the following command:

```bash
docker-compose up -d
```

## Step 2: Set Up Maven Project

Create a new Maven project structure:

```
my-java-project/
├── src/
│   └── main/
│       └── java/
│           └── com/
│               └── createdb/
│                   ├── DatabaseClient.java
│                   └── Main.java
├── sql/
│   ├── CallNumbers.sql
│   ├── Contributors.sql
│   ├── Items.sql
│   ├── Resources.sql
│   └── Subjects.sql
├── pom.xml
```

## Step 3: Define SQL Files

Create SQL files to define the tables:

### `sql/CallNumbers.sql`

```sql
CREATE TABLE IF NOT EXISTS CallNumbers (
    id VARCHAR(255) PRIMARY KEY,
    callnumber VARCHAR(255) NOT NULL
);
```

### `sql/Contributors.sql`

```sql
CREATE TABLE IF NOT EXISTS Contributors (
    id VARCHAR(255) PRIMARY KEY,
    contributor VARCHAR(255) NOT NULL
);
```

### `sql/Items.sql`

```sql
CREATE TABLE IF NOT EXISTS Items (
    id SERIAL PRIMARY KEY,
    callnumber VARCHAR(255) NOT NULL,
    created_published VARCHAR(255),
    date DATE,
    notes TEXT,
    sourcecollection VARCHAR(255),
    title VARCHAR(255) NOT NULL,
    externalid VARCHAR(255) UNIQUE NOT NULL
);
```

### `sql/Resources.sql`

```sql
CREATE TABLE IF NOT EXISTS Resources (
    id SERIAL PRIMARY KEY,
    external_id VARCHAR(255) UNIQUE NOT NULL,
    image VARCHAR(255),
    pdf VARCHAR(255)
);
```

### `sql/Subjects.sql`

```sql
CREATE TABLE IF NOT EXISTS Subjects (
    id SERIAL PRIMARY KEY,
    external_id VARCHAR(255) UNIQUE NOT NULL,
    subject VARCHAR(255) NOT NULL
);
```

## Step 4: Create Maven `pom.xml`

Create a `pom.xml` file with the necessary dependencies and plugins:

```xml
<project xmlns="http://maven.apache.org/POM/4.0.0"
         xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
         xsi:schemaLocation="http://maven.apache.org/POM/4.0.0 http://maven.apache.org/xsd/maven-4.0.0.xsd">
    <modelVersion>4.0.0</modelVersion>

    <groupId>com.example</groupId>
    <artifactId>sup-court-db-normalizer</artifactId>
    <version>1.0-SNAPSHOT</version>

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
            <groupId>com.google.cloud</groupId>
            <artifactId>google-cloud-bigquery</artifactId>
            <version>2.1.4</version>
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
                    <mainClass>com.createdb.Main</mainClass>
                </configuration>
            </plugin>
        </plugins>
    </build>
</project>
```

## Step 5: Write Java Code

### `DatabaseClient.java`

```java
package com.createdb;

import java.sql.*;
import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

public class DatabaseClient {
    private Connection connection;

    public DatabaseClient(String url, String user, String password) throws SQLException {
        connection = DriverManager.getConnection(url, user, password);
    }

    // Execute SQL file
    public void executeSqlFile(String filePath) throws SQLException, IOException {
        String sql = new String(Files.readAllBytes(Paths.get(filePath)));
        String[] sqlStatements = sql.split(";");

        try (Statement stmt = connection.createStatement()) {
            for (String statement : sqlStatements) {
                if (!statement.trim().isEmpty()) {
                    stmt.execute(statement.trim());
                }
            }
        }
    }

    // Close connection
    public void close() throws SQLException {
        if (connection != null && !connection.isClosed()) {
            connection.close();
        }
    }
}
```

### `Main.java`

```java
package com.createdb;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class Main {
    public static void main(String[] args) {
        String url = "jdbc:postgresql://localhost:5432/postgres";
        String user = "example";
        String password = "example";
        String dbName = "supreme-court";

        try (Connection connection = DriverManager.getConnection(url, user, password);
             Statement statement = connection.createStatement()) {

            // Create the database if it doesn't exist
            ResultSet resultSet = statement.executeQuery("SELECT 1 FROM pg_database WHERE datname = '" + dbName + "'");
            if (!resultSet.next()) {
                statement.executeUpdate("CREATE DATABASE "" + dbName + """);
                System.out.println("Database " + dbName + " created successfully.");
            } else {
                System.out.println("Database " + dbName + " already exists.");
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }

        // Connect to the new database and execute SQL files to create tables
        try {
            String dbUrl = "jdbc:postgresql://localhost:5432/" + dbName;
            DatabaseClient dbClient = new DatabaseClient(dbUrl, user, password);

            // Execute SQL files to create tables
            dbClient.executeSqlFile("sql/CallNumbers.sql");
            dbClient.executeSqlFile("sql/Contributors.sql");
            dbClient.executeSqlFile("sql/Items.sql");
            dbClient.executeSqlFile("sql/Resources.sql");
            dbClient.executeSqlFile("sql/Subjects.sql");

            dbClient.close();
        } catch (SQLException | IOException e) {
            e.printStackTrace();
        }
    }
}
```

## Step 6: Run the Project

1. **Compile the project**:
   ```bash
   mvn compile
   ```

2. **Run the `Main` class**:
   ```bash
   mvn exec:java -Dexec.mainClass="com.createdb.Main"
   ```

This setup will create the `supreme-court` database if it does not exist and then execute the SQL files to create the tables. You can verify the results using Adminer by navigating to `http://localhost:8080` in your web browser.

## Conclusion

In this post, we set up a PostgreSQL database using Docker, created a Maven project to manage our Java code, defined our database tables with SQL files, and wrote Java code to automate the creation of the database and tables. This setup provides a robust foundation for further development and integration with your data processing applications.
