package com.data_ingestion;

import java.sql.*;
import java.util.List;

public class DataIngestionClient {
    private Connection connection;

    public DataIngestionClient(String url, String user, String password) throws SQLException {
        connection = DriverManager.getConnection(url, user, password);
    }

    // Insert data
    public void insertData(String insertSQL, List<Object> parameters) throws SQLException {
        try (PreparedStatement pstmt = connection.prepareStatement(insertSQL)) {
            for (int i = 0; i < parameters.size(); i++) {
                pstmt.setObject(i + 1, parameters.get(i));
            }
            pstmt.executeUpdate();
        }
    }

    // Close connection
    public void close() throws SQLException {
        if (connection != null && !connection.isClosed()) {
            connection.close();
        }
    }
}
