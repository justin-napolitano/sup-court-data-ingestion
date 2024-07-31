package com.data_ingestion.dataingestion;

import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.storage.*;

import java.io.FileInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

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
