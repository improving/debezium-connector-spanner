/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.spanner.util;

import java.io.IOException;
import java.time.Duration;
import java.util.Map;

import org.awaitility.Awaitility;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import okhttp3.HttpUrl;
import okhttp3.MediaType;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;

public class KafkaConnectRestClient {

    private static final Logger LOG = LoggerFactory.getLogger(KafkaConnectRestClient.class);
    private static final MediaType JSON = MediaType.parse("application/json");

    private final HttpUrl baseUrl;
    private final OkHttpClient http;
    private final ObjectMapper mapper = new ObjectMapper();

    public KafkaConnectRestClient(String baseUrl) {
        this.baseUrl = HttpUrl.parse(baseUrl);
        this.http = new OkHttpClient();
    }

    public void waitForWorkerReady(Duration timeout) {
        Awaitility.await()
                .atMost(timeout)
                .ignoreExceptions()
                .until(this::isWorkerReady);
    }

    private boolean isWorkerReady() throws IOException {
        Request request = new Request.Builder()
                .url(baseUrl.resolve("/connectors"))
                .get()
                .build();
        try (Response response = http.newCall(request).execute()) {
            return response.isSuccessful();
        }
    }

    public void deployConnector(String name, Map<String, String> config) {
        LOG.info("Deploying connector '{}' to real Kafka Connect worker", name);
        HttpUrl url = baseUrl.newBuilder()
                .addPathSegment("connectors")
                .addPathSegment(name)
                .addPathSegment("config")
                .build();
        String body;
        try {
            body = mapper.writeValueAsString(config);
        }
        catch (IOException e) {
            throw new RuntimeException("Failed to serialize connector config for '" + name + "'", e);
        }
        Request request = new Request.Builder()
                .url(url)
                .put(RequestBody.create(body, JSON))
                .build();
        try (Response response = http.newCall(request).execute()) {
            if (!response.isSuccessful()) {
                String responseBody = response.body() != null ? response.body().string() : "";
                throw new RuntimeException("Connector deploy request for '" + name + "' returned status "
                        + response.code() + ": " + responseBody);
            }
            LOG.info("Deployed connector '{}'", name);
        }
        catch (IOException e) {
            throw new RuntimeException("Failed to deploy connector '" + name + "'", e);
        }
    }

    public void deleteConnector(String name) {
        LOG.info("Removing connector '{}' from real Kafka Connect worker", name);
        HttpUrl url = baseUrl.newBuilder()
                .addPathSegment("connectors")
                .addPathSegment(name)
                .build();
        Request request = new Request.Builder().url(url).delete().build();
        try (Response response = http.newCall(request).execute()) {
            if (!response.isSuccessful() && response.code() != 404) {
                throw new RuntimeException("Connector delete request for '" + name + "' returned status " + response.code());
            }
        }
        catch (IOException e) {
            throw new RuntimeException("Failed to delete connector '" + name + "'", e);
        }
    }

    public String getConnectorState(String name) {
        HttpUrl url = baseUrl.newBuilder()
                .addPathSegment("connectors")
                .addPathSegment(name)
                .addPathSegment("status")
                .build();
        Request request = new Request.Builder().url(url).get().build();
        try (Response response = http.newCall(request).execute()) {
            if (response.code() == 404) {
                return null;
            }
            if (!response.isSuccessful()) {
                throw new RuntimeException("Connector status request for '" + name + "' returned status " + response.code());
            }
            JsonNode root = mapper.readTree(response.body().string());
            return root.path("connector").path("state").asText(null);
        }
        catch (IOException e) {
            throw new RuntimeException("Failed to fetch status for connector '" + name + "'", e);
        }
    }

    public boolean isConnectorRunning(String name) {
        return "RUNNING".equals(getConnectorState(name));
    }

    public void waitForConnectorRunning(String name, Duration timeout) {
        Awaitility.await()
                .atMost(timeout)
                .ignoreExceptions()
                .until(() -> isConnectorRunning(name));
    }

    public void waitForConnectorAbsent(String name, Duration timeout) {
        Awaitility.await()
                .atMost(timeout)
                .ignoreExceptions()
                .until(() -> getConnectorState(name) == null);
    }
}
