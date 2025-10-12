/*
    TickTockDB Kafka Connector is open-source, and is maintained by
    Yongtao You (yongtao.you@gmail.com) and Yi Lin (ylin30@gmail.com).

    This program is free software: you can redistribute it and/or modify
    it under the terms of the GNU General Public License as published by
    the Free Software Foundation, either version 3 of the License, or
    (at your option) any later version.

    This program is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU General Public License for more details.

    You should have received a copy of the GNU General Public License
    along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */
package com.github.ytyou.kafka.connect.ticktockdb;

import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Map;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;

/**
 * TickTockDBSinkTask sends records to a TickTockDB endpoint.
 */
public class TickTockDBSinkTask extends SinkTask {
    private static final Logger log = LoggerFactory.getLogger(TickTockDBSinkTask.class);

    private String ticktockdb;
    private HttpClient client = HttpClient.newHttpClient();

    public TickTockDBSinkTask() {
    }

    @Override
    public String version() {
        return new TickTockDBSinkConnector().version();
    }

    @Override
    public void start(Map<String, String> props) {
        AbstractConfig config = new AbstractConfig(TickTockDBSinkConnector.CONFIG_DEF, props);
        ticktockdb = config.getString(TickTockDBSinkConnector.TICKTOCKDB_CONFIG);
        client = HttpClient.newBuilder()
            .version(HttpClient.Version.HTTP_1_1)
            .followRedirects(HttpClient.Redirect.NORMAL)
            .connectTimeout(Duration.ofSeconds(10))
            .build();
    }

    @Override
    public void put(Collection<SinkRecord> sinkRecords) {
        if (ticktockdb == null)
            return;

        for (SinkRecord record : sinkRecords) {
            if (record.value().toString().contains("\""))
                continue;   // TickTockDB does not support string values

            // send to TickTockDB
            HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create("http://" + ticktockdb + "/api/write"))
                .header("Content-Type", "text/plain")
                .POST(HttpRequest.BodyPublishers.ofString(record.value().toString()))
                .build();

            HttpResponse<String> response;

            try {
                response = client.send(request, HttpResponse.BodyHandlers.ofString());
            } catch (Exception e) {
                throw new ConnectException("Failed to send to " + ticktockdb, e);
            }

            if (response.statusCode() != 200)
            {
                log.error("Failed to send {} to {}", record.value(), ticktockdb);
                throw new ConnectException("Failed to send to " + ticktockdb + ", status = " + response.statusCode());
            }
        }
    }

    @Override
    public void flush(Map<TopicPartition, OffsetAndMetadata> offsets) {
        // TODO: Instruct TickTockDB to flush!
    }

    @Override
    public void stop() {
        // Close the client?
    }
}
