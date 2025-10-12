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

import org.apache.kafka.common.config.ConfigValue;
import org.apache.kafka.connect.connector.ConnectorContext;
import org.apache.kafka.connect.sink.SinkConnector;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.Mockito.mock;

public class TickTockDBSinkConnectorTest {

    private static final String MULTIPLE_TOPICS = "topic1,topic2";
    private static final String TICKTOCKDB = "host:6182";

    private TickTockDBSinkConnector connector;
    private Map<String, String> sinkProperties;

    @BeforeEach
    public void setup() {
        connector = new TickTockDBSinkConnector();
        ConnectorContext ctx = mock(ConnectorContext.class);
        connector.initialize(ctx);

        sinkProperties = new HashMap<>();
        sinkProperties.put(SinkConnector.TOPICS_CONFIG, MULTIPLE_TOPICS);
        sinkProperties.put(TickTockDBSinkConnector.TICKTOCKDB_CONFIG, TICKTOCKDB);
    }

    @Test
    public void testConnectorConfigValidation() {
        List<ConfigValue> configValues = connector.config().validate(sinkProperties);
        for (ConfigValue val : configValues) {
            assertEquals(0, val.errorMessages().size(), "Config property errors: " + val.errorMessages());
        }
    }

    @Test
    public void testSinkTasks() {
        connector.start(sinkProperties);
        List<Map<String, String>> taskConfigs = connector.taskConfigs(1);
        assertEquals(1, taskConfigs.size());

        taskConfigs = connector.taskConfigs(2);
        assertEquals(2, taskConfigs.size());
    }

    @Test
    public void testSinkTasksStdout() {
        sinkProperties.remove(TickTockDBSinkConnector.TICKTOCKDB_CONFIG);
        connector.start(sinkProperties);
        List<Map<String, String>> taskConfigs = connector.taskConfigs(1);
        assertEquals(1, taskConfigs.size());
        assertNull(taskConfigs.get(0).get(TickTockDBSinkConnector.TICKTOCKDB_CONFIG));
    }

    @Test
    public void testTaskClass() {
        connector.start(sinkProperties);
        assertEquals(TickTockDBSinkTask.class, connector.taskClass());
    }

    @Test
    public void testConnectorConfigsPropagateToTaskConfigs() {
        // This is required so that updates in transforms/converters/clients configs get reflected
        // in tasks without manual restarts of the tasks (see https://issues.apache.org/jira/browse/KAFKA-13809)
        sinkProperties.put("transforms", "insert");
        connector.start(sinkProperties);
        List<Map<String, String>> taskConfigs = connector.taskConfigs(1);
        assertEquals(1, taskConfigs.size());
        assertEquals("insert", taskConfigs.get(0).get("transforms"));
    }
}
