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

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.common.utils.AppInfoParser;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.sink.SinkConnector;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Very simple sink connector that works with stdout or a file.
 */
public class TickTockDBSinkConnector extends SinkConnector {

    private static final Logger log = LoggerFactory.getLogger(TickTockDBSinkConnector.class);
    public static final String TICKTOCKDB_CONFIG = "ticktockdb";
    static final ConfigDef CONFIG_DEF = new ConfigDef()
        .define(TICKTOCKDB_CONFIG, Type.STRING, null, Importance.HIGH, "TickTockDB endpoint to replicate data to");

    private Map<String, String> props;

    @Override
    public String version() {
        return AppInfoParser.getVersion();
    }

    @Override
    public void start(Map<String, String> props) {
        this.props = props;
        AbstractConfig config = new AbstractConfig(CONFIG_DEF, props);
        String ticktockdb = config.getString(TICKTOCKDB_CONFIG);
        log.info("Starting TickTockDB sink connector sending to {}", ticktockdb);
    }

    @Override
    public Class<? extends Task> taskClass() {
        // TODO: Either TickTockDBSinkPutTask.class or TickTockDBSinkWriteTask.class,
        // depending on the FORMAT_CONFIG
        return TickTockDBSinkTask.class;
    }

    @Override
    public List<Map<String, String>> taskConfigs(int maxTasks) {
        ArrayList<Map<String, String>> configs = new ArrayList<>();
        for (int i = 0; i < maxTasks; i++) {
            configs.add(props);
        }
        return configs;
    }

    @Override
    public void stop() {
        // Nothing to do since TickTockDBSinkConnector has no background monitoring.
    }

    @Override
    public ConfigDef config() {
        return CONFIG_DEF;
    }

    @Override
    public boolean alterOffsets(Map<String, String> connectorConfig, Map<TopicPartition, Long> offsets) {
        // Nothing to do here since TickTockDBSinkConnector does not manage offsets externally nor does it require any
        // custom offset validation
        return true;
    }
}
