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

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.common.utils.AppInfoParser;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.source.ExactlyOnceSupport;
import org.apache.kafka.connect.source.SourceConnector;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Very simple source connector that works with stdin or a file.
 */
public class TickTockDBSourceConnector extends SourceConnector {

    private static final Logger log = LoggerFactory.getLogger(TickTockDBSourceConnector.class);
    public static final String TOPIC_CONFIG = "topic";
    public static final String PORT_CONFIG = "port";
    public static final String TIMEOUT_CONFIG = "timeout";

    public static final int DEFAULT_PORT = 6181;
    public static final int DEFAULT_TIMEOUT = 100;  // ms

    static final ConfigDef CONFIG_DEF = new ConfigDef()
        .define(TOPIC_CONFIG, Type.STRING, ConfigDef.NO_DEFAULT_VALUE, new ConfigDef.NonEmptyString(), Importance.HIGH, "The topic to publish data to")
        .define(PORT_CONFIG, Type.INT, DEFAULT_PORT, Importance.LOW, "The port we will listen on")
        .define(TIMEOUT_CONFIG, Type.INT, DEFAULT_TIMEOUT, Importance.LOW, "The timeout, in ms, while reading from incoming network traffic");

    private Map<String, String> props;

    @Override
    public String version() {
        return AppInfoParser.getVersion();
    }

    @Override
    public void start(Map<String, String> props) {
        this.props = props;
    }

    @Override
    public Class<? extends Task> taskClass() {
        return TickTockDBSourceTask.class;
    }

    @Override
    public List<Map<String, String>> taskConfigs(int maxTasks) {
        ArrayList<Map<String, String>> configs = new ArrayList<>();
        // Only one input stream makes sense.
        configs.add(props);
        return configs;
    }

    @Override
    public void stop() {
        // Nothing to do since FileStreamSourceConnector has no background monitoring.
    }

    @Override
    public ConfigDef config() {
        return CONFIG_DEF;
    }

    @Override
    public ExactlyOnceSupport exactlyOnceSupport(Map<String, String> props) {
        return ExactlyOnceSupport.UNSUPPORTED;
    }
}
