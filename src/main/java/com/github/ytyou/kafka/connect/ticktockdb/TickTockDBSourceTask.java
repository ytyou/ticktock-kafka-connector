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
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketTimeoutException;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * TickTockDBSourceTask reads from stdin or a file.
 */
public class TickTockDBSourceTask extends SourceTask {
    private static final Logger log = LoggerFactory.getLogger(TickTockDBSourceTask.class);
    private static final Schema VALUE_SCHEMA = Schema.STRING_SCHEMA;

    private String topic;
    private int port;       // the port we are listening on
    private int timeout;    // ms
    private ServerSocket socket;
    private ConcurrentLinkedQueue<BufferedReader> readers;
    private ConcurrentLinkedQueue<DataOutputStream> writers;
    private Listener listener;

    public TickTockDBSourceTask() {
    }

    @Override
    public String version() {
        return new TickTockDBSourceConnector().version();
    }

    @Override
    public void start(Map<String, String> props) {
        readers = new ConcurrentLinkedQueue<BufferedReader>();
        writers = new ConcurrentLinkedQueue<DataOutputStream>();

        AbstractConfig config = new AbstractConfig(TickTockDBSourceConnector.CONFIG_DEF, props);
        topic = config.getString(TickTockDBSourceConnector.TOPIC_CONFIG);
        port = config.getInt(TickTockDBSourceConnector.PORT_CONFIG);
        timeout = config.getInt(TickTockDBSourceConnector.TIMEOUT_CONFIG);  // ms

        try {
            socket = new ServerSocket(port);
            socket.setSoTimeout(timeout);
        } catch (Exception e) {
            socket = null;
            log.error("Error creating socket on port {}: {}", port, e);
        }

        listener = new Listener(socket);
        listener.start();     // start the socket listening thread
    }

    @Override
    public List<SourceRecord> poll() throws InterruptedException {
        ArrayList<SourceRecord> records = null;
        Iterator<BufferedReader> rit = readers.iterator();
        Iterator<DataOutputStream> wit = writers.iterator();

        while (rit.hasNext() && wit.hasNext()) {
            BufferedReader reader = rit.next();
            DataOutputStream writer = wit.next();

            try {
                if (! reader.ready())
                    continue;
                for (int i = 0; i < 10; i++) {  // TODO: config BATCH_SIZE
                    String line = reader.readLine();
                    if (line == null || line.isEmpty())
                        continue;
                    if (line.equals("version"))
                    {
                        writer.writeUTF("{\"repo\":\"github.com/ytyou/ticktock.git\",\"version\":\"1.0.0\",\"branch\":\"main\"}");
                        continue;
                    }
                    log.info("ADDED A LINE!!!!!!!!!!!!!!: " + line);
                    if (records == null)
                        records = new ArrayList<>();
                    records.add(new SourceRecord(Collections.emptyMap(), Collections.emptyMap(),
                        topic, null, null, null, VALUE_SCHEMA, line, System.currentTimeMillis()));
                }
            } catch (SocketTimeoutException e) {
                // OK, no data at the moment
            } catch (Exception e) {
                try {
                    reader.close();
                    writer.close();
                } catch (Exception ignore) {
                }
                rit.remove();
                wit.remove();
                log.warn("Connection closed: {}", e);
            }
        }
        return records;
    }

    @Override
    public void stop() {
        log.trace("Stopping");
        if (listener != null)
            listener.terminate();
    }

    private void addSocket(Socket so) {
        try {
            readers.add(new BufferedReader(new InputStreamReader(so.getInputStream())));
            writers.add(new DataOutputStream(so.getOutputStream()));
            log.info("ADDED A READER!!!!!!!!!!!!!!");
        } catch (Exception e) {
            log.error("Error creating input stream from socket: {}", e);
        }
    }


    private class Listener extends Thread {
        private ServerSocket socket;
        private volatile boolean running = true;

        public Listener(ServerSocket socket) {
            this.socket = socket;
        }

        public void terminate() {
            running = false;
        }

        private void sleep(int ms) {
            if (! running)
                return;
            try {
                Thread.sleep(ms);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }

        public void run() {
            while (running) {
                try {
                    Socket so = this.socket.accept();
                    addSocket(so);
                } catch (SocketTimeoutException e) {
                    sleep(1000);
                } catch (Exception e) {
                    Thread.currentThread().interrupt();
                    running = false;
                    break;
                }
            }
            try {
                socket.close();
            } catch (Exception e) {
            }
        }
    }
}
