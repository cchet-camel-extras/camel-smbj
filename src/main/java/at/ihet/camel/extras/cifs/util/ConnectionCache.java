/*****************************************************************
 Copyright 2015-2018 the original author or authors.

 Licensed under the Apache License, Version 2.0 (the "License");
 you may not use this file except in compliance with the License.
 You may obtain a copy of the License at

 http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 See the License for the specific language governing permissions and
 limitations under the License.
 *****************************************************************/
package at.ihet.camel.extras.cifs.util;

import com.hierynomus.smbj.SMBClient;
import com.hierynomus.smbj.connection.Connection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/**
 * This class is meant to manage connections for a host, so that the connection haven't been to be created for each operation.
 * The connections are held in a map, whereby the key for a connection has the form of 'host:port'.
 *
 * @author Thomas Herzog <herzog.thomas81@gmail.com>
 * @since 10/26/2018
 */
public class ConnectionCache {

    private static final Integer DEFAULT_PORT_KEY_PART = 0;
    private static final Logger LOG = LoggerFactory.getLogger(ConnectionCache.class);
    private static Map<String, Connection> CONNECTION_CACHE = new ConcurrentHashMap<>();

    private ConnectionCache() {
    }

    public static Connection getOrCreateConnection(final SMBClient client,
                                                   final String host,
                                                   final Integer port) {
        final String connectionKey = buildCacheKey(Objects.requireNonNull(host, "Cannot create connection for null host"),
                                                   port);
        Connection connection = CONNECTION_CACHE.computeIfAbsent(connectionKey, key -> createConnection(Objects.requireNonNull(client, "Cannot create connection for null client"), host, port));
        if (!connection.isConnected()) {
            releaseConnection(client, host, port);
            connection = CONNECTION_CACHE.putIfAbsent(connectionKey, createConnection(client, host, port));
        }

        return connection;
    }

    public static void releaseConnection(final SMBClient client,
                                         final String host,
                                         final Integer port) {
        final String connectionKey = buildCacheKey(Objects.requireNonNull(host, "Cannot create connection for null host"),
                                                   port);
        final Connection connection = CONNECTION_CACHE.remove(connectionKey);
        if (connection != null) {
            try {
                connection.close();
            } catch (IOException e) {
                LOG.info("Could not close connection during release", e);
            }
        }
    }

    private static Connection createConnection(final SMBClient client,
                                               final String host,
                                               final Integer port) {
        try {
            if (port == null) {
                return client.connect(host);
            }
            return client.connect(host, port);
        } catch (IOException e) {
            throw new IllegalStateException("Connection cannot be created", e);
        }
    }

    private static String buildCacheKey(final String host,
                                        final Integer port) {
        return String.format("%s:%d", host, Optional.of(port).orElse(DEFAULT_PORT_KEY_PART));
    }
}
