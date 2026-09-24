/*
 * Copyright 2026 Aiven Oy
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.aiven.kafka.connect.opensearch.transport;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLException;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.file.Path;
import java.security.KeyStore;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.common.config.SslConfigs;

import io.aiven.kafka.connect.opensearch.OpenSearchSinkConnectorConfig;

import com.sun.net.httpserver.HttpsConfigurator;
import com.sun.net.httpserver.HttpsServer;
import org.apache.hc.client5.http.async.methods.SimpleHttpResponse;
import org.apache.hc.client5.http.async.methods.SimpleRequestBuilder;
import org.apache.hc.client5.http.impl.async.HttpAsyncClients;
import org.apache.hc.core5.ssl.SSLContextBuilder;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * Connects as {@code localhost} to a server whose certificate is issued for {@code wrong.example.com}. The certificate
 * itself is trusted, so only hostname verification can fail the handshake.
 */
class HttpClientConfigCallbackTest {

    private static final char[] PASSWORD = "changeit".toCharArray();

    private static HttpsServer server;

    private static Path keyStorePath;

    @BeforeAll
    static void startServer() throws Exception {
        keyStorePath = Path.of(HttpClientConfigCallbackTest.class.getResource("/tls/wrong-hostname.p12").toURI());
        final var keyStore = KeyStore.getInstance("PKCS12");
        try (var in = HttpClientConfigCallbackTest.class.getResourceAsStream("/tls/wrong-hostname.p12")) {
            keyStore.load(in, PASSWORD);
        }
        final var keyManagerFactory = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
        keyManagerFactory.init(keyStore, PASSWORD);
        final var serverSslContext = SSLContext.getInstance("TLS");
        serverSslContext.init(keyManagerFactory.getKeyManagers(), null, null);

        server = HttpsServer.create(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0), 0);
        server.setHttpsConfigurator(new HttpsConfigurator(serverSslContext));
        server.createContext("/", exchange -> {
            exchange.sendResponseHeaders(200, -1);
            exchange.close();
        });
        server.start();
    }

    @AfterAll
    static void stopServer() {
        server.stop(0);
    }

    @Test
    void acceptsMismatchedHostnameWhenEndpointIdentificationDisabled() throws Exception {
        final var response = get(Map.of(OpenSearchSinkConnectorConfig.SSL_CONFIG_PREFIX
                + SslConfigs.SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_CONFIG, ""));

        assertEquals(200, response.getCode());
    }

    @Test
    void rejectsMismatchedHostnameByDefault() {
        final var e = assertThrows(ExecutionException.class, () -> get(Map.of()));

        assertInstanceOf(SSLException.class, e.getCause());
    }

    private SimpleHttpResponse get(final Map<String, String> extraProps) throws Exception {
        final var props = new HashMap<>(extraProps);
        props.put(OpenSearchSinkConnectorConfig.CONNECTION_URL_CONFIG, url());
        final var config = new OpenSearchSinkConnectorConfig(props);
        final var clientSslContext = SSLContextBuilder.create()
                .setKeyStoreType("PKCS12")
                .loadTrustMaterial(keyStorePath, PASSWORD)
                .build();

        try (var client = new HttpClientConfigCallback(clientSslContext, config)
                .customizeHttpClient(HttpAsyncClients.custom())
                .build()) {
            client.start();
            return client.execute(SimpleRequestBuilder.get(url()).build(), null).get(10, TimeUnit.SECONDS);
        }
    }

    private static String url() {
        return "https://localhost:" + server.getAddress().getPort() + "/";
    }
}
