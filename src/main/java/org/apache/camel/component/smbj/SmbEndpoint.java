/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.camel.component.smbj;

import com.hierynomus.smbj.SMBClient;
import org.apache.camel.Exchange;
import org.apache.camel.Processor;
import org.apache.camel.component.file.GenericFile;
import org.apache.camel.component.file.GenericFileEndpoint;
import org.apache.camel.impl.DefaultExchange;
import org.apache.camel.processor.idempotent.MemoryIdempotentRepository;
import org.apache.camel.spi.UriEndpoint;
import org.apache.camel.spi.UriParam;

import java.net.URI;
import java.util.Map;

/**
 * @author Thomas Herzog <herzog.thomas81@gmail.com>
 * @since 10/26/2018
 */
@UriEndpoint(scheme = "smb", title = "SMB", syntax = "smb://server[:port]/share[?options]", consumerClass = SmbConsumer.class)
public class SmbEndpoint extends GenericFileEndpoint<SmbFile> {

    private String host;
    private Integer port;
    private String share;

    @UriParam(name = "download", defaultValue = "false", defaultValueNote = "Per default files are not downloaded", description = "True if file download is intended, false otherwise", javaType = "java.lang.Boolean")
    private Boolean download = false;
    @UriParam(name = "fastExistsCheck", defaultValue = "false", defaultValueNote = "Fast exists check is disabled per default", description = "True if fast exist check mode is enabled, false otherwise", javaType = "java.lang.Boolean")
    private boolean fastExistsCheck = false;

    public SmbEndpoint(final String endpointUri,
                       final SmbComponent component,
                       final SmbConfiguration configuration) {
        super(endpointUri, component);
        if (configuration != null) {
            this.configuration = configuration;
        }
        if (endpointUri != null) {
            configure(URI.create(endpointUri));
        }
    }

    @Override
    public SmbConsumer createConsumer(Processor processor) throws Exception {
        // Cannot delete and move at the same time
        if (isDelete() && getMove() != null) {
            throw new IllegalArgumentException("You cannot set both delete=true and move options");
        }

        // if noop=true then idempotent should also be configured
        if (isNoop() && !isIdempotentSet()) {
            log.info("Endpoint is configured with noop=true so forcing endpoint to be idempotent as well");
            setIdempotent(true);
        }

        // if idempotent and no repository set then create a default one
        if (isIdempotentSet() && isIdempotent() && idempotentRepository == null) {
            log.info("Using default memory based idempotent repository with cache max size: " + DEFAULT_IDEMPOTENT_CACHE_SIZE);
            idempotentRepository = MemoryIdempotentRepository.memoryIdempotentRepository(DEFAULT_IDEMPOTENT_CACHE_SIZE);
        }

        final SmbConfiguration configuration = getConfiguration();
        final SmbFileOperations fileOperations = new SmbFileOperations(new SMBClient(configuration.getSmbConfig()));
        fileOperations.setEndpoint(this);
        SmbConsumer consumer = new SmbConsumer(this,
                                               processor,
                                               fileOperations);
        consumer.setMaxMessagesPerPoll(getMaxMessagesPerPoll());
        consumer.setEagerLimitMaxMessagesPerPoll(isEagerMaxMessagesPerPoll());
        configureConsumer(consumer);

        return consumer;
    }

    @Override
    public SmbProducer createProducer() {
        final SmbConfiguration configuration = getConfiguration();
        final SmbFileOperations fileOperations = new SmbFileOperations(new SMBClient(configuration.getSmbConfig()));
        fileOperations.setEndpoint(this);

        return new SmbProducer(this, fileOperations);
    }

    @Override
    public Exchange createExchange(GenericFile<SmbFile> file) {
        Exchange answer = new DefaultExchange(this);
        if (file != null) {
            file.bindToExchange(answer);
        }
        return answer;
    }

    @Override
    public String getScheme() {
        return "smb";
    }

    @Override
    public char getFileSeparator() {
        return '\\';
    }

    @Override
    public boolean isAbsolute(String name) {
        return false;
    }

    @Override
    public SmbConfiguration getConfiguration() {
        return (SmbConfiguration) configuration;
    }

    @Override
    protected Map<String, Object> getParamsAsMap() {
        Map<String, Object> map = super.getParamsAsMap();
        map.put("fastExistsCheck", fastExistsCheck);
        return map;
    }

    private void configure(URI uri) {
        setHost(uri.getHost());
        if (uri.getPort() <= 0) {
            setPort(null);
        } else {
            setPort(uri.getPort());
        }
        if (uri.getPath().chars().filter(c -> c == '\\').filter(c -> c == '/').count() > 1) {
            throw new IllegalArgumentException(String.format("URI path cannot contain '%s' or '%s' characters", "\\", "/"));
        }
        setShare(uri.getPath().replaceAll("\\\\", "").replace("/", ""));
    }

    //<editor-fold desc="Getter and Setter">
    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public Integer getPort() {
        return port;
    }

    public void setPort(Integer port) {
        this.port = port;
    }

    public String getShare() {
        return share;
    }

    public void setShare(String share) {
        this.share = share;
    }

    public Boolean getDownload() {
        return download;
    }

    public void setDownload(Boolean download) {
        this.download = download;
    }

    public boolean isFastExistsCheck() {
        return fastExistsCheck;
    }

    public void setFastExistsCheck(boolean fastExistsCheck) {
        this.fastExistsCheck = fastExistsCheck;
    }
    //</editor-fold>
}
