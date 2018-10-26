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
package at.ihet.camel.extras.cifs;

import com.hierynomus.smbj.SMBClient;
import com.hierynomus.smbj.share.DiskEntry;
import org.apache.camel.Exchange;
import org.apache.camel.Processor;
import org.apache.camel.component.file.GenericFile;
import org.apache.camel.component.file.GenericFileEndpoint;
import org.apache.camel.impl.DefaultExchange;
import org.apache.camel.processor.idempotent.MemoryIdempotentRepository;
import org.apache.camel.spi.UriEndpoint;

import java.io.File;

/**
 * @author Thomas Herzog <herzog.thomas81@gmail.com>
 * @since 10/26/2018
 */
@UriEndpoint(scheme = "cifs", title = "CIFS", syntax = "cifs://server/share?username=user&password=secret[&domain=domain][&version=version]", consumerClass = CifsConsumer.class)
public class CifsEndpoint extends GenericFileEndpoint<DiskEntry> {

    public CifsEndpoint(final String endpointUri,
                        final CifsComponent component,
                        final CifsConfiguration configuration) {
        super(endpointUri, component);
        if (configuration != null) {
            this.configuration = configuration;
        }
    }

    @Override
    public CifsConsumer createConsumer(Processor processor) throws Exception {
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

        final CifsConfiguration configuration = getConfiguration();
        CifsConsumer consumer = new CifsConsumer(this,
                                                 processor,
                                                 new CifsFileOperations(new SMBClient(configuration.getSmbConfig()), configuration),
                                                 processStrategy != null ? processStrategy : createGenericFileStrategy());
        consumer.setMaxMessagesPerPoll(getMaxMessagesPerPoll());
        consumer.setEagerLimitMaxMessagesPerPoll(isEagerMaxMessagesPerPoll());
        configureConsumer(consumer);

        return consumer;
    }

    @Override
    public CifsProducer createProducer() {
        final CifsConfiguration configuration = getConfiguration();
        return new CifsProducer(this, new CifsFileOperations(new SMBClient(configuration.getSmbConfig()), configuration));
    }

    @Override
    public Exchange createExchange(GenericFile<DiskEntry> file) {
        Exchange answer = new DefaultExchange(this);
        if (file != null) {
            file.bindToExchange(answer);
        }
        return answer;
    }

    @Override
    public String getScheme() {
        return "cifs";
    }

    @Override
    public char getFileSeparator() {
        return File.separatorChar;
    }

    @Override
    public boolean isAbsolute(String name) {
        return false;
    }

    public CifsConfiguration getConfiguration() {
        return (CifsConfiguration) configuration;
    }
}
