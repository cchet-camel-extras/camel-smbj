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

import org.apache.camel.component.file.GenericFileComponent;
import org.apache.camel.component.file.GenericFileEndpoint;

import java.net.URI;
import java.util.Map;

/**
 * @author Thomas Herzog <herzog.thomas81@gmail.com>
 * @since 10/26/2018
 */
public class SmbComponent extends GenericFileComponent<SmbFile> {

    @Override
    protected SmbEndpoint buildFileEndpoint(String uri,
                                            String remaining,
                                            Map<String, Object> parameters) throws Exception {
        log.debug(String.format("Building file endpoint for: uri[%s], remaining[%s], parameters[%s]", uri, remaining, parameters));

        return new SmbEndpoint(uri, this, new SmbConfiguration(new URI(fixSpaces(uri))));
    }

    @Override
    protected void afterPropertiesSet(GenericFileEndpoint<SmbFile> endpoint) throws Exception {
        // Nothing to do for now
    }

    private String fixSpaces(String input) {
        return input.replace(" ", "%20");
    }

}
