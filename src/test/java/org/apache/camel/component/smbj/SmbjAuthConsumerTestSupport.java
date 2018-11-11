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

import org.apache.camel.test.testcontainers.Wait;
import org.testcontainers.containers.BindMode;
import org.testcontainers.containers.GenericContainer;

import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.Arrays;

/**
 * @author Thomas Herzog <herzog.thomas81@gmail.com>
 * @since 11/11/2018
 */
public abstract class SmbjAuthConsumerTestSupport extends AbstractSmbjTest {

    protected static final String SMB_USER = "smbuser";
    protected static final String SMB_PWD = SMB_USER;
    protected static final String SMB_SHARE = SMB_USER;
    protected static final String SMB_SHARE_DIR = SMB_DIR + File.separator + SMB_USER;

    @Override
    protected void cleanupDirectories() throws IOException {
        cleanupDirectory(Paths.get(SMB_SHARE_DIR));
    }

    @Override
    protected String createSmbBaseUri() {
        return SMB_USER + ":" + SMB_PWD + "@localhost:50445/" + SMB_SHARE + "?versions=2_1";
    }

    @Override
    protected GenericContainer<?> createContainer() {
        final GenericContainer container = new GenericContainer(CONTAINER_IMAGE)
                .withNetworkAliases(CONTAINER_NAME)
                // Here we have the share directory
                .withFileSystemBind(SMB_DIR, "/shares", BindMode.READ_WRITE)
                .withCommand("-u", String.format("%s;%s", SMB_USER, SMB_PWD),
                             "-s", String.format("%s;/shares/%s;yes;no;no;%s;%s;%s;", SMB_SHARE, SMB_USER, SMB_USER, SMB_USER, SMB_USER),
                             "-W")
                .waitingFor(Wait.forLogMessageContaining("ready to serve connections", 1));
        // The ports to connect to smb share
        container.setPortBindings(Arrays.asList("50137:137", "50138:138", "50139:139", "50445:445"));

        return container;
    }
}
