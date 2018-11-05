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

import org.apache.camel.test.testcontainers.ContainerAwareTestSupport;
import org.apache.camel.test.testcontainers.Wait;
import org.testcontainers.containers.BindMode;
import org.testcontainers.containers.GenericContainer;

import java.io.File;
import java.nio.file.Paths;
import java.util.Arrays;

/**
 * @author Thomas Herzog <herzog.thomas81@gmail.com>
 * @since 11/5/2018
 */
public abstract class AbstractSmbjTest extends ContainerAwareTestSupport {

    protected static final String CONTAINER_IMAGE = "dperson/samba:latest";
    protected static final String CONTAINER_NAME = "smbj";
    protected static final String SMB_SHARE_DIR = System.getProperty("java.io.tmpdir") + "camel-smbj-test-share";
    protected static final String SMB_SHARE_SOURCE_DIR = "/shares/source";
    protected static final String SMB_SHARE_TARGET_DIR = "/shares/target";

    protected static void cleanupTestSharecontent() {
        final File dir = Paths.get(SMB_SHARE_DIR).toFile();
        if (dir.isDirectory()) {
            Arrays.stream(dir.listFiles()).forEach(File::delete);
        }
    }

    @Override
    protected GenericContainer<?> createContainer() {
        final GenericContainer container = new GenericContainer(CONTAINER_IMAGE)
                .withNetworkAliases(CONTAINER_NAME)
                // Here we have the share directory
                .withFileSystemBind(SMB_SHARE_DIR, "/share", BindMode.READ_WRITE)
                .withCommand("-u", "source;source",
                             "-u", "target;target",
                             "-s", String.format("source;%s;yes;no;no;source;source;source", SMB_SHARE_SOURCE_DIR),
                             "-s", String.format("target;%s;yes;no;no;target;target;target", SMB_SHARE_TARGET_DIR),
                             "-W")
                .waitingFor(Wait.forLogMessageContaining("ready to serve connections", 1));
        // The ports to connect to smb share
        container.setPortBindings(Arrays.asList("50137:137", "50138:138", "50139:139", "50445:445"));

        return container;
    }
}
