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
import org.junit.After;
import org.junit.Before;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.utility.MountableFile;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Objects;
import java.util.Random;

/**
 * @author Thomas Herzog <herzog.thomas81@gmail.com>
 * @since 11/11/2018
 */
public abstract class SmbjAuthConsumerTestSupport extends AbstractSmbjTest {

    protected static final String SMB_USER = "smbuser";
    protected static final String SMB_PWD = SMB_USER;
    private static final String SMB_SHARE = SMB_USER;
    private static final Random random = new Random(System.currentTimeMillis());
    private final String TMP_DIR;
    private final String SMB_SHARE_DIR;
    private GenericContainer container;

    public SmbjAuthConsumerTestSupport() {
        TMP_DIR = WORK_DIR + File.separator + "auth-consumer-tests-" + (random.nextInt(100000) + 1);
        SMB_SHARE_DIR = TMP_DIR + File.separator + "smb-source-" + (random.nextInt(100000) + 1);
    }

    @Before
    public void prepare() throws IOException {
        cleanup();
        Files.createDirectories(Paths.get(SMB_SHARE_DIR));
    }

    @After
    public void cleanup() throws IOException {
        cleanupDirectory(Paths.get(TMP_DIR));
    }

    @Override
    protected String createSmbBaseUri() {
        return SMB_USER + ":" + SMB_PWD + "@localhost:50445/" + SMB_SHARE + "?versions=2_1";
    }

    @Override
    protected String createSmbBaseUri(final String username,
                                      final String pwd) {
        return username + ":" + pwd + "@localhost:50445/" + SMB_SHARE + "?versions=2_1";
    }

    @Override
    protected GenericContainer<?> createContainer() {
        try {
            container = new GenericContainer(CONTAINER_IMAGE)
                    .withNetworkAliases(CONTAINER_NAME).withPrivilegedMode(true)
                    //.withFileSystemBind(SMB_SHARE_DIR, String.format("/shares/%s", SMB_USER), BindMode.READ_WRITE)
                    .withCommand("-u", String.format("%s;%s", SMB_USER, SMB_PWD),
                                 "-s", String.format("%s;/shares/%s;yes;no;no;%s;%s;%s;", SMB_SHARE, SMB_USER, SMB_USER, SMB_USER, SMB_USER),
                                 "-W")
                    .waitingFor(Wait.forLogMessageContaining("ready to serve connections", 1));
            // The ports to connect to smb share
            container.setPortBindings(Arrays.asList("50137:137", "50138:138", "50139:139", "50445:445"));

            return container;
        } catch (Exception e) {
            throw new IllegalStateException("Could not create container", e);
        }
    }

    protected String createTestFileForShare(final String fileName,
                                            final String... directories) throws IOException {
        if (Objects.requireNonNull(fileName, "File must not be empty").trim().isEmpty()) {
            throw new IllegalArgumentException("File name must not be empty");
        }

        String parentHost = SMB_SHARE_DIR;
        String parent = "";
        if (directories != null && directories.length > 0) {
            parent = "/" + String.join("/", directories);
            parentHost += String.join(File.separator, directories);
        }
        final Path path = Paths.get(parentHost, fileName);
        Files.deleteIfExists(path);
        Files.createDirectories(path.getParent());
        Files.createFile(path);

        container.copyFileToContainer(MountableFile.forHostPath(path), String.format("/shares/%s%s/%s", SMB_USER, parent, fileName));

        return path.normalize().toString();
    }
}
