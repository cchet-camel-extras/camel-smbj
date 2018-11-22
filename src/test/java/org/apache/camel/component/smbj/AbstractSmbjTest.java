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
import org.junit.AfterClass;
import org.junit.BeforeClass;

import java.io.IOException;
import java.nio.file.*;

/**
 * @author Thomas Herzog <herzog.thomas81@gmail.com>
 * @since 11/5/2018
 */
public abstract class AbstractSmbjTest extends ContainerAwareTestSupport {

    protected static final String CONTAINER_IMAGE = "dperson/samba:latest";
    protected static final String CONTAINER_NAME = "smbj";
    protected static final String WORK_DIR = getTmpDir() + "camel-smbj-test";

    @BeforeClass
    public static void beforeTests() throws IOException {
        afterTests();
        Files.createDirectories(Paths.get(WORK_DIR));
    }

    @AfterClass
    public static void afterTests() throws IOException {
        deleteDirectoryRecursion(Paths.get(WORK_DIR));
    }

    protected static String getTmpDir() {
        return System.getProperty("java.io.tmpdir");
    }

    protected static void cleanupDirectory(final Path dirPath) throws IOException {
        if (Files.isDirectory(dirPath)) {
            deleteDirectoryRecursion(dirPath);
            Files.createDirectories(dirPath);
        }
    }

    private static void deleteDirectoryRecursion(Path path) throws IOException {
        if (Files.isDirectory(path, LinkOption.NOFOLLOW_LINKS)) {
            try (DirectoryStream<Path> entries = Files.newDirectoryStream(path)) {
                for (Path entry : entries) {
                    deleteDirectoryRecursion(entry);
                }
            }
        } else {
            Files.deleteIfExists(path);
        }
    }

    protected abstract String createTestFileForShare(final String fileName,
                                                     final String... directories) throws IOException;

    protected abstract String createSmbBaseUri();

    protected abstract String createSmbBaseUri(String username,
                                               String pwd);
}
