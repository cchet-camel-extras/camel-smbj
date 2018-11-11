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

import java.io.File;
import java.io.IOException;
import java.nio.file.*;

/**
 * @author Thomas Herzog <herzog.thomas81@gmail.com>
 * @since 11/5/2018
 */
public abstract class AbstractSmbjTest extends ContainerAwareTestSupport {

    protected static final String CONTAINER_IMAGE = "dperson/samba:latest";
    protected static final String CONTAINER_NAME = "smbj";
    protected static final String WORK_DIR = getTmpDir() + "camel-smbj-test-share";
    protected static final String SMB_DIR = WORK_DIR + File.separator + "smb-share";
    protected static final String SMB_TMP_DIR = WORK_DIR + File.separator + "smb-tmp";

    @BeforeClass
    public static void beforeTest() throws IOException {
        deleteDirectoryRecursion(Paths.get(WORK_DIR));
        Files.createDirectories(Paths.get(SMB_TMP_DIR));
    }

    @AfterClass
    public static void afterTest() throws IOException {
        deleteDirectoryRecursion(Paths.get(WORK_DIR));
    }

    protected static void cleanupTmpDirectory() throws IOException {
        cleanupDirectory(Paths.get(SMB_TMP_DIR));
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
        }
        Files.deleteIfExists(path);
    }

    protected abstract String createSmbBaseUri();

    protected abstract void cleanupDirectories() throws IOException;
}
