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

import org.apache.camel.EndpointInject;
import org.apache.camel.Exchange;
import org.apache.camel.RoutesBuilder;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.component.mock.MockEndpoint;
import org.apache.camel.util.FileUtil;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Properties;

/**
 * @author Thomas Herzog <herzog.thomas81@gmail.com>
 * @since 11/5/2018
 */
public class SmbjConsumerAuthTest extends SmbjAuthConsumerTestSupport {

    @EndpointInject(uri = "mock:result")
    private MockEndpoint mockEndpoint;

    @Before
    public void beforeAndAfter() throws IOException {
        cleanupDirectories();
    }

    @Test
    public void consumerRoute_withAuth_mustBeValid() throws InterruptedException, IOException {
        // -- Given --
        final Path testFile = Paths.get(FileUtil.normalizePath(WORK_DIR + "/test.txt"));
        final Path smbShareFile = Paths.get(FileUtil.normalizePath(SMB_SHARE_DIR + "/test.txt"));
        final Path expectedFile = Paths.get(FileUtil.normalizePath(SMB_SHARE_DIR + "/.processed/test.txt"));
        System.setProperties(new Properties() {{
            setProperty("test.smb.user", SMB_USER);
            setProperty("test.smb.pwd", SMB_PWD);
        }});
        mockEndpoint.expectedMessageCount(1);
        mockEndpoint.expectedHeaderReceived(Exchange.FILE_NAME_ONLY, smbShareFile.toFile().getName());

        // -- When --
        final Path path = Files.createFile(testFile);
        Files.move(path, smbShareFile);

        // -- Then --
        mockEndpoint.setAssertPeriod(500);
        mockEndpoint.assertIsSatisfied();
    }

    @Test
    public void test2() {

    }

    @Override
    protected RoutesBuilder createRouteBuilder() throws Exception {
        return new RouteBuilder() {
            @Override
            public void configure() throws Exception {
                from("smb://" + createSmbBaseUri() + "&delete=true&delay=100").to(mockEndpoint);
            }
        };
    }
}
