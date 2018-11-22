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
import org.junit.Test;

import java.io.IOException;

/**
 * @author Thomas Herzog <herzog.thomas81@gmail.com>
 * @since 11/5/2018
 */
public class SmbjConsumerTest extends SmbjAuthConsumerTestSupport {

    @EndpointInject(uri = "mock:result")
    private MockEndpoint mockEndpoint;

    @Test
    public void consumerRoute_withSingleSharedFile_mustBeConsumed() throws InterruptedException, IOException {
        // -- Given --
        final String actualFileName = "test.txt";
        createTestFileForShare(actualFileName);
        mockEndpoint.expectedMessageCount(1);
        mockEndpoint.expectedHeaderReceived(Exchange.FILE_NAME_ONLY, actualFileName);

        // -- When --
        mockEndpoint.setAssertPeriod(500);

        // -- Then --
        mockEndpoint.assertIsSatisfied();
    }

    @Test
    public void consumerRoute_withExistingFile_mustNotBeConsumed() throws InterruptedException, IOException {
        // -- Given --
        final String actualFileName = "test.txt";
        createTestFileForShare(actualFileName);
        // TODO: Doesn't work because copy file to container demands an existing directory
        createTestFileForShare(actualFileName, ".consumed");
        mockEndpoint.expectedMessageCount(0);

        // -- When --
        mockEndpoint.setAssertPeriod(500);

        // -- Then --
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
                from("smb://" + createSmbBaseUri() + "&delay=100&move=.consumed&fileExist=Move&moveExisting=.exists&moveFailed=.failed").to(mockEndpoint);
            }
        };
    }
}
