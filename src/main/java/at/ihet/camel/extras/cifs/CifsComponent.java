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

import com.hierynomus.smbj.share.DiskEntry;
import org.apache.camel.component.file.GenericFileComponent;
import org.apache.camel.component.file.GenericFileEndpoint;

import java.util.Map;

/**
 * @author Thomas Herzog <herzog.thomas81@gmail.com>
 * @since 10/26/2018
 */
public class CifsComponent extends GenericFileComponent<DiskEntry> {

    @Override
    protected CifsEndpoint buildFileEndpoint(String uri,
                                             String remaining,
                                             Map<String, Object> parameters) throws Exception {
        return null;
    }

    @Override
    protected void afterPropertiesSet(GenericFileEndpoint<DiskEntry> endpoint) throws Exception {

    }
}
