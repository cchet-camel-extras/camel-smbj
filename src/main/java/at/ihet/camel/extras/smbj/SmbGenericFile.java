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
package at.ihet.camel.extras.smbj;

import org.apache.camel.component.file.GenericFile;

/**
 * @author Thomas Herzog <herzog.thomas81@gmail.com>
 * @since 10/27/2018
 */
public class SmbGenericFile extends GenericFile<SmbFile> {

    @Override
    public char getFileSeparator() {
        return '\\';
    }
}
