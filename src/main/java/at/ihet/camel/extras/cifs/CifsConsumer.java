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
import org.apache.camel.Message;
import org.apache.camel.Processor;
import org.apache.camel.component.file.GenericFile;
import org.apache.camel.component.file.GenericFileConsumer;
import org.apache.camel.component.file.GenericFileProcessStrategy;

import java.util.List;
import java.util.Objects;

/**
 * @author Thomas Herzog <herzog.thomas81@gmail.com>
 * @since 10/26/2018
 */
public class CifsConsumer extends GenericFileConsumer<DiskEntry> {

    public CifsConsumer(final CifsEndpoint endpoint,
                        final Processor processor,
                        final CifsFileOperations operations,
                        final GenericFileProcessStrategy<DiskEntry> processStrategy) {
        super(Objects.requireNonNull(endpoint, "Cannot create consumer with null endpoint"),
              Objects.requireNonNull(processor, "Cannot create consumer with null processor"),
              Objects.requireNonNull(operations, "Cannot create consumer with null operations"),
              Objects.requireNonNull(processStrategy, "Cannot create consumer with null processStrategy"));
    }

    @Override
    protected boolean pollDirectory(String fileName,
                                    List<GenericFile<DiskEntry>> genericFiles,
                                    int depth) {
        return false;
    }

    @Override
    protected void updateFileHeaders(GenericFile<DiskEntry> file,
                                     Message message) {

    }

    @Override
    protected boolean isMatched(GenericFile<DiskEntry> file,
                                String doneFileName,
                                List<DiskEntry> files) {
        return false;
    }
}
