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

import org.apache.camel.Message;
import org.apache.camel.Processor;
import org.apache.camel.component.file.GenericFile;
import org.apache.camel.component.file.GenericFileConsumer;
import org.apache.camel.component.file.GenericFileOperations;
import org.apache.camel.util.FileUtil;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

/**
 * @author Thomas Herzog <herzog.thomas81@gmail.com>
 * @since 10/26/2018
 */
public class SmbConsumer extends GenericFileConsumer<SmbFile> {

    private final SmbConfiguration configuration;

    public SmbConsumer(SmbEndpoint endpoint,
                       Processor processor,
                       GenericFileOperations<SmbFile> operations) {
        super(Objects.requireNonNull(endpoint, "Cannot create consumer with null endpoint"),
              Objects.requireNonNull(processor, "Cannot create consumer with null processor"),
              Objects.requireNonNull(operations, "Cannot create consumer with null operations"));
        this.configuration = endpoint.getConfiguration();
    }

    @Override
    protected boolean pollDirectory(String fileName,
                                    List<GenericFile<SmbFile>> fileList,
                                    int depth) {
        log.trace(String.format("Pooling directory with delay '%s' and strategy '%s'", this.getDelay(), this.getPollStrategy().getClass().toString()));
        log.trace(String.format("fileName[%s]", fileName));

        final List<SmbFile> smbFiles = operations.listFiles(fileName);

        // Walk found files
        for (final SmbFile file : smbFiles) {
            if (!canPollMoreFiles(fileList)) {
                return false;
            }

            if (file.isDirectory()) {
                if (endpoint.isRecursive()) {
                    int nextDepth = depth++;
                    pollDirectory(file.getFileNameFull(), fileList, nextDepth);
                }
            } else {
                final GenericFile<SmbFile> genericFile = asGenericFile(fileName, file);
                if (isValidFile(genericFile, false, smbFiles)) {
                    fileList.add(asGenericFile(fileName, file));
                }
            }
        }

        return true;
    }

    @Override
    protected void updateFileHeaders(GenericFile<SmbFile> file,
                                     Message message) {
        // TODO: What we need to do form smb file?
    }

    private GenericFile<SmbFile> asGenericFile(String path,
                                               SmbFile file) {
        final SmbGenericFile answer = new SmbGenericFile();
        answer.setAbsolute(true);
        answer.setEndpointPath("");
        answer.setAbsoluteFilePath(file.getFileNameFull());
        answer.setFileNameOnly(file.getFileName());
        answer.setFileLength(file.getFileLength());
        answer.setFile(file);
        answer.setLastModified(file.getLastModified());
        answer.setFileName(file.getFileNameFull());
        answer.setAbsolute(true);

        log.trace(String.format("absoluteFilePath[%s] filenameonly[%s] filename[%s] relativepath[%s]",
                                answer.getAbsoluteFilePath(),
                                answer.getFileNameOnly(),
                                answer.getFileName(),
                                answer.getRelativeFilePath()));
        return answer;
    }

    @Override
    protected boolean isMatched(GenericFile<SmbFile> file,
                                String doneFileName,
                                List<SmbFile> files) {
        String onlyName = FileUtil.stripPath(doneFileName);

        for (final SmbFile smbFile : files) {
            if (smbFile.getFileNameFull().equals(onlyName)) {
                return true;
            }
        }

        log.trace(String.format("Done file: %s does not exist", doneFileName));
        return false;
    }

    @Override
    protected boolean isRetrieveFile() {
        return Optional.ofNullable(((SmbEndpoint) getEndpoint()).getConfiguration().getDownload()).orElse(false);
    }
}
