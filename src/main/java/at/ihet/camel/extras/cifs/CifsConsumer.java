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

import com.hierynomus.msfscc.fileinformation.FileAllInformation;
import com.hierynomus.smbj.share.DiskEntry;
import org.apache.camel.Message;
import org.apache.camel.Processor;
import org.apache.camel.component.file.GenericFile;
import org.apache.camel.component.file.GenericFileConsumer;
import org.apache.camel.component.file.GenericFileProcessStrategy;
import org.apache.camel.util.FileUtil;
import org.apache.camel.util.ObjectHelper;

import java.io.IOException;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

/**
 * @author Thomas Herzog <herzog.thomas81@gmail.com>
 * @since 10/26/2018
 */
public class CifsConsumer extends GenericFileConsumer<DiskEntry> {

    private String currentRelativePath = "";

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
                                    List<GenericFile<DiskEntry>> fileList,
                                    int depth) {

        log.trace(String.format("pollDirectory() running. My delay is [%s] and my strategy is [%s]", this.getDelay(), this.getPollStrategy().getClass().toString()));
        log.trace(String.format("pollDirectory() fileName[%s]", fileName));

        List<DiskEntry> smbFiles;
        smbFiles = operations.listFiles(fileName);
        for (DiskEntry diskEntry : smbFiles) {
            if (!canPollMoreFiles(fileList)) {
                return false;
            }

            if (diskEntry.getFileInformation(FileAllInformation.class).getStandardInformation().isDirectory()) {
                if (endpoint.isRecursive()) {
                    currentRelativePath = diskEntry.getFileName().split("/")[0] + "/";
                    int nextDepth = depth++;
                    pollDirectory(fileName + "/" + diskEntry.getFileName(), fileList, nextDepth);
                } else {
                    currentRelativePath = "";
                }
            } else {
                try {
                    GenericFile<DiskEntry> genericFile = asGenericFile(fileName, diskEntry);
                    if (isValidFile(genericFile, false, smbFiles)) {
                        fileList.add(asGenericFile(fileName, diskEntry));
                    }
                } catch (IOException e) {
                    throw ObjectHelper.wrapRuntimeCamelException(e);
                }
            }
        }
        return true;
    }

    @Override
    protected void updateFileHeaders(GenericFile<DiskEntry> genericFile,
                                     Message message) {
        // TODO
    }

    // TODO: this needs some checking!
    private GenericFile<DiskEntry> asGenericFile(String path,
                                                 DiskEntry file) throws IOException {
        CifsGenericFile answer = new CifsGenericFile();
        answer.setAbsoluteFilePath(path + answer.getFileSeparator() + file.getFileName());
        answer.setAbsolute(true);
        answer.setEndpointPath("/");
        answer.setFileNameOnly(file.getFileName());
        answer.setFileLength(file.getFileInformation().getStandardInformation().getEndOfFile());
        answer.setFile(file);
        answer.setLastModified(file.getFileInformation().getBasicInformation().getChangeTime().toEpochMillis());
        answer.setFileName(currentRelativePath + file.getFileName());
        answer.setRelativeFilePath(file.getFileName());

        log.trace("asGenericFile():");
        log.trace(String.format("absoluteFilePath[%s] filenameonly[%s] filename[%s] relativepath[%s]",
                                answer.getAbsoluteFilePath(),
                                answer.getFileNameOnly(),
                                answer.getFileName(),
                                answer.getRelativeFilePath()));
        return answer;
    }

    @Override
    protected boolean isMatched(GenericFile<DiskEntry> file,
                                String doneFileName,
                                List<DiskEntry> files) {
        String onlyName = FileUtil.stripPath(doneFileName);

        for (DiskEntry f : files) {
            if (f.getFileName().equals(onlyName)) {
                return true;
            }
        }

        log.trace("Done file: {} does not exist", doneFileName);
        return false;
    }

    @Override
    protected boolean isRetrieveFile() {
        return Optional.ofNullable(((CifsEndpoint) getEndpoint()).getConfiguration().getDownload()).orElse(false);
    }

}
