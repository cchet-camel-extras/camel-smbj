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

import java.io.File;
import java.util.Arrays;
import java.util.regex.Matcher;

/**
 * @author Thomas Herzog <herzog.thomas81@gmail.com>
 * @since 10/27/2018
 */
public class SmbFile {

    private final boolean isDirectory;
    private final boolean isArchive;
    private final boolean isHidden;
    private final boolean isReadOnly;
    private final boolean isSystem;
    private final String fileNameFull;
    private final String parentDirectory;
    private final String fileName;
    private final long fileLength;
    private final long lastModified;

    public SmbFile(boolean isDirectory,
                   boolean isArchive,
                   boolean isHidden,
                   boolean isReadOnly,
                   boolean isSystem,
                   String fileNameFull,
                   long fileLength,
                   long lastModified) {
        this.isDirectory = isDirectory;
        this.isArchive = isArchive;
        this.isHidden = isHidden;
        this.isReadOnly = isReadOnly;
        this.isSystem = isSystem;
        this.fileLength = fileLength;
        this.lastModified = lastModified;

        final String[] fileNameParts = fileNameFull.split(Matcher.quoteReplacement("\\|/"));
        fileName = fileNameParts[fileNameParts.length - 1];
        parentDirectory = String.join(File.separator, Arrays.copyOfRange(fileNameParts, 0, fileNameParts.length - 1));
        this.fileNameFull = parentDirectory + File.separator + fileName;

    }

    public boolean isDirectory() {
        return isDirectory;
    }

    public boolean isArchive() {
        return isArchive;
    }

    public boolean isHidden() {
        return isHidden;
    }

    public boolean isReadOnly() {
        return isReadOnly;
    }

    public boolean isSystem() {
        return isSystem;
    }

    public String getFileNameFull() {
        return fileNameFull;
    }

    public String getParentDirectory() {
        return parentDirectory;
    }

    public String getFileName() {
        return fileName;
    }

    public long getFileLength() {
        return fileLength;
    }

    public long getLastModified() {
        return lastModified;
    }
}
