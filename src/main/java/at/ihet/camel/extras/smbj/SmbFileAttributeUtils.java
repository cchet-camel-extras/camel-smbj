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

import com.hierynomus.msfscc.fileinformation.FileBasicInformation;

/**
 * @author Thomas Herzog <herzog.thomas81@gmail.com>
 * @since 10/27/2018
 */
public class SmbFileAttributeUtils {

    private static final long FILE_ATTRIBUTE_DIRECTORY = 0x10L;
    private static final long FILE_ATTRIBUTE_READONLY = 0x1L;
    private static final long FILE_ATTRIBUTE_HIDDEN = 0x2L;
    private static final long FILE_ATTRIBUTE_ARCHIVE = 0x20L;
    private static final long FILE_ATTRIBUTE_SYSTEM = 0x4L;

    public static boolean isDirectory(FileBasicInformation info) {
        return (info.getFileAttributes() & FILE_ATTRIBUTE_DIRECTORY) == FILE_ATTRIBUTE_DIRECTORY;
    }

    public static long getLastModified(FileBasicInformation info) {
        return info.getLastWriteTime().toEpochMillis();
    }

    public static boolean isArchive(FileBasicInformation info) {
        return (info.getFileAttributes() & FILE_ATTRIBUTE_ARCHIVE) == FILE_ATTRIBUTE_ARCHIVE;
    }

    public static boolean isHidden(FileBasicInformation info) {
        return (info.getFileAttributes() & FILE_ATTRIBUTE_HIDDEN) == FILE_ATTRIBUTE_HIDDEN;
    }

    public static boolean isReadOnly(FileBasicInformation info) {
        return (info.getFileAttributes() & FILE_ATTRIBUTE_READONLY) == FILE_ATTRIBUTE_READONLY;
    }

    public static boolean isSystem(FileBasicInformation info) {
        return (info.getFileAttributes() & FILE_ATTRIBUTE_SYSTEM) == FILE_ATTRIBUTE_SYSTEM;
    }
}
