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


import com.hierynomus.msdtyp.AccessMask;
import com.hierynomus.msfscc.fileinformation.FileBasicInformation;
import com.hierynomus.msfscc.fileinformation.FileNamesInformation;
import com.hierynomus.mssmb2.SMB2CreateDisposition;
import com.hierynomus.mssmb2.SMB2ShareAccess;
import com.hierynomus.mssmb2.SMBApiException;
import com.hierynomus.smbj.SMBClient;
import com.hierynomus.smbj.connection.Connection;
import com.hierynomus.smbj.session.Session;
import com.hierynomus.smbj.share.DiskEntry;
import com.hierynomus.smbj.share.DiskShare;
import org.apache.camel.Exchange;
import org.apache.camel.InvalidPayloadException;
import org.apache.camel.component.file.GenericFileEndpoint;
import org.apache.camel.component.file.GenericFileExist;
import org.apache.camel.component.file.GenericFileOperationFailedException;
import org.apache.camel.component.file.GenericFileOperations;
import org.apache.camel.util.FileUtil;
import org.apache.camel.util.ObjectHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.EnumSet;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * This class is the implementation of the interface {@link GenericFileOperations} for a CIFS/SMB share accessed via smbj.
 *
 * @author Thomas Herzog <herzog.thomas81@gmail.com>
 * @since 10/26/2018
 */
public class SmbFileOperations implements GenericFileOperations<SmbFile> {

    private static final Logger LOG = LoggerFactory.getLogger(SmbFileOperations.class);
    private final SMBClient client;
    private SmbConfiguration smbConfiguration;
    private String host;
    private Integer port;
    private GenericFileEndpoint<SmbFile> endpoint;

    public SmbFileOperations(final SMBClient client) {
        this.client = Objects.requireNonNull(client, "Cannot perform file operations with a null client");
    }

    private static String removeLeadingBackslash(final String name) {
        if (name.startsWith("\\")) {
            return name.replaceFirst("\\\\", "");
        }
        return name;
    }

    private static SmbFile mapDiskEntryToSmbFile(final DiskEntry entry) {
        final FileBasicInformation info = entry.getFileInformation(FileBasicInformation.class);
        final long fileSize;
        if (!entry.getFileInformation().getStandardInformation().isDirectory()) {
            fileSize = 0;
        } else {
            fileSize = entry.getFileInformation().getStandardInformation().getEndOfFile();
        }
        return new SmbFile(entry.getFileInformation().getStandardInformation().isDirectory(),
                           SmbFileAttributeUtils.isArchive(info),
                           SmbFileAttributeUtils.isHidden(info),
                           SmbFileAttributeUtils.isReadOnly(info),
                           SmbFileAttributeUtils.isSystem(info),
                           entry.getFileName(),
                           fileSize,
                           entry.getFileInformation().getBasicInformation().getChangeTime().toEpochMillis());
    }

    @Override
    public void setEndpoint(GenericFileEndpoint<SmbFile> endpoint) {
        this.endpoint = Objects.requireNonNull(endpoint, "Endpoint must not be null");
        this.smbConfiguration = (SmbConfiguration) Objects.requireNonNull(endpoint.getConfiguration(), "Cannot perform file operations with a null smb configuration");
        this.host = Objects.requireNonNull(smbConfiguration.getHost(), "Host ist required for smb connection");
        this.port = smbConfiguration.getPort();
    }

    @Override
    public boolean deleteFile(String name) throws GenericFileOperationFailedException {
        try {
            name = removeLeadingBackslash(name);
            try (final DiskShare share = openShare()) {
                if (share.fileExists(name)) {
                    share.rm(name);
                    return true;
                }
                LOG.warn(String.format("Tried to delete file which does not exists '%s'", name));
                return false;
            }
        } catch (Exception e) {
            throw new GenericFileOperationFailedException(String.format("Could not delete file '%s'", name));
        }
    }

    @Override
    public boolean existsFile(String name) throws GenericFileOperationFailedException {
        try {
            try (final DiskShare share = openShare()) {
                return share.fileExists(removeLeadingBackslash(name));
            }
        } catch (Exception e) {
            throw new GenericFileOperationFailedException(String.format("Could not delete file '%s'", name));
        }
    }

    @Override
    public boolean renameFile(String from,
                              String to) throws GenericFileOperationFailedException {
        try {
            from = removeLeadingBackslash(from);
            try (final DiskShare share = openShare()) {
                if (share.fileExists(from)) {
                    getReadOnlyFile(share, from).rename(removeLeadingBackslash(to), true);
                    return true;
                }
                LOG.warn(String.format("Tried to rename file which does not exists '%s'", from));
                return false;
            }
        } catch (Exception e) {
            throw new GenericFileOperationFailedException(String.format("Could not rename file from '%s' to '%s'", from, to), e);
        }
    }

    @Override
    public boolean buildDirectory(String directory,
                                  boolean absolute) throws GenericFileOperationFailedException {
        try {
            directory = removeLeadingBackslash(directory);
            try (final DiskShare share = openShare()) {
                if (!share.folderExists(directory)) {
                    share.mkdir(directory);
                }
                LOG.warn(String.format("Tried to create directory which already not exists '%s'", directory));
                return true;
            }
        } catch (IOException e) {
            throw new GenericFileOperationFailedException(String.format("Could not create directory '%s'", directory));
        }
    }

    @Override
    public boolean retrieveFile(String name,
                                Exchange exchange,
                                long size) throws GenericFileOperationFailedException {
        try {
            name = removeLeadingBackslash(name);
            try (final DiskShare share = openShare()) {
                if (share.fileExists(name) || share.folderExists(name)) {
                    final DiskEntry entry = getReadOnlyFile(share, name);
                    if (!entry.getFileInformation().getStandardInformation().isDirectory()) {
                        final com.hierynomus.smbj.share.File file = (com.hierynomus.smbj.share.File) entry;
                        exchange.getIn().setBody(file.getInputStream());
                        return true;
                    } else {
                        throw new GenericFileOperationFailedException(String.format("Could not retrieve file because it is a directory '%s'", name));
                    }
                }
                LOG.warn(String.format("Tried to retrieve a file which does not exists '%s'", name));
                return false;
            }
        } catch (Exception e) {
            throw new GenericFileOperationFailedException(String.format("Could not retrieve file '%s'", name), e);
        }
    }

    @Override
    public void releaseRetrievedFileResources(Exchange exchange) throws GenericFileOperationFailedException {
        // INFO: Nothing to do
    }

    @Override
    public boolean storeFile(String name,
                             Exchange exchange,
                             long size) throws GenericFileOperationFailedException {
        boolean append = false;
        name = removeLeadingBackslash(name);

        if (existsFile(name)) {
            if (endpoint.getFileExist() == GenericFileExist.Ignore) {
                // ignore but indicate that the file was written
                LOG.debug(String.format("An existing file already exists: '%s'. Ignore and do not override it.", name));
                return false;
            } else if (endpoint.getFileExist() == GenericFileExist.Fail) {
                throw new GenericFileOperationFailedException("File already exist: " + name + ". Cannot write new file.");
            } else if (endpoint.getFileExist() == GenericFileExist.Move) {
                // move any existing file first
                doMoveExistingFile(name);
            } else if (endpoint.isEagerDeleteTargetFile() && endpoint.getFileExist() == GenericFileExist.Override) {
                // we override the target so we do this by deleting it so the
                // temp file can be renamed later
                // with success as the existing target file have been deleted
                LOG.debug("Eagerly deleting existing file: " + name);
                if (!deleteFile(name)) {
                    throw new GenericFileOperationFailedException("Cannot delete file: " + name);
                }

                throw new GenericFileOperationFailedException("Cannot store the file, because it already exists");
            } else if (endpoint.getFileExist() == GenericFileExist.Append) {
                append = true;
            }
        }

        String storeName = getPath(name);

        try {
            try (final DiskShare share = openShare()) {
                final DiskEntry entry = getWritableFile(share, name);
                if (!entry.getFileInformation().getStandardInformation().isDirectory()) {
                    final com.hierynomus.smbj.share.File file = (com.hierynomus.smbj.share.File) entry;
                    try (BufferedInputStream bis = new BufferedInputStream(exchange.getMessage().getMandatoryBody(InputStream.class))) {
                        try (final BufferedOutputStream bos = new BufferedOutputStream(file.getOutputStream(append))) {
                            byte[] data = new byte[512 * 1024];
                            int dataSize;
                            while ((dataSize = bis.read(data)) != -1) {
                                bos.write(data, 0, dataSize);
                            }
                            bos.flush();
                            file.rename(storeName);
                            return true;
                        }
                    }
                } else {
                    throw new GenericFileOperationFailedException(String.format("Could not store file, because it is a directory '%s'", name));
                }
            }
        } catch (IOException | InvalidPayloadException e) {
            throw new GenericFileOperationFailedException(String.format("Could not store file '%s'", name), e);
        } catch (SMBApiException e) {
            LOG.debug("storeFile failed with exception", e);
            return false;
        }
    }

    @Override
    public String getCurrentDirectory() throws GenericFileOperationFailedException {
        throw new UnsupportedOperationException("Cannot get current directory on a smb share");
    }

    @Override
    public void changeCurrentDirectory(String path) throws GenericFileOperationFailedException {
        throw new UnsupportedOperationException("Cannot switch to a directory on a smb share");
    }

    @Override
    public void changeToParentDirectory() throws GenericFileOperationFailedException {
        throw new UnsupportedOperationException("Cannot switch to a directory on a smb share");
    }

    @Override
    public List<SmbFile> listFiles() throws GenericFileOperationFailedException {
        return listFiles("");
    }

    @Override
    public List<SmbFile> listFiles(String path) throws GenericFileOperationFailedException {
        try (final DiskShare share = openShare()) {
            return share.list(path, FileNamesInformation.class).stream()
                        // Exclude linux specific directories
                        .filter(entry -> !entry.getFileName().equals("."))
                        .filter(entry -> !entry.getFileName().equals(".."))
                        .map(info -> getReadOnlyFile(share, info.getFileName()))
                        .map(SmbFileOperations::mapDiskEntryToSmbFile)
                        .collect(Collectors.toList());
        } catch (Exception e) {
            throw new GenericFileOperationFailedException(String.format("Could not list files for path: '%s'", path), e);
        }
    }

    private Connection getConnection() {
        return ConnectionCache.getConnectionAndCreateIfNecessary(client, endpoint.getId(), host, port);
    }

    private Session getSession() {
        if (smbConfiguration.isNtlmAuthentication()) {
            return getConnection().authenticate(smbConfiguration.createAuthenticationContext());
        }
        throw new IllegalStateException("For now only NTLM authentication is supported");
    }

    public DiskShare openShare() {
        return (DiskShare) getSession().connectShare(smbConfiguration.getShare());
    }

    private DiskEntry getReadOnlyFile(final DiskShare share,
                                      final String name) {
        return share.open(name,
                          EnumSet.of(AccessMask.GENERIC_READ),
                          null,
                          EnumSet.of(SMB2ShareAccess.FILE_SHARE_READ),
                          SMB2CreateDisposition.FILE_OPEN,
                          null);
    }

    private DiskEntry getWritableFile(final DiskShare share,
                                      final String name) {
        return share.open(name,
                          EnumSet.of(AccessMask.FILE_WRITE_DATA),
                          null,
                          EnumSet.of(SMB2ShareAccess.FILE_SHARE_WRITE),
                          SMB2CreateDisposition.FILE_SUPERSEDE,
                          null);
    }

    /**
     * Copied from FileOperations from camel-file
     * Moves any existing file due fileExists=Move is in use.
     */
    private void doMoveExistingFile(String fileName) throws GenericFileOperationFailedException {
        // need to evaluate using a dummy and simulate the file first, to have access to all the file attributes
        // create a dummy exchange as Exchange is needed for expression evaluation
        // we support only the following 3 tokens.
        Exchange dummy = endpoint.createExchange();
        String parent = FileUtil.onlyPath(fileName);
        String onlyName = FileUtil.stripPath(fileName);
        dummy.getIn().setHeader(Exchange.FILE_NAME, fileName);
        dummy.getIn().setHeader(Exchange.FILE_NAME_ONLY, onlyName);
        dummy.getIn().setHeader(Exchange.FILE_PARENT, parent);

        String to = endpoint.getMoveExisting().evaluate(dummy, String.class);
        // we must normalize it (to avoid having both \ and / in the name which confuses java.io.File)
        to = FileUtil.normalizePath(to);
        if (ObjectHelper.isEmpty(to)) {
            throw new GenericFileOperationFailedException("moveExisting evaluated as empty String, cannot move existing file: " + fileName);
        }

        // ensure any paths is created before we rename as the renamed file may be in a different path (which may be non exiting)
        // use java.io.File to compute the file path
        File toFile = new File(to);
        String directory = toFile.getParent();
        boolean absolute = FileUtil.isAbsolute(toFile);
        if (directory != null) {
            if (!buildDirectory(directory, absolute)) {
                LOG.debug("Cannot build directory [{}] (could be because of denied permissions)", directory);
            }
        }

        // deal if there already exists a file
        if (existsFile(to)) {
            if (endpoint.isEagerDeleteTargetFile()) {
                LOG.trace("Deleting existing file: {}", to);
                if (!deleteFile(to)) {
                    throw new GenericFileOperationFailedException("Cannot delete file: " + to);
                }
            } else {
                throw new GenericFileOperationFailedException("Cannot moved existing file from: " + fileName + " to: " + to + " as there already exists a file: " + to);
            }
        }

        LOG.trace("Moving existing file: {} to: {}", fileName, to);
        if (!renameFile(fileName, to)) {
            throw new GenericFileOperationFailedException("Cannot rename file from: " + fileName + " to: " + to);
        }
    }

    private String getPath(String pathEnd) {
        String path = smbConfiguration.getHost() + pathEnd;
        return path.replace('\\', '/');
    }

    public SMBClient getClient() {
        return client;
    }
}
