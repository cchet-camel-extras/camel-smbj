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
import com.hierynomus.msfscc.fileinformation.FileAllInformation;
import com.hierynomus.msfscc.fileinformation.FileIdBothDirectoryInformation;
import com.hierynomus.mssmb2.SMB2CreateDisposition;
import com.hierynomus.mssmb2.SMB2ShareAccess;
import com.hierynomus.smbj.SMBClient;
import com.hierynomus.smbj.connection.Connection;
import com.hierynomus.smbj.session.Session;
import com.hierynomus.smbj.share.DiskEntry;
import com.hierynomus.smbj.share.DiskShare;
import org.apache.camel.Exchange;
import org.apache.camel.component.file.GenericFileEndpoint;
import org.apache.camel.component.file.GenericFileExist;
import org.apache.camel.component.file.GenericFileOperationFailedException;
import org.apache.camel.component.file.GenericFileOperations;
import org.apache.camel.util.FileUtil;
import org.apache.camel.util.ObjectHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.regex.Matcher;
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
    private SmbEndpoint endpoint;

    /**
     * @param client the configured smb client of smbj, which is used to access files on the smb share.
     */
    public SmbFileOperations(final SMBClient client) {
        this.client = Objects.requireNonNull(client, "Cannot perform file operations with a null client");
    }

    /**
     * Normalizes the file name or path by removing the leading slash or backslash, because smbj appends a leading slash itself
     *
     * @param name the name to normalize
     * @return the normalized file name or path
     */
    private static String normalizeFileNameOrPath(final String name) {
        if (name.startsWith("\\")) {
            return name.replaceFirst(Matcher.quoteReplacement("\\"), "");
        }
        return name;
    }

    /**
     * Downloads the file to a new input stream which is located in memory.
     *
     * @param is       the input stream of the file to download
     * @param exchange the exchange where to set the input stream of the downloaded file in the in.body
     */
    private static void downloadFileToMemoryAndCreateInputStream(final InputStream is,
                                                                 final Exchange exchange) {
        try (final ByteArrayOutputStream bos = new ByteArrayOutputStream()) {
            byte[] buffer = new byte[4096];
            int len;
            while ((len = is.read(buffer)) > -1) {
                bos.write(buffer, 0, len);
            }
            bos.flush();

            exchange.getIn().setBody(new BufferedInputStream(new ByteArrayInputStream(bos.toByteArray())));
        } catch (Exception e) {
            throw new GenericFileOperationFailedException("Could not download file: '%s'", e);
        }
    }

    @Override
    public void setEndpoint(GenericFileEndpoint<SmbFile> endpoint) {
        this.endpoint = (SmbEndpoint) Objects.requireNonNull(endpoint, "Endpoint must not be null");
        this.smbConfiguration = (SmbConfiguration) Objects.requireNonNull(endpoint.getConfiguration(), "Cannot perform file operations with a null smb configuration");
    }

    @Override
    public boolean deleteFile(final String name) throws GenericFileOperationFailedException {
        final String normalizedName = normalizeFileNameOrPath(name);
        try {
            return invokeOnDiskShare(share -> {
                if (share.fileExists(normalizedName)) {
                    share.rm(normalizedName);
                    return true;
                }
                return false;
            });
        } catch (Exception e) {
            throw new GenericFileOperationFailedException(String.format("Could not delete file '%s'", normalizedName));
        }
    }

    @Override
    public boolean existsFile(final String name) throws GenericFileOperationFailedException {
        final String normalizedName = normalizeFileNameOrPath(name);
        try {
            return invokeOnDiskShare(share -> share.fileExists(normalizedName));
        } catch (Exception e) {
            throw new GenericFileOperationFailedException(String.format("Could not delete file '%s'", normalizedName));
        }
    }

    @Override
    public boolean renameFile(final String from,
                              final String to) throws GenericFileOperationFailedException {
        final String normalizedFrom = normalizeFileNameOrPath(from);
        final String normalizedTo = normalizeFileNameOrPath(to);
        try {
            return invokeOnDiskShare(share -> {
                if (share.fileExists(normalizedFrom)) {
                    openWritableFile(share, normalizedFrom).rename(normalizedTo, true);
                    return true;
                }
                return false;
            });
        } catch (Exception e) {
            throw new GenericFileOperationFailedException(String.format("Could not rename file from '%s' to '%s'", normalizedFrom, normalizedTo), e);
        }
    }

    /**
     * Downloads the file represented by the input stream to a local directory and opens a stream to the new file and sets it on the exchange object.
     *
     * @param tmpDirectory the tmp directory where to store the file
     * @param name         the name of the file to download
     * @param is           the input stream of the file to download
     * @param exchange     the exchange where to set the input stream of the downloaded file in the in.body
     * @throws GenericFileOperationFailedException if the download fails
     */
    private static void downloadFileToTmpAndCreateInputStream(final String tmpDirectory,
                                                              final String name,
                                                              final InputStream is,
                                                              final Exchange exchange) {
        try {
            Path tmpFile = Paths.get(tmpDirectory, name);
            Files.createDirectories(tmpFile.getParent());
            Files.deleteIfExists(tmpFile);
            Files.copy(is, tmpFile);
            is.close();

            exchange.getIn().setHeader(Exchange.FILE_LOCAL_WORK_PATH, tmpFile.toFile());
            exchange.getIn().setBody(new BufferedInputStream(Files.newInputStream(tmpFile)));
        } catch (Exception e) {
            throw new GenericFileOperationFailedException("Could not download to temporary file", e);
        }
    }

    /**
     * Maps the file information to the smb file model
     *
     * @param path the path he file has been listed from
     * @param info the file info of the listed file
     * @return the mapped smb file model object
     * @see SmbFile
     * @see FileIdBothDirectoryInformation
     */
    private SmbFile mapFileInformationToSmbFile(final String path,
                                                final FileIdBothDirectoryInformation info) {
        final long attributes = info.getFileAttributes();
        final boolean directory = SmbFileAttributeUtils.isDirectory(attributes);
        final String pathPrefix = (path.isEmpty()) ? path : (path + "\\");
        final long fileSize;
        if (directory) {
            fileSize = 0;
        } else {
            fileSize = info.getEndOfFile();
        }
        return new SmbFile(directory,
                           SmbFileAttributeUtils.isArchive(attributes),
                           SmbFileAttributeUtils.isHidden(attributes),
                           SmbFileAttributeUtils.isReadOnly(attributes),
                           SmbFileAttributeUtils.isSystem(attributes),
                           normalizeFileNameOrPath(pathPrefix + info.getFileName()),
                           fileSize,
                           info.getChangeTime().toEpochMillis());
    }

    /**
     * Maps the file information to the smb file model
     *
     * @param path the path he file has been listed from
     * @param info the file info of the listed file
     * @return the mapped smb file model object
     * @see SmbFile
     * @see FileAllInformation
     */
    private SmbFile mapFileInformationToSmbFile(final String path,
                                                final FileAllInformation info) {
        final long attributes = info.getBasicInformation().getFileAttributes();
        final boolean directory = SmbFileAttributeUtils.isDirectory(attributes);
        final String pathPrefix = (path.isEmpty()) ? path : (path + "\\");
        final long fileSize;
        if (directory) {
            fileSize = 0;
        } else {
            fileSize = info.getStandardInformation().getEndOfFile();
        }
        return new SmbFile(directory,
                           SmbFileAttributeUtils.isArchive(attributes),
                           SmbFileAttributeUtils.isHidden(attributes),
                           SmbFileAttributeUtils.isReadOnly(attributes),
                           SmbFileAttributeUtils.isSystem(attributes),
                           normalizeFileNameOrPath(pathPrefix + info.getNameInformation()),
                           fileSize,
                           info.getBasicInformation().getChangeTime().toEpochMillis());
    }

    @Override
    public void releaseRetrievedFileResources(Exchange exchange) throws GenericFileOperationFailedException {
        // INFO: Nothing to do
    }

    @Override
    public boolean storeFile(final String name,
                             final Exchange exchange,
                             final long size) throws GenericFileOperationFailedException {
        boolean append = false;
        final String normalizedName = normalizeFileNameOrPath(name);

        if (existsFile(normalizedName)) {
            if (endpoint.getFileExist() == GenericFileExist.Ignore) {
                // ignore but indicate that the file was written
                LOG.debug(String.format("An existing file already exists: '%s'. Ignore and do not override it.", normalizedName));
                return false;
            } else if (endpoint.getFileExist() == GenericFileExist.Fail) {
                throw new GenericFileOperationFailedException(String.format("File already exist: '%s'. Cannot write new file.", normalizedName));
            } else if (endpoint.getFileExist() == GenericFileExist.Move) {
                // move any existing file first
                doMoveExistingFile(normalizedName);
            } else if (endpoint.isEagerDeleteTargetFile() && endpoint.getFileExist() == GenericFileExist.Override) {
                // we override the target so we do this by deleting it so the
                // temp file can be renamed later
                // with success as the existing target file have been deleted
                LOG.debug("Eagerly deleting existing file: " + normalizedName);
                if (!deleteFile(normalizedName)) {
                    throw new GenericFileOperationFailedException(String.format("Cannot delete file: '%s'", normalizedName));
                }
            } else if (endpoint.getFileExist() == GenericFileExist.Append) {
                append = true;
            }
        }

        final boolean appendContent = append;

        try {
            return invokeOnDiskShare(share -> {
                final DiskEntry entry = openWritableFile(share, normalizedName);
                if (!entry.getFileInformation().getStandardInformation().isDirectory()) {
                    final com.hierynomus.smbj.share.File file = (com.hierynomus.smbj.share.File) entry;
                    try (BufferedInputStream bis = new BufferedInputStream(exchange.getMessage().getMandatoryBody(InputStream.class))) {
                        try (final BufferedOutputStream bos = new BufferedOutputStream(file.getOutputStream(appendContent))) {
                            byte[] data = new byte[512 * 1024];
                            int dataSize;
                            while ((dataSize = bis.read(data)) != -1) {
                                bos.write(data, 0, dataSize);
                            }
                            bos.flush();
                            file.rename(normalizedName);
                            return true;
                        }
                    } catch (Exception e) {
                        throw e;
                    }
                } else {
                    throw new GenericFileOperationFailedException(String.format("Could not store file, because it is a directory '%s'", normalizedName));
                }
            });
        } catch (Exception e) {
            throw new GenericFileOperationFailedException(String.format("Could not store file '%s'", normalizedName), e);
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
    public List<SmbFile> listFiles(final String path) throws GenericFileOperationFailedException {
        try {
            return invokeOnDiskShare(share -> {
                if (!share.fileExists(path) && !share.folderExists(path)) {
                    return Collections.emptyList();
                }
                // Lock strategy wants to list files with filename, which is not supported by smbj
                if (share.fileExists(path)) {
                    final FileAllInformation info = share.getFileInformation(path, FileAllInformation.class);
                    return Collections.singletonList(mapFileInformationToSmbFile(path, info));
                }

                return share.list(path).stream()
                            // Exclude Linux . and .. directories
                            .filter(entry -> !entry.getFileName().equals("."))
                            .filter(entry -> !entry.getFileName().equals(".."))
                            .map(info -> mapFileInformationToSmbFile(path, info))
                            .collect(Collectors.toList());
            });
        } catch (Exception e) {
            throw new GenericFileOperationFailedException(String.format("Could not list files for path: '%s'", path), e);
        }
    }

    /**
     * Creates a connection to the smb share
     *
     * @return the created connection
     */
    private Connection createConnection() {
        try {
            if (endpoint.getPort() == null) {
                return client.connect(endpoint.getHost());
            } else {
                return client.connect(endpoint.getHost(), endpoint.getPort());
            }
        } catch (IOException e) {
            throw new GenericFileOperationFailedException("Could not create connection", e);
        }
    }

    /**
     * Creates a session for the given connection and authenticates the user
     *
     * @param connection the connection for creating the session
     * @return the created session
     */
    private Session createSession(final Connection connection) {
        if (smbConfiguration.isNtlmAuthentication()) {
            return connection.authenticate(smbConfiguration.createAuthenticationContext());
        }
        throw new IllegalStateException("For now only NTLM authentication is supported");
    }

    /**
     * Creates a disk share for the given session
     *
     * @param session the session for creating the disk share
     * @return the created disk share
     */
    public DiskShare createShare(final Session session) {
        return (DiskShare) session.connectShare(endpoint.getShare());
    }

    /**
     * Opens a read only file from the smb share
     *
     * @param share the disk share to open the file from
     * @param name  the fully qualified file name to open
     * @return the opened file
     * @throws com.hierynomus.mssmb2.SMBApiException if the file cannot be opened
     */
    private DiskEntry openReadOnlyFile(final DiskShare share,
                                       final String name) {
        return share.openFile(name,
                              EnumSet.of(AccessMask.GENERIC_READ),
                              null,
                              EnumSet.of(SMB2ShareAccess.FILE_SHARE_READ),
                              SMB2CreateDisposition.FILE_OPEN,
                              null);
    }

    /**
     * Opens a writable file from the smb share
     *
     * @param share the disk share to get the file from
     * @param name  the fully qualified file name to open
     * @return the opened file
     * @throws com.hierynomus.mssmb2.SMBApiException if the file cannot be opened
     */
    private DiskEntry openWritableFile(final DiskShare share,
                                       final String name) {
        return share.openFile(name,
                              EnumSet.of(AccessMask.GENERIC_ALL),
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
                LOG.trace(String.format("Deleting existing file: %s", to));
                if (!deleteFile(to)) {
                    throw new GenericFileOperationFailedException(String.format("Cannot delete file: %s", to));
                }
            } else {
                throw new GenericFileOperationFailedException(String.format("Cannot moved existing file from: '%s' -> '%s' as there already exists a file with that name", fileName, to));
            }
        }

        LOG.trace("Moving existing file: {} to: {}", fileName, to);
        if (!renameFile(fileName, to)) {
            throw new GenericFileOperationFailedException(String.format("Cannot rename file from: '%s' -> '%s'", fileName, to));
        }
    }

    @Override
    public boolean buildDirectory(final String directory,
                                  final boolean absolute) throws GenericFileOperationFailedException {
        try {
            return invokeOnDiskShare(share -> {
                final String[] directories = directory.split(Matcher.quoteReplacement("\\"));
                String buildDirectory = "";
                for (final String part : directories) {
                    if (!part.isEmpty()) {
                        buildDirectory += (buildDirectory.isEmpty()) ? part : ("\\" + part);
                        if (!share.folderExists(buildDirectory)) {
                            share.mkdir(buildDirectory);
                        }
                    }
                }
                return true;
            });
        } catch (Exception e) {
            throw new GenericFileOperationFailedException(String.format("Could not create directory '%s'", directory), e);
        }
    }

    @Override
    public boolean retrieveFile(final String name,
                                final Exchange exchange,
                                final long size) throws GenericFileOperationFailedException {
        final String normalizedName = normalizeFileNameOrPath(name);
        try {
            return invokeOnDiskShare(share -> {
                if (share.fileExists(normalizedName)) {
                    final DiskEntry entry = openReadOnlyFile(share, normalizedName);
                    final com.hierynomus.smbj.share.File file = (com.hierynomus.smbj.share.File) entry;
                    // Download file to memory
                    if (Optional.ofNullable(endpoint.getLocalWorkDirectory()).orElse("").trim().isEmpty()) {
                        downloadFileToMemoryAndCreateInputStream(file.getInputStream(), exchange);
                    }
                    // Download file to temporary directory and return file
                    else {
                        final String actualTmpDir = endpoint.getLocalWorkDirectory() + File.separator + endpoint.getId();
                        downloadFileToTmpAndCreateInputStream(actualTmpDir, name, file.getInputStream(), exchange);
                    }
                    return true;
                }
                return false;
            });
        } catch (Exception e) {
            throw new GenericFileOperationFailedException(String.format("Could not retrieve file '%s'", normalizedName), e);
        }
    }

    /**
     * Invokes a function within an open disk share, to avoid boilerplate code .
     *
     * @param function the function to execute within the disk share
     * @param <T>      the return type of the function, defined by the provided function
     * @return the function result
     * @throws Exception if an error occurred during the execution
     */
    private <T> T invokeOnDiskShare(final SmbjDiskShareFunction<T> function) throws Exception {
        try (final Connection connection = createConnection()) {
            try (final Session session = createSession(connection)) {
                try (final DiskShare share = createShare(session)) {
                    return function.apply(share);
                }
            }
        }
    }

    /**
     * This interface is used to provide a function which is executed within an open disk share.
     *
     * @param <T> the return type of the function
     */
    @FunctionalInterface
    private interface SmbjDiskShareFunction<T> {
        T apply(DiskShare share) throws Exception;
    }
}
