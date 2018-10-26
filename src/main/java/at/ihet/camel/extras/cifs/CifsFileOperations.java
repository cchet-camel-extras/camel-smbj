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


import at.ihet.camel.extras.cifs.util.ConnectionCache;
import com.hierynomus.msdtyp.AccessMask;
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
import java.util.Date;
import java.util.EnumSet;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * This class is the implementation of the interface {@link GenericFileOperations} for a CIFS share accessed by smbj.
 *
 * @author Thomas Herzog <herzog.thomas81@gmail.com>
 * @since 10/26/2018
 */
public class CifsFileOperations implements GenericFileOperations<DiskEntry> {

    private static final Logger LOG = LoggerFactory.getLogger(CifsFileOperations.class);
    private final SMBClient client;
    private CifsConfiguration cifsConfiguration;
    private String host;
    private Integer port;
    private GenericFileEndpoint<DiskEntry> endpoint;

    public CifsFileOperations(final SMBClient client) {
        this.client = Objects.requireNonNull(client, "Cannot perform file operations with a null client");
    }

    @Override
    public void setEndpoint(GenericFileEndpoint<DiskEntry> endpoint) {
        this.endpoint = Objects.requireNonNull(endpoint, "Endpoint must not be null");
        this.cifsConfiguration = Objects.requireNonNull(cifsConfiguration, "Cannot perform file operations with a null cifs configuration");
        this.host = Objects.requireNonNull(cifsConfiguration.getHost(), "Host ist required for cifs connection");
        this.port = cifsConfiguration.getPort();
    }

    @Override
    public boolean deleteFile(String name) throws GenericFileOperationFailedException {
        try {
            try (final DiskShare share = openShare()) {
                share.rm(name);
                return true;
            }
        } catch (IOException e) {
            throw new GenericFileOperationFailedException(String.format("Could not delete file '%s'", name));
        }
    }

    @Override
    public boolean existsFile(String name) throws GenericFileOperationFailedException {
        try {
            try (final DiskShare share = openShare()) {
                return share.fileExists(name);
            }
        } catch (IOException e) {
            throw new GenericFileOperationFailedException(String.format("Could not delete file '%s'", name));
        }
    }

    @Override
    public boolean renameFile(String from,
                              String to) throws GenericFileOperationFailedException {
        try {
            try (final DiskShare share = openShare()) {
                getReadOnlyFile(share, from).rename(to);
                return true;
            }
        } catch (IOException e) {
            throw new GenericFileOperationFailedException(String.format("Could not rename file from '%s' to '%s'", from, to), e);
        } catch (SMBApiException e) {
            LOG.debug("renameFile failed with exception", e);
            return false;
        }
    }

    @Override
    public boolean buildDirectory(String directory,
                                  boolean absolute) throws GenericFileOperationFailedException {
        try {
            try (final DiskShare share = openShare()) {
                share.mkdir(cifsConfiguration.getDirectory() + File.separator + directory);
                return true;
            }
        } catch (IOException e) {
            LOG.debug("buildDirectory failed with exception", e);
            return false;
        }
    }

    @Override
    public boolean retrieveFile(String name,
                                Exchange exchange,
                                long size) throws GenericFileOperationFailedException {
        try {
            try (final DiskShare share = openShare()) {
                exchange.getIn().setBody(getReadOnlyFile(share, name).getInputStream());
                return true;
            }
        } catch (IOException e) {
            throw new GenericFileOperationFailedException(String.format("Could not retrieve file '%s'", name), e);
        } catch (SMBApiException e) {
            LOG.debug("retrieveFile failed with exception", e);
            return false;
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
            try (BufferedInputStream bis = new BufferedInputStream(exchange.getMessage().getMandatoryBody(InputStream.class))) {
                try (final DiskShare share = openShare()) {
                    com.hierynomus.smbj.share.File file = getWritableFile(share, name);
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
            }
        } catch (IOException | InvalidPayloadException e) {
            throw new GenericFileOperationFailedException(String.format("Could not retrieve file '%s'", name), e);
        } catch (SMBApiException e) {
            LOG.debug("retrieveFile failed with exception", e);
            return false;
        }
    }

    @Override
    public String getCurrentDirectory() throws GenericFileOperationFailedException {
        throw new UnsupportedOperationException("Cannot get current directory on a cifs share");
    }

    @Override
    public void changeCurrentDirectory(String path) throws GenericFileOperationFailedException {
        throw new UnsupportedOperationException("Cannot switch to a directory on a cifs share");
    }

    @Override
    public void changeToParentDirectory() throws GenericFileOperationFailedException {
        throw new UnsupportedOperationException("Cannot switch to a directory on a cifs share");
    }

    @Override
    public List<DiskEntry> listFiles() throws GenericFileOperationFailedException {
        return listFiles("");
    }

    @Override
    public List<DiskEntry> listFiles(String path) throws GenericFileOperationFailedException {
        try (final DiskShare share = openShare()) {
            return share.list(path, FileNamesInformation.class).stream().map(info -> getReadOnlyFile(share, info.getFileName()))
                        .collect(Collectors.toList());
        } catch (Exception e) {
            throw new GenericFileOperationFailedException(String.format("Could not list files for path: '%s'", path), e);
        }
    }

    private Connection getConnection() {
        return ConnectionCache.getOrCreateConnection(client, host, port);
    }

    private Session getSession() {
        if (cifsConfiguration.isNtlmAuthentication()) {
            return getConnection().authenticate(cifsConfiguration.createAuthenticationContext());
        }
        throw new IllegalStateException("For now only NTLM authentication is supported");
    }

    public DiskShare openShare() {
        return (DiskShare) getSession().connectShare(cifsConfiguration.getDirectory());
    }

    private com.hierynomus.smbj.share.File getReadOnlyFile(final DiskShare share,
                                                           final String name) {
        return share.openFile(name,
                              EnumSet.of(AccessMask.GENERIC_READ),
                              null,
                              EnumSet.of(SMB2ShareAccess.FILE_SHARE_READ),
                              SMB2CreateDisposition.FILE_OPEN,
                              null);
    }

    private com.hierynomus.smbj.share.File getWritableFile(final DiskShare share,
                                                           final String name) {
        return share.openFile(name,
                              EnumSet.of(AccessMask.FILE_WRITE_DATA),
                              null,
                              EnumSet.of(SMB2ShareAccess.FILE_SHARE_WRITE),
                              SMB2CreateDisposition.FILE_CREATE,
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
        String path = cifsConfiguration.getHost() + pathEnd;
        return path.replace('\\', '/');
    }

    private Long lastModifiedDate(Exchange exchange) {
        Long last = null;
        if (endpoint.isKeepLastModified()) {
            Date date = exchange.getIn().getHeader(Exchange.FILE_LAST_MODIFIED, Date.class);
            if (date != null) {
                last = date.getTime();
            } else {
                // fallback and try a long
                last = exchange.getIn().getHeader(Exchange.FILE_LAST_MODIFIED, Long.class);
            }
        }
        return last;
    }

}
