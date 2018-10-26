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


import com.hierynomus.msdtyp.AccessMask;
import com.hierynomus.mssmb2.SMB2CreateDisposition;
import com.hierynomus.mssmb2.SMB2ShareAccess;
import com.hierynomus.mssmb2.SMBApiException;
import com.hierynomus.protocol.commons.buffer.Buffer;
import com.hierynomus.smbj.SMBClient;
import com.hierynomus.smbj.connection.Connection;
import com.hierynomus.smbj.session.Session;
import com.hierynomus.smbj.share.DiskEntry;
import com.hierynomus.smbj.share.DiskShare;
import org.apache.camel.Exchange;
import org.apache.camel.component.file.GenericFileEndpoint;
import org.apache.camel.component.file.GenericFileOperationFailedException;
import org.apache.camel.component.file.GenericFileOperations;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.EnumSet;
import java.util.List;
import java.util.Objects;

/**
 * @author Thomas Herzog <herzog.thomas81@gmail.com>
 * @since 10/26/2018
 */
public class CifsFileOperations implements GenericFileOperations<DiskEntry> {

    private static final Logger LOG = LoggerFactory.getLogger(CifsFileOperations.class);
    private final SMBClient client;
    private final CifsConfiguration cifsConfiguration;
    private GenericFileEndpoint<DiskEntry> endpoint;

    public CifsFileOperations(final SMBClient client,
                              final CifsConfiguration cifsConfiguration) {
        this.client = Objects.requireNonNull(client, "Cannot perform file operations with a null client");
        this.cifsConfiguration = Objects.requireNonNull(cifsConfiguration, "Cannot perform file operations with a null cifs configuration");
    }

    @Override
    public void setEndpoint(GenericFileEndpoint<DiskEntry> endpoint) {
        this.endpoint = endpoint;
    }

    @Override
    public boolean deleteFile(String name) throws GenericFileOperationFailedException {
        try (final Connection connection = client.connect(cifsConfiguration.getHost(), cifsConfiguration.getPort())) {
            final Session session = authenticate(connection);
            try (final DiskShare share = (DiskShare) session.connectShare(cifsConfiguration.getDirectory())) {
                share.rm(name);
                return true;
            }
        } catch (IOException e) {
            throw new GenericFileOperationFailedException(String.format("Could not delete file '%s'", name));
        }
    }

    @Override
    public boolean existsFile(String name) throws GenericFileOperationFailedException {
        try (final Connection connection = client.connect(cifsConfiguration.getHost(), cifsConfiguration.getPort())) {
            final Session session = authenticate(connection);
            try (final DiskShare share = (DiskShare) session.connectShare(cifsConfiguration.getDirectory())) {
                return share.fileExists(name);
            }
        } catch (IOException e) {
            throw new GenericFileOperationFailedException(String.format("Could not delete file '%s'", name));
        }
    }

    @Override
    public boolean renameFile(String from,
                              String to) throws GenericFileOperationFailedException {
        try (final Connection connection = client.connect(cifsConfiguration.getHost(), cifsConfiguration.getPort())) {
            final Session session = authenticate(connection);
            try (final DiskShare share = (DiskShare) session.connectShare(cifsConfiguration.getDirectory())) {
                com.hierynomus.smbj.share.File srcFile = share.openFile(from,
                                                                        EnumSet.of(AccessMask.GENERIC_READ),
                                                                        null,
                                                                        EnumSet.of(SMB2ShareAccess.FILE_SHARE_READ),
                                                                        SMB2CreateDisposition.FILE_OPEN,
                                                                        null);
                com.hierynomus.smbj.share.File destFile = share.openFile(from,
                                                                         EnumSet.of(AccessMask.FILE_WRITE_DATA),
                                                                         null,
                                                                         EnumSet.of(SMB2ShareAccess.FILE_SHARE_WRITE),
                                                                         SMB2CreateDisposition.FILE_OVERWRITE_IF,
                                                                         null);
                srcFile.remoteCopyTo(destFile);
                return true;
            }
        } catch (IOException e) {
            throw new GenericFileOperationFailedException(String.format("Could not rename file from '%s' to '%s'", from, to));
        } catch (SMBApiException e) {
            LOG.debug("renameFile failed with exception", e);
            return false;
        } catch (Buffer.BufferException e) {
            throw new GenericFileOperationFailedException(String.format("Could not rename file from '%s' to '%s'", from, to));
        }
    }

    @Override
    public boolean buildDirectory(String directory,
                                  boolean absolute) throws GenericFileOperationFailedException {
        try (final Connection connection = client.connect(cifsConfiguration.getHost(), cifsConfiguration.getPort())) {
            final Session session = authenticate(connection);
            try (final DiskShare share = (DiskShare) session.connectShare(cifsConfiguration.getDirectory())) {
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
        try (final Connection connection = client.connect(cifsConfiguration.getHost(), cifsConfiguration.getPort())) {
            final Session session = authenticate(connection);
            try (final DiskShare share = (DiskShare) session.connectShare(cifsConfiguration.getDirectory())) {
                com.hierynomus.smbj.share.File file = share.openFile(name,
                                                                     EnumSet.of(AccessMask.GENERIC_READ),
                                                                     null,
                                                                     EnumSet.of(SMB2ShareAccess.FILE_SHARE_READ),
                                                                     SMB2CreateDisposition.FILE_OPEN,
                                                                     null);
                exchange.getIn().setBody(file.getInputStream());
                return true;
            }
        } catch (IOException e) {
            throw new GenericFileOperationFailedException(String.format("Could not retrieve file '%s'", name));
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
        try (final Connection connection = client.connect(cifsConfiguration.getHost(), cifsConfiguration.getPort())) {
            final Session session = authenticate(connection);
            try (final DiskShare share = (DiskShare) session.connectShare(cifsConfiguration.getDirectory())) {
                com.hierynomus.smbj.share.File file = share.openFile(name,
                                                                     EnumSet.of(AccessMask.FILE_WRITE_DATA),
                                                                     null,
                                                                     EnumSet.of(SMB2ShareAccess.FILE_SHARE_WRITE),
                                                                     SMB2CreateDisposition.FILE_CREATE,
                                                                     null);
                // TODO: Write file. Take care about move options as well
                return true;
            }
        } catch (IOException e) {
            throw new GenericFileOperationFailedException(String.format("Could not retrieve file '%s'", name));
        } catch (SMBApiException e) {
            LOG.debug("retrieveFile failed with exception", e);
            return false;
        }
    }

    @Override
    public String getCurrentDirectory() throws GenericFileOperationFailedException {
        return null;
    }

    @Override
    public void changeCurrentDirectory(String path) throws GenericFileOperationFailedException {

    }

    @Override
    public void changeToParentDirectory() throws GenericFileOperationFailedException {

    }

    @Override
    public List<DiskEntry> listFiles() throws GenericFileOperationFailedException {
        return null;
    }

    @Override
    public List<DiskEntry> listFiles(String path) throws GenericFileOperationFailedException {
        return null;
    }

    private Session authenticate(final Connection connection) {
        if (cifsConfiguration.isNtlmAuthentication()) {
            return connection.authenticate(cifsConfiguration.createAuthenticationContext());
        }
        throw new IllegalStateException("For now only NTLM authentication is supported");
    }
}
