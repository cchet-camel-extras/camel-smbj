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

import com.hierynomus.mssmb2.SMB2Dialect;
import com.hierynomus.smbj.SmbConfig;
import com.hierynomus.smbj.auth.AuthenticationContext;
import org.apache.camel.component.file.GenericFileConfiguration;
import org.apache.camel.spi.UriParam;

import java.net.URI;
import java.util.*;
import java.util.concurrent.TimeUnit;

/**
 * This class represents the configuration for smbj.
 *
 * @author Thomas Herzog <herzog.thomas81@gmail.com>
 * @since 10/26/2018
 */
public class SmbConfiguration extends GenericFileConfiguration {

    private final SmbConfig.Builder builder;

    @UriParam(name = "domain", defaultValue = "", defaultValueNote = "No domain is assumed as default", description = "The domain for the authentication", javaType = "java.lang.String")
    private String domain = "";
    @UriParam(name = "username", description = "The username for the authentication", javaType = "java.lang.String")
    private String username;
    @UriParam(name = "password", description = "The password for the authentication", javaType = "java.lang.String", secret = true)
    private String password;
    @UriParam(name = "versions", defaultValue = "unkown", defaultValueNote = "Client will determine what dialect to use", description = "The comma separated list of versions to use [unkown|2_0_2|2_1|2xx]", javaType = "java.lang.String")
    private String versions = "unkown";
    @UriParam(name = "authType", defaultValue = "ntlm", defaultValueNote = "USe NTLM authentication as default", description = "The authentication type to use default: ntlm or spnego", javaType = "java.lang.String")
    private String authType = "ntlm";
    @UriParam(name = "dfs", defaultValue = "false", defaultValueNote = "Assuming that no DFS services are available", description = "True if DFS services are available, false otherwise", javaType = "java.lang.Boolean")
    private Boolean dfs = false;
    @UriParam(name = "multiProtocol", defaultValue = "false", defaultValueNote = "Assuming that no multiple protocols are supported", description = "True if multi protocols are available, false otherwise", javaType = "java.lang.Boolean")
    private Boolean multiProtocol = false;
    @UriParam(name = "signing", defaultValue = "false", defaultValueNote = "Assuming that no sign in is required", description = "True if sign in is required, false otherwise", javaType = "java.lang.Boolean")
    private Boolean signing = false;
    @UriParam(name = "uuid", description = "The uuid to use for the client", javaType = "java.lang.String")
    private String uuid;
    @UriParam(name = "bufferSize", defaultValue = "1048576", defaultValueNote = "1048576 bytes default buffer size for read/write", description = "The read/write buffer size in bytes to use", javaType = "java.lang.Integer")
    private Integer bufferSize = 1048576;
    @UriParam(name = "readBufferSize", description = "The read buffer size to use", javaType = "java.lang.Integer")
    private Integer readBufferSize;
    @UriParam(name = "writeBufferSize", description = "The write buffer size to use", javaType = "java.lang.Integer")
    private Integer writeBufferSize;
    @UriParam(name = "timeout", defaultValue = "60000", defaultValueNote = "Default read/write timeout is 60000ms", description = "The read/write timeout in milliseconds", javaType = "java.lang.Integer")
    private Integer timeout = 60000;
    @UriParam(name = "readTimeout", description = "The read timeout in milliseconds", javaType = "java.lang.Integer")
    private Integer readTimeout;
    @UriParam(name = "writeTimeout", description = "The write timeout in milliseconds", javaType = "java.lang.Integer")
    private Integer writeTimeout;
    @UriParam(name = "socketTimeout", defaultValue = "60000", defaultValueNote = "Default socket timeout is 60000ms", description = "The socket timeout in milliseconds", javaType = "java.lang.Integer")
    private Integer socketTimeout = 60000;
    @UriParam(name = "transactTimeout", defaultValue = "60000", defaultValueNote = "Default transaction timeout is 60000ms", description = "The transaction timeout in milliseconds", javaType = "java.lang.Integer")
    private Integer transactTimeout = 60000;

    public SmbConfiguration(final URI uri) {
        this.builder = SmbConfig.builder();
        configure(uri);
        setDirectory("");
    }

    public SmbConfig getSmbConfig() {
        builder.withDialects(resolveSmbDialectsFromVersions())
               .withDfsEnabled(dfs)
               .withMultiProtocolNegotiate(multiProtocol)
               .withRandomProvider(new Random(System.currentTimeMillis()))
               .withSigningRequired(signing)
               .withReadBufferSize(Optional.ofNullable(readBufferSize).orElse(bufferSize))
               .withWriteBufferSize(Optional.ofNullable(writeBufferSize).orElse(bufferSize))
               .withReadTimeout(Optional.ofNullable(readTimeout).orElse(timeout), TimeUnit.MILLISECONDS)
               .withWriteTimeout(Optional.ofNullable(writeTimeout).orElse(timeout), TimeUnit.MILLISECONDS)
               .withTransactTimeout(transactTimeout, TimeUnit.MILLISECONDS)
               .withSoTimeout(socketTimeout);

        if (uuid != null) {
            builder.withClientGuid(UUID.fromString(uuid));
        }
        return builder.build();
    }

    public AuthenticationContext createAuthenticationContext() {
        if (!"ntlm".equalsIgnoreCase(authType)) {
            throw new IllegalStateException(String.format("Cannot create AuthenticationContext for currently selected authType: '%s'", authType));
        }
        return new AuthenticationContext(Optional.ofNullable(username).orElseThrow(() -> new IllegalArgumentException("Username cannot be null when using NTLM authentication")),
                                         Optional.ofNullable(password).orElse("").toCharArray(),
                                         Optional.ofNullable(domain).orElse(""));
    }

    public boolean isNtlmAuthentication() {
        return authType.equalsIgnoreCase("ntlm");
    }

    private SMB2Dialect[] resolveSmbDialectsFromVersions() {
        final String[] versionArr = versions.split(",");
        final List<SMB2Dialect> dialects = new LinkedList<>();
        for (final String version : versionArr) {
            final String normalizedVersion = version.trim().toUpperCase();
            if (!normalizedVersion.isEmpty()) {
                if (version.equalsIgnoreCase(SMB2Dialect.UNKNOWN.name())) {
                    dialects.add(SMB2Dialect.UNKNOWN);
                } else {
                    dialects.add(SMB2Dialect.valueOf(String.format("SMB_%s", version.trim().toUpperCase())));
                }
            }
        }
        return dialects.toArray(new SMB2Dialect[dialects.size()]);
    }

    //<editor-fold desc="Getter and Setter">
    public String getDomain() {
        return domain;
    }

    public void setDomain(String domain) {
        this.domain = domain;
    }

    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public String getVersions() {
        return versions;
    }

    public void setVersions(String versions) {
        this.versions = versions;
    }

    public String getAuthType() {
        return authType;
    }

    public void setAuthType(String authType) {
        this.authType = authType;
    }

    public Boolean getDfs() {
        return dfs;
    }

    public void setDfs(Boolean dfs) {
        this.dfs = dfs;
    }

    public Boolean getMultiProtocol() {
        return multiProtocol;
    }

    public void setMultiProtocol(Boolean multiProtocol) {
        this.multiProtocol = multiProtocol;
    }

    public Boolean getSigning() {
        return signing;
    }

    public void setSigning(Boolean signing) {
        this.signing = signing;
    }

    public String getUuid() {
        return uuid;
    }

    public void setUuid(String uuid) {
        this.uuid = uuid;
    }

    public Integer getBufferSize() {
        return bufferSize;
    }

    public void setBufferSize(Integer bufferSize) {
        this.bufferSize = bufferSize;
    }

    public Integer getReadBufferSize() {
        return readBufferSize;
    }

    public void setReadBufferSize(Integer readBufferSize) {
        this.readBufferSize = readBufferSize;
    }

    public Integer getWriteBufferSize() {
        return writeBufferSize;
    }

    public void setWriteBufferSize(Integer writeBufferSize) {
        this.writeBufferSize = writeBufferSize;
    }

    public Integer getTimeout() {
        return timeout;
    }

    public void setTimeout(Integer timeout) {
        this.timeout = timeout;
    }

    public Integer getReadTimeout() {
        return readTimeout;
    }

    public void setReadTimeout(Integer readTimeout) {
        this.readTimeout = readTimeout;
    }

    public Integer getWriteTimeout() {
        return writeTimeout;
    }

    public void setWriteTimeout(Integer writeTimeout) {
        this.writeTimeout = writeTimeout;
    }

    public Integer getSocketTimeout() {
        return socketTimeout;
    }

    public void setSocketTimeout(Integer socketTimeout) {
        this.socketTimeout = socketTimeout;
    }

    public Integer getTransactTimeout() {
        return transactTimeout;
    }

    public void setTransactTimeout(Integer transactTimeout) {
        this.transactTimeout = transactTimeout;
    }
    //</editor-fold>
}
