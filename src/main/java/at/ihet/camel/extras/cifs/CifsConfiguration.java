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

import com.hierynomus.mssmb2.SMB2Dialect;
import com.hierynomus.smbj.SmbConfig;
import com.hierynomus.smbj.auth.AuthenticationContext;
import org.apache.camel.component.file.GenericFileConfiguration;
import org.apache.camel.spi.UriParam;
import org.apache.camel.util.ObjectHelper;

import java.net.URI;
import java.util.Optional;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

/**
 * @author Thomas Herzog <herzog.thomas81@gmail.com>
 * @since 10/26/2018
 */
public class CifsConfiguration extends GenericFileConfiguration {

    private static final String DOMAIN_SEPARATOR = ";";
    private static final String USER_PASS_SEPARATOR = ":";
    private SmbConfig.Builder builder;
    private String domain;
    private String host;
    private Integer port;

    @UriParam(name = "version", defaultValue = "unkown", defaultValueNote = "Client will determine what dialect to use", description = "The version to use [unkown|2_0_2|2_1|2xx|3_0|3_0_2|3_1_1]", javaType = "java.lang.String")
    private String version;
    @UriParam(name = "authType", defaultValue = "ntlm", defaultValueNote = "USe NTLM authentication as default", description = "The authentication type to use default: ntlm or spnego", javaType = "java.lang.String")
    private String authType;
    @UriParam(name = "username", description = "The username for the authentication", javaType = "java.lang.String")
    private String username;
    @UriParam(name = "password", description = "The password for the authentication", javaType = "java.lang.String", secret = true)
    private String password;
    @UriParam(name = "dfs", defaultValue = "false", defaultValueNote = "Assuming that no DFS services are available", description = "True if DFS services are available, false otherwise", javaType = "java.lang.Boolean")
    private Boolean dfs;
    @UriParam(name = "multiProtocol", defaultValue = "false", defaultValueNote = "Assuming that no multiple protocols are supported", description = "True if multi protocols are available, false otherwise", javaType = "java.lang.Boolean")
    private Boolean multiProtocol;
    @UriParam(name = "signing", defaultValue = "false", defaultValueNote = "Assuming that no sign in is required", description = "True if sign in is required, false otherwise", javaType = "java.lang.Boolean")
    private Boolean signing;
    @UriParam(name = "download", defaultValue = "false", defaultValueNote = "Per default files are not downloaded", description = "True if file download is intended, false otherwise", javaType = "java.lang.Boolean")
    private Boolean download;
    @UriParam(name = "shareName", description = "The shareName of the share", javaType = "java.lang.String")
    private String shareName;
    @UriParam(name = "uuid", description = "The uuid to use for the client", javaType = "java.lang.String")
    private String uuid;
    @UriParam(name = "bufferSize", defaultValue = "1048576", defaultValueNote = "1048576 bytes default buffer size for read/write", description = "The read/write buffer size in bytes to use", javaType = "java.lang.Integer")
    private Integer bufferSize;
    @UriParam(name = "readBufferSize", description = "The read buffer size to use", javaType = "java.lang.Integer")
    private Integer readBufferSize;
    @UriParam(name = "writeBufferSize", description = "The write buffer size to use", javaType = "java.lang.Integer")
    private Integer writeBufferSize;
    @UriParam(name = "timeout", defaultValue = "60000", defaultValueNote = "Default read/write timeout is 60000ms", description = "The read/write timeout in milliseconds", javaType = "java.lang.Integer")
    private Integer timeout;
    @UriParam(name = "readTimeout", description = "The read timeout in milliseconds", javaType = "java.lang.Integer")
    private Integer readTimeout;
    @UriParam(name = "writeTimeout", description = "The write timeout in milliseconds", javaType = "java.lang.Integer")
    private Integer writeTimeout;
    @UriParam(name = "socketTimeout", defaultValue = "60000", defaultValueNote = "Default socket timeout is 60000ms", description = "The socket timeout in milliseconds", javaType = "java.lang.Integer")
    private Integer socketTimeout;
    @UriParam(name = "transactTimeout", defaultValue = "60000", defaultValueNote = "Default transaction timeout is 60000ms", description = "The transaction timeout in milliseconds", javaType = "java.lang.Integer")
    private Integer transactTimeout;

    public CifsConfiguration(final URI uri) {
        configure(uri);
        this.builder = SmbConfig.builder();
    }

    public SmbConfig getSmbConfig() {
        builder = builder.withDialects(resolveSmbDialect())
                         .withDfsEnabled(dfs)
                         .withMultiProtocolNegotiate(multiProtocol)
                         .withRandomProvider(new Random(System.currentTimeMillis()))
                         .withSigningRequired(signing)
                         .withReadBufferSize(Optional.of(readBufferSize).orElse(bufferSize))
                         .withWriteBufferSize(Optional.of(writeBufferSize).orElse(bufferSize))
                         .withReadTimeout(Optional.of(readTimeout).orElse(timeout), TimeUnit.MILLISECONDS)
                         .withWriteTimeout(Optional.of(writeTimeout).orElse(timeout), TimeUnit.MILLISECONDS)
                         .withTransactTimeout(transactTimeout, TimeUnit.MILLISECONDS)
                         .withSoTimeout(socketTimeout);

        if (uuid == null) {
            builder.withClientGuid(UUID.fromString(uuid));
        }
        return builder.build();
    }

    public AuthenticationContext createAuthenticationContext() {
        if (!signing) {
            throw new IllegalStateException("Cannot create AuthenticationContext if no sign in is required");
        }
        return new AuthenticationContext(Optional.of(username).orElseThrow(() -> new IllegalArgumentException("Username cannot be null when using NTLM authentication")),
                                         Optional.of(password).orElse("").toCharArray(),
                                         Optional.of(domain).orElse(""));
    }

    public boolean isNtlmAuthentication() {
        return authType.equalsIgnoreCase("ntlm");
    }

    @Override
    public void configure(URI uri) {
        super.configure(uri);
        String userInfo = uri.getUserInfo();

        if (userInfo != null) {
            if (userInfo.contains(DOMAIN_SEPARATOR)) {
                setDomain(ObjectHelper.before(userInfo, DOMAIN_SEPARATOR));
                userInfo = ObjectHelper.after(userInfo, DOMAIN_SEPARATOR);
            }
            if (userInfo.contains(USER_PASS_SEPARATOR)) {
                setUsername(ObjectHelper.before(userInfo, USER_PASS_SEPARATOR));
                setPassword(ObjectHelper.after(userInfo, USER_PASS_SEPARATOR));
            } else {
                setUsername(userInfo);
            }
        }

        setHost(uri.getHost());
        if (uri.getPort() <= 0) {
            setPort(null);
        } else {
            setPort(uri.getPort());
        }
        setDirectory(uri.getPath().replace("\\", "/"));
    }

    private SMB2Dialect resolveSmbDialect() {
        if (version.equalsIgnoreCase("unkown")) {
            return SMB2Dialect.UNKNOWN;
        }
        return SMB2Dialect.valueOf(String.format("SMB_%s", version.toUpperCase()));
    }

    //region Getter and Setter
    public String getDomain() {
        return domain;
    }

    public void setDomain(String domain) {
        this.domain = domain;
    }

    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public Integer getPort() {
        return port;
    }

    public void setPort(Integer port) {
        this.port = port;
    }

    public String getVersion() {
        return version;
    }

    public void setVersion(String version) {
        this.version = version;
    }

    public String getAuthType() {
        return authType;
    }

    public void setAuthType(String authType) {
        this.authType = authType;
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

    public Boolean getDownload() {
        return download;
    }

    public void setDownload(Boolean download) {
        this.download = download;
    }

    public String getShareName() {
        return shareName;
    }

    public void setShareName(String shareName) {
        this.shareName = shareName;
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
    //endregion
}
