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
package at.ihet.camel.extras.smbj.strategy;

import at.ihet.camel.extras.smbj.SmbFile;
import org.apache.camel.CamelContext;
import org.apache.camel.Expression;
import org.apache.camel.component.file.GenericFileExclusiveReadLockStrategy;
import org.apache.camel.component.file.GenericFileProcessStrategy;
import org.apache.camel.component.file.strategy.*;
import org.apache.camel.util.ObjectHelper;

import java.util.Map;

/**
 * Copied from 'org.apache.camel.component.file.remote.strategy.FtpChangedExclusiveReadLockStrategy' because for smb as well.
 *
 * @author Thomas Herzog <herzog.thomas81@gmail.com>
 * @since 10/27/2018
 */
public class SmbProcessStrategyFactory {

    private SmbProcessStrategyFactory() {
    }

    public static GenericFileProcessStrategy<SmbFile> createGenericFileProcessStrategy(final CamelContext context,
                                                                                       final Map<String, Object> params) {

        // We assume a value is present only if its value not null for String and 'true' for boolean
        Expression moveExpression = (Expression) params.get("move");
        Expression moveFailedExpression = (Expression) params.get("moveFailed");
        Expression preMoveExpression = (Expression) params.get("preMove");
        boolean isNoop = params.get("noop") != null;
        boolean isDelete = params.get("delete") != null;
        boolean isMove = moveExpression != null || preMoveExpression != null || moveFailedExpression != null;

        if (isDelete) {
            GenericFileDeleteProcessStrategy<SmbFile> strategy = new GenericFileDeleteProcessStrategy<SmbFile>();
            strategy.setExclusiveReadLockStrategy(getExclusiveReadLockStrategy(params));
            if (preMoveExpression != null) {
                GenericFileExpressionRenamer<SmbFile> renamer = new GenericFileExpressionRenamer<SmbFile>();
                renamer.setExpression(preMoveExpression);
                strategy.setBeginRenamer(renamer);
            }
            if (moveFailedExpression != null) {
                GenericFileExpressionRenamer<SmbFile> renamer = new GenericFileExpressionRenamer<SmbFile>();
                renamer.setExpression(moveFailedExpression);
                strategy.setFailureRenamer(renamer);
            }
            return strategy;
        } else if (isMove || isNoop) {
            GenericFileRenameProcessStrategy<SmbFile> strategy = new GenericFileRenameProcessStrategy<SmbFile>();
            strategy.setExclusiveReadLockStrategy(getExclusiveReadLockStrategy(params));
            if (!isNoop && moveExpression != null) {
                // move on commit is only possible if not noop
                GenericFileExpressionRenamer<SmbFile> renamer = new GenericFileExpressionRenamer<SmbFile>();
                renamer.setExpression(moveExpression);
                strategy.setCommitRenamer(renamer);
            }
            // both move and noop supports pre move
            if (moveFailedExpression != null) {
                GenericFileExpressionRenamer<SmbFile> renamer = new GenericFileExpressionRenamer<SmbFile>();
                renamer.setExpression(moveFailedExpression);
                strategy.setFailureRenamer(renamer);
            }
            // both move and noop supports pre move
            if (preMoveExpression != null) {
                GenericFileExpressionRenamer<SmbFile> renamer = new GenericFileExpressionRenamer<SmbFile>();
                renamer.setExpression(preMoveExpression);
                strategy.setBeginRenamer(renamer);
            }
            return strategy;
        } else {
            // default strategy will do nothing
            GenericFileNoOpProcessStrategy<SmbFile> strategy = new GenericFileNoOpProcessStrategy<SmbFile>();
            strategy.setExclusiveReadLockStrategy(getExclusiveReadLockStrategy(params));
            return strategy;
        }

    }

    @SuppressWarnings("unchecked")
    private static GenericFileExclusiveReadLockStrategy<SmbFile> getExclusiveReadLockStrategy(final Map<String, Object> params) {
        GenericFileExclusiveReadLockStrategy<SmbFile> strategy = (GenericFileExclusiveReadLockStrategy<SmbFile>) params.get("exclusiveReadLockStrategy");
        if (strategy != null) {
            return strategy;
        }

        // no explicit strategy set then fallback to readLock option
        String readLock = (String) params.get("readLock");
        if (ObjectHelper.isNotEmpty(readLock)) {
            if ("none".equals(readLock) || "false".equals(readLock)) {
                return null;
            } else if ("rename".equals(readLock)) {
                GenericFileRenameExclusiveReadLockStrategy<SmbFile> readLockStrategy = new GenericFileRenameExclusiveReadLockStrategy<SmbFile>();
                Long timeout = (Long) params.get("readLockTimeout");
                if (timeout != null) {
                    readLockStrategy.setTimeout(timeout);
                }
                Long checkInterval = (Long) params.get("readLockCheckInterval");
                if (checkInterval != null) {
                    readLockStrategy.setCheckInterval(checkInterval);
                }
                Boolean readLockMarkerFile = (Boolean) params.get("readLockMarkerFile");
                if (readLockMarkerFile != null) {
                    readLockStrategy.setMarkerFiler(readLockMarkerFile);
                }
                return readLockStrategy;
            } else if ("changed".equals(readLock)) {
                SmbChangedExclusiveReadLockStrategy readLockStrategy = new SmbChangedExclusiveReadLockStrategy();
                Long timeout = (Long) params.get("readLockTimeout");
                if (timeout != null) {
                    readLockStrategy.setTimeout(timeout);
                }
                Long checkInterval = (Long) params.get("readLockCheckInterval");
                if (checkInterval != null) {
                    readLockStrategy.setCheckInterval(checkInterval);
                }
                Long minLength = (Long) params.get("readLockMinLength");
                if (minLength != null) {
                    readLockStrategy.setMinLength(minLength);
                }
                Long minAge = (Long) params.get("readLockMinAge");
                if (null != minAge) {
                    readLockStrategy.setMinAge(minAge);
                }
                Boolean fastExistsCheck = (Boolean) params.get("fastExistsCheck");
                if (fastExistsCheck != null) {
                    readLockStrategy.setFastExistsCheck(fastExistsCheck);
                }
                Boolean readLockMarkerFile = (Boolean) params.get("readLockMarkerFile");
                if (readLockMarkerFile != null) {
                    readLockStrategy.setMarkerFiler(readLockMarkerFile);
                }
                return readLockStrategy;
            }
        }

        return null;
    }

}
