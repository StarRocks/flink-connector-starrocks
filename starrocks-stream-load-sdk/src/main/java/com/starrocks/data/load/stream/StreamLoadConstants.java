/*
 * Copyright 2021-present StarRocks, Inc. All rights reserved.
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.starrocks.data.load.stream;

public interface StreamLoadConstants {

    String PATH_STREAM_LOAD = "/api/{db}/{table}/_stream_load";

    String PATH_TRANSACTION_BEGIN = "/api/transaction/begin";
    String PATH_TRANSACTION_SEND = "/api/transaction/load";
    String PATH_TRANSACTION_ROLLBACK = "/api/transaction/rollback";
    String PATH_TRANSACTION_PRE_COMMIT = "/api/transaction/prepare";
    String PATH_TRANSACTION_COMMIT = "/api/transaction/commit";

    String PATH_STREAM_LOAD_STATE = "/api/{db}/get_load_state?label={label}";

    String RESULT_STATUS_OK = "OK";
    String RESULT_STATUS_SUCCESS = "Success";
    String RESULT_STATUS_INTERNAL_ERROR = "INTERNAL_ERROR";
    String RESULT_STATUS_FAILED = "Fail";
    String RESULT_STATUS_LABEL_EXISTED = "Label Already Exists";
    String RESULT_STATUS_TRANSACTION_NOT_EXISTED = "TXN_NOT_EXISTS";
    String RESULT_STATUS_TRANSACTION_IN_PROCESSING = "TXN_IN_PROCESSING";
    String RESULT_STATUS_TRANSACTION_COMMIT_TIMEOUT = "Commit Timeout";
    String RESULT_STATUS_TRANSACTION_PUBLISH_TIMEOUT = "Publish Timeout";

    String EXISTING_JOB_STATUS_FINISHED = "FINISHED";

    public static String getBeginUrl(String host) {
        if (host == null) {
            throw new IllegalArgumentException("None of the hosts in `load_url` could be connected.");
        }

        return host + StreamLoadConstants.PATH_TRANSACTION_BEGIN;
    }

    public static String getSendUrl(String host) {
        if (host == null) {
            throw new IllegalArgumentException("None of the hosts in `load_url` could be connected.");
        }

        return host + StreamLoadConstants.PATH_TRANSACTION_SEND;
    }

    public static String getPrepareUrl(String host) {
        if (host == null) {
            throw new IllegalArgumentException("None of the hosts in `load_url` could be connected.");
        }
        return host + StreamLoadConstants.PATH_TRANSACTION_PRE_COMMIT;
    }

    public static String getCommitUrl(String host) {
        if (host == null) {
            throw new IllegalArgumentException("None of the hosts in `load_url` could be connected.");
        }
        return host + StreamLoadConstants.PATH_TRANSACTION_COMMIT;
    }

    public static String getRollbackUrl(String host) {
        if (host == null) {
            throw new IllegalArgumentException("None of the hosts in `load_url` could be connected.");
        }
        return host + StreamLoadConstants.PATH_TRANSACTION_ROLLBACK;
    }
}
