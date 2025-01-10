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

import com.fasterxml.jackson.databind.JsonNode;
import com.starrocks.data.load.stream.exception.StreamLoadFailException;
import com.starrocks.data.load.stream.properties.StreamLoadProperties;
import org.apache.http.Header;
import org.apache.http.HttpHeaders;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.DefaultRedirectStrategy;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.message.BasicHeader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static com.starrocks.data.load.stream.StreamLoadConstants.getBeginUrl;
import static com.starrocks.data.load.stream.StreamLoadConstants.getCommitUrl;
import static com.starrocks.data.load.stream.StreamLoadConstants.getPrepareUrl;
import static com.starrocks.data.load.stream.StreamLoadConstants.getRollbackUrl;

public class TransactionStreamLoader extends DefaultStreamLoader {

    private static final Logger log = LoggerFactory.getLogger(TransactionStreamLoader.class);

    private Header[] defaultTxnHeaders;

    private Header[] beginTxnHeader;

    private HttpClientBuilder clientBuilder;

    private StreamLoadManager manager;

    protected void initTxHeaders(StreamLoadProperties properties) {
        Map<String, String> headers = new HashMap<>();
        headers.put(HttpHeaders.AUTHORIZATION, StreamLoadUtils.getBasicAuthHeader(properties.getUsername(), properties.getPassword()));
        this.defaultTxnHeaders = headers.entrySet().stream()
                .map(entry -> new BasicHeader(entry.getKey(), entry.getValue()))
                .toArray(Header[]::new);

        Map<String, String> beginHeaders = new HashMap<>(headers);
        String timeout = properties.getHeaders().get("timeout");
        if (timeout == null) {
            beginHeaders.put("timeout", "600");
        } else {
            beginHeaders.put("timeout", timeout);
        }
        String warehouse = properties.getHeaders().get("warehouse");
        if (warehouse != null) {
            beginHeaders.put("warehouse", warehouse);
        }
        this.beginTxnHeader = beginHeaders.entrySet().stream()
                .map(entry -> new BasicHeader(entry.getKey(), entry.getValue()))
                .toArray(Header[]::new);
    }

    @Override
    public void start(StreamLoadProperties properties, StreamLoadManager manager) {
        super.start(properties, manager);
        this.manager = manager;
        enableTransaction();
        initTxHeaders(properties);
        clientBuilder = HttpClients.custom()
                .setRedirectStrategy(new DefaultRedirectStrategy() {
                    @Override
                    protected boolean isRedirectable(String method) {
                        return true;
                    }
                });
    }

    @Override
    public boolean begin(TableRegion region) {
        if (region.getLabel() == null) {
            region.setLabel(region.getLabelGenerator().next());
            if (doBegin(region)) {
                return true;
            } else {
                region.setLabel(null);
                return false;
            }
        }
        return true;
    }

    protected boolean doBegin(TableRegion region) {
        String host = getAvailableHost();
        String beginUrl = getBeginUrl(host);
        String label = region.getLabel();
        log.info("Transaction start, label : {}", label);

        HttpPost httpPost = new HttpPost(beginUrl);
        httpPost.setHeaders(beginTxnHeader);
        httpPost.addHeader("label", label);
        httpPost.addHeader("db", region.getDatabase());
        httpPost.addHeader("table", region.getTable());

        httpPost.setConfig(RequestConfig.custom()
                        .setSocketTimeout(properties.getSocketTimeout())
                        .setExpectContinueEnabled(true)
                        .setRedirectsEnabled(true)
                        .build());

        String db = region.getDatabase();
        String table = region.getTable();
        log.info("Transaction start, db: {}, table: {}, label: {}, request : {}", db, table, label, httpPost);

        try (CloseableHttpClient client = clientBuilder.build()) {
            String responseBody;
            try (CloseableHttpResponse response = client.execute(httpPost)) {
                responseBody = parseHttpResponse("begin transaction", region.getDatabase(), region.getTable(), label, response);
            }
            log.info("Transaction started, db: {}, table: {}, label: {}, body : {}", db, table, label, responseBody);

            JsonNode node = objectMapper.readTree(responseBody);
            JsonNode statusNode = node.get("Status");
            String status = statusNode == null ? null : statusNode.asText();

            if (status == null) {
                String errMsg = String.format("Can't find 'Status' in the response of transaction begin request. " +
                        "Transaction load is supported since StarRocks 2.4, and please make sure your " +
                        "StarRocks version support transaction load first. db: %s, table: %s, label: %s, response: %s",
                        db, table, label, responseBody);
                log.error(errMsg);
                throw new StreamLoadFailException(errMsg);
            }

            if (StreamLoadConstants.RESULT_STATUS_OK.equals(status)) {
                return true;
            }

            String errMsg = String.format("Transaction start failed, db: %s, table: %s, label: %s, responseBody: %s",
                    region.getDatabase(), region.getTable(), label, responseBody);
            throw new StreamLoadFailException(errMsg);
        } catch (StreamLoadFailException se) {
            throw se;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public boolean prepare(StreamLoadSnapshot.Transaction transaction) {
        String host = getAvailableHost();
        String prepareUrl = getPrepareUrl(host);

        HttpPost httpPost = new HttpPost(prepareUrl);
        httpPost.setHeaders(defaultTxnHeaders);
        httpPost.addHeader("label", transaction.getLabel());
        httpPost.addHeader("db", transaction.getDatabase());
        httpPost.addHeader("table", transaction.getTable());

        httpPost.setConfig(RequestConfig.custom()
                        .setSocketTimeout(properties.getSocketTimeout())
                        .setExpectContinueEnabled(true)
                        .setRedirectsEnabled(true)
                        .build());

        log.info("Transaction prepare, label : {}, request : {}", transaction.getLabel(), httpPost);

        try (CloseableHttpClient client = clientBuilder.build()) {
            String responseBody;
            try (CloseableHttpResponse response = client.execute(httpPost)) {
                responseBody = parseHttpResponse("prepare transaction", transaction.getDatabase(), transaction.getTable(),
                        transaction.getLabel(), response);
            }
            log.info("Transaction prepared, label : {}, body : {}", transaction.getLabel(), responseBody);

            StreamLoadResponse streamLoadResponse = new StreamLoadResponse();
            StreamLoadResponse.StreamLoadResponseBody streamLoadBody =
                    objectMapper.readValue(responseBody, StreamLoadResponse.StreamLoadResponseBody.class);
            streamLoadResponse.setBody(streamLoadBody);
            String status = streamLoadBody.getStatus();
            if (status == null) {
                throw new StreamLoadFailException(String.format("Prepare transaction status is null. db: %s, table: %s, " +
                        "label: %s, response body: %s", transaction.getDatabase(), transaction.getTable(), transaction.getLabel(),
                        responseBody));
            }

            switch (status) {
                case StreamLoadConstants.RESULT_STATUS_OK:
                    manager.callback(streamLoadResponse);
                    return true;
                case StreamLoadConstants.RESULT_STATUS_TRANSACTION_NOT_EXISTED: {
                    // currently this could happen after timeout which is specified in http header,
                    // but as a protection we check the state again
                    String labelState = getLabelState(host, transaction.getDatabase(), transaction.getTable(), transaction.getLabel(),
                            Collections.singleton(TransactionStatus.PREPARE.name()));
                    if (!TransactionStatus.PREPARED.isSame(labelState)) {
                       String errMsg = String.format("Transaction prepare failed because of unexpected state, " +
                                       "label: %s, state: %s", transaction.getLabel(), labelState);
                       log.error(errMsg);
                       throw new StreamLoadFailException(errMsg);
                    } else {
                        return true;
                    }
                case StreamLoadConstants.RESULT_STATUS_TRANSACTION_IN_PROCESSING:
                    String warnMsg = String.format("Transaction in processing, db: %s, table: %s, label: %s, " +
                                    "\nresponseBody: %s", transaction.getDatabase(), transaction.getTable(),
                                    transaction.getLabel(), responseBody);
                    log.warn(warnMsg);
                    return false;
                }
            }

            String errorLog = getErrorLog(streamLoadBody.getErrorURL());
            String errorMsg = String.format("Transaction prepare failed, db: %s, table: %s, label: %s, " +
                            "\nresponseBody: %s\nerrorLog: %s", transaction.getDatabase(), transaction.getTable(),
                            transaction.getLabel(), responseBody, errorLog);
            log.error(errorMsg);
            throw new StreamLoadFailException(errorMsg);
        } catch (StreamLoadFailException se) {
            throw se;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public boolean commit(StreamLoadSnapshot.Transaction transaction) {
        String host = getAvailableHost();
        String commitUrl = getCommitUrl(host);

        HttpPost httpPost = new HttpPost(commitUrl);
        httpPost.setHeaders(defaultTxnHeaders);
        httpPost.addHeader("label", transaction.getLabel());
        httpPost.addHeader("db", transaction.getDatabase());
        httpPost.addHeader("table", transaction.getTable());

        httpPost.setConfig(RequestConfig.custom()
                        .setSocketTimeout(properties.getSocketTimeout())
                        .setExpectContinueEnabled(true)
                        .setRedirectsEnabled(true)
                        .build());

        log.info("Transaction commit, label: {}, request : {}", transaction.getLabel(), httpPost);

        try (CloseableHttpClient client = clientBuilder.build()) {
            String responseBody;
            try (CloseableHttpResponse response = client.execute(httpPost)) {
                responseBody = parseHttpResponse("commit transaction", transaction.getDatabase(), transaction.getTable(),
                        transaction.getLabel(), response);
            }
            log.info("Transaction committed, lable: {}, body : {}", transaction.getLabel(), responseBody);

            StreamLoadResponse streamLoadResponse = new StreamLoadResponse();
            StreamLoadResponse.StreamLoadResponseBody streamLoadBody =
                    objectMapper.readValue(responseBody, StreamLoadResponse.StreamLoadResponseBody.class);
            streamLoadResponse.setBody(streamLoadBody);
            String status = streamLoadBody.getStatus();
            if (status == null) {
                throw new StreamLoadFailException(String.format("Commit transaction status is null. db: %s, table: %s, " +
                                "label: %s, response body: %s", transaction.getDatabase(), transaction.getTable(), transaction.getLabel(),
                                        responseBody));
            }

            if (StreamLoadConstants.RESULT_STATUS_TRANSACTION_IN_PROCESSING.equals(status)) {
                log.info("Transaction in processing, retry later, lable: {}, body : {}", transaction.getLabel(), responseBody);
                return false;
            }

            if (StreamLoadConstants.RESULT_STATUS_OK.equals(status)) {
                manager.callback(streamLoadResponse);
                return true;
            }

            // there are many corner cases that can lead to non-ok status. some of them are
            // 1. TXN_NOT_EXISTS: transaction timeout and the label is cleanup up
            // 2. Failed: the error message can be "has no backend", The case is that FE leader restarts, and after
            //    that commit the transaction repeatedly because flink/spark job continues failover for some reason , but
            //    the transaction actually success, and this commit should be successful
            // To reduce the dependency for the returned status type, always check the label state
            String labelState = getLabelState(host, transaction.getDatabase(), transaction.getTable(), transaction.getLabel(), Collections.emptySet());
            if (TransactionStatus.COMMITTED.isSame(labelState) || TransactionStatus.VISIBLE.isSame(labelState)) {
                return true;
            }

            String errorLog = getErrorLog(streamLoadBody.getErrorURL());
            log.error("Transaction commit failed, db: {}, table: {}, label: {}, label state: {}, \nresponseBody: {}\nerrorLog: {}",
                    transaction.getDatabase(), transaction.getTable(), transaction.getLabel(), labelState, responseBody, errorLog);

            String exceptionMsg = String.format("Transaction commit failed, db: %s, table: %s, label: %s, commit response status: %s," +
                   " label state: %s", transaction.getDatabase(), transaction.getTable(), transaction.getLabel(), status, labelState);
            // transaction not exist often happens after transaction timeouts
            if (StreamLoadConstants.RESULT_STATUS_TRANSACTION_NOT_EXISTED.equals(status) ||
                TransactionStatus.UNKNOWN.isSame(labelState)) {
                exceptionMsg += ". commit response status with TXN_NOT_EXISTS or label state with UNKNOWN often happens when transaction" +
                        " timeouts, and please check StarRocks FE leader's log to confirm it. You can find the transaction id for the label" +
                        " in the FE log first, and search with the transaction id and the keyword 'expired'";
            }
            throw new StreamLoadFailException(exceptionMsg);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public boolean rollback(StreamLoadSnapshot.Transaction transaction) {
        String host = getAvailableHost();
        String rollbackUrl = getRollbackUrl(host);
        log.info("Transaction rollback, label : {}", transaction.getLabel());

        HttpPost httpPost = new HttpPost(rollbackUrl);
        httpPost.setHeaders(defaultTxnHeaders);
        httpPost.addHeader("label", transaction.getLabel());
        httpPost.addHeader("db", transaction.getDatabase());
        httpPost.addHeader("table", transaction.getTable());

        try (CloseableHttpClient client = clientBuilder.build()) {
            String responseBody;
            try (CloseableHttpResponse response = client.execute(httpPost)) {
                responseBody = parseHttpResponse("abort transaction", transaction.getDatabase(), transaction.getTable(),
                        transaction.getLabel(), response);
            }
            log.info("Transaction rollback, label: {}, body : {}", transaction.getLabel(), responseBody);

            JsonNode node = objectMapper.readTree(responseBody);
            JsonNode statusNode = node.get("Status");
            String status = statusNode == null ? null : statusNode.asText();

            if (status == null) {
                String errMsg = String.format("Abort transaction status is null. db: %s, table: %s, label: %s, response: %s",
                        transaction.getDatabase(), transaction.getTable(), transaction.getLabel(), responseBody);
                log.error(errMsg);
                throw new StreamLoadFailException(errMsg);
            }

            if (StreamLoadConstants.RESULT_STATUS_SUCCESS.equals(status) || StreamLoadConstants.RESULT_STATUS_OK.equals(status)) {
                return true;
            }

            JsonNode msgNode = node.get("Message");
            String msg = msgNode == null ? "" : msgNode.asText();
            log.error("Transaction rollback failed, db: {}, table: {}, label : {}, message: {}",
                    transaction.getDatabase(), transaction.getTable(), transaction.getLabel(), msg);
            return false;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    protected String getSendUrl(String host, String database, String table) {
       return StreamLoadConstants.getSendUrl(host);
    }
}
