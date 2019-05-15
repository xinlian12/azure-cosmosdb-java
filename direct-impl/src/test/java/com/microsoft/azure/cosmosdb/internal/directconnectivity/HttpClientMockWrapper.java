/*
 * The MIT License (MIT)
 * Copyright (c) 2018 Microsoft Corporation
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */

package com.microsoft.azure.cosmosdb.internal.directconnectivity;

import com.microsoft.azure.cosmosdb.rx.internal.http.HttpClient;
import com.microsoft.azure.cosmosdb.rx.internal.http.HttpHeaders;
import com.microsoft.azure.cosmosdb.rx.internal.http.HttpRequest;
import com.microsoft.azure.cosmosdb.rx.internal.http.HttpResponse;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.ByteBufUtil;
import org.mockito.Mockito;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class HttpClientMockWrapper {
    public static HttpClientBehaviourBuilder httpClientBehaviourBuilder() {
        return new HttpClientBehaviourBuilder();
    }

    public static class HttpClientBehaviourBuilder {
        private int status;
        private String content;
        private HttpHeaders httpHeaders = new HttpHeaders();
        private Exception networkFailure;

        public HttpClientBehaviourBuilder withNetworkFailure(Exception networkFailure) {
            this.networkFailure = networkFailure;
            return this;
        }

        public HttpClientBehaviourBuilder withStatus(int status) {
            this.status = status;
            return this;
        }

        public HttpClientBehaviourBuilder withHeaders(HttpHeaders httpHeaders) {
            this.httpHeaders = httpHeaders;
            return this;
        }

        public HttpClientBehaviourBuilder withHeaders(String... pairs) {
            if (pairs.length % 2 != 0) {
                throw new IllegalArgumentException();
            }

            for(int i = 0; i < pairs.length/ 2; i++) {
                this.httpHeaders.set(pairs[2*i], pairs[2*i +1]);
            }

            return this;
        }

        public HttpClientBehaviourBuilder withContent(String content) {
            this.content = content;
            return this;
        }

        public HttpClientBehaviourBuilder withHeaderLSN(long lsn) {
            this.httpHeaders.set(WFConstants.BackendHeaders.LSN, Long.toString(lsn));
            return this;
        }

        public HttpClientBehaviourBuilder withHeaderPartitionKeyRangeId(String partitionKeyRangeId) {
            this.httpHeaders.set(WFConstants.BackendHeaders.PARTITION_KEY_RANGE_ID, partitionKeyRangeId);
            return this;
        }

        public HttpClientBehaviourBuilder withHeaderSubStatusCode(int subStatusCode) {
            this.httpHeaders.set(WFConstants.BackendHeaders.SUB_STATUS, Integer.toString(subStatusCode));
            return this;
        }

        public HttpResponse asHttpClientResponse() {
            if (this.networkFailure != null) {
                return null;
            }

            HttpResponse resp = Mockito.mock(HttpResponse.class);
            Mockito.doReturn(this.status).when(resp).statusCode();
            Mockito.doReturn(Flux.just(ByteBufUtil.writeUtf8(ByteBufAllocator.DEFAULT, this.content))).when(resp).body();

            try {
                Mockito.doReturn(this.httpHeaders).when(resp).headers();

            } catch (IllegalArgumentException | SecurityException e) {
                throw new IllegalStateException("Failed to instantiate class object.", e);
            }

            return resp;
        }

        public Exception asNetworkFailure() {
            return this.networkFailure;
        }

        @Override
        public String toString() {
            return "HttpClientBehaviourBuilder{" +
                    "status=" + status +
                    ", content='" + content + '\'' +
                    ", httpHeaders=" + httpHeaders +
                    ", networkFailure=" + networkFailure +
                    '}';
        }
    }

    private final HttpClient httpClient;
    private final List<HttpRequest> requests = Collections.synchronizedList(new ArrayList<>());

    public HttpClientMockWrapper(long responseAfterMillis, HttpResponse httpClientResponse) {
        this(responseAfterMillis, httpClientResponse, null);
    }

    private static Mono<HttpResponse> httpClientResponseOrException(HttpResponse httpClientResponse, Exception e) {
        assert ((httpClientResponse != null && e == null) || (httpClientResponse == null && e != null));
        return httpClientResponse != null ? Mono.just(httpClientResponse) : Mono.error(e);
    }

    public HttpClientMockWrapper(long responseAfterMillis, Exception e) {
        this(responseAfterMillis, null, e);
    }

    public HttpClientMockWrapper(HttpResponse httpClientResponse) {
        this(0, httpClientResponse);
    }

    private HttpClientMockWrapper(long responseAfterMillis, final HttpResponse httpResponse, final Exception e) {
        httpClient = Mockito.mock(HttpClient.class);
        assert httpResponse == null || e == null;

        Mockito.when(httpClient.port(Mockito.anyInt())).thenReturn(httpClient);

        Mockito.doAnswer(invocationOnMock -> {
            HttpRequest httpRequest = invocationOnMock.getArgumentAt(0, HttpRequest.class);
            requests.add(httpRequest);
            if (responseAfterMillis <= 0) {
                return httpClientResponseOrException(httpResponse, e);
            } else {
                return Mono.delay(Duration.ofMillis(responseAfterMillis)).flatMap(t -> httpClientResponseOrException(httpResponse, e));
            }
        }).when(httpClient).send(Mockito.any(HttpRequest.class));
    }

    public HttpClientMockWrapper(HttpClientBehaviourBuilder builder) {
        this(0, builder.asHttpClientResponse(), builder.asNetworkFailure());
    }

    public HttpClientMockWrapper(Exception e) {
        this(0, e);
    }

    public HttpClient getClient() {
        return httpClient;
    }

    public List<HttpRequest> getCapturedInvocation() {
        return requests;
    }
}
