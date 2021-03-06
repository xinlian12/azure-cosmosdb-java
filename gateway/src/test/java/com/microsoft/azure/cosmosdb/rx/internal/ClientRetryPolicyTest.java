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

package com.microsoft.azure.cosmosdb.rx.internal;

import com.microsoft.azure.cosmosdb.RetryOptions;
import com.microsoft.azure.cosmosdb.internal.OperationType;
import com.microsoft.azure.cosmosdb.internal.ResourceType;
import io.netty.handler.timeout.ReadTimeoutException;
import org.mockito.Mockito;
import org.testng.annotations.Test;
import rx.Completable;
import rx.Single;
import rx.observers.TestSubscriber;

import java.net.URL;
import java.time.Duration;
import java.util.concurrent.TimeUnit;

public class ClientRetryPolicyTest {
    private final static int TIMEOUT = 10000;

    @Test(groups = "unit")
    public void networkFailureOnRead() throws Exception {
        RetryOptions retryOptions = new RetryOptions();
        GlobalEndpointManager endpointManager = Mockito.mock(GlobalEndpointManager.class);
        Mockito.doReturn(new URL("http://localhost")).when(endpointManager).resolveServiceEndpoint(Mockito.any(RxDocumentServiceRequest.class));
        Mockito.doReturn(Completable.complete()).when(endpointManager).refreshLocationAsync(Mockito.eq(null), Mockito.eq(false));
        ClientRetryPolicy clientRetryPolicy = new ClientRetryPolicy(endpointManager, true, retryOptions);

        Exception exception = ReadTimeoutException.INSTANCE;

        RxDocumentServiceRequest dsr = RxDocumentServiceRequest.createFromName(
                OperationType.Read, "/dbs/db/colls/col/docs/docId", ResourceType.Document);
        dsr.requestContext = Mockito.mock(DocumentServiceRequestContext.class);

        clientRetryPolicy.onBeforeSendRequest(dsr);

        for (int i = 0; i < 10; i++) {
            Single<IRetryPolicy.ShouldRetryResult> shouldRetry = clientRetryPolicy.shouldRetry(exception);

            validateSuccess(shouldRetry, ShouldRetryValidator.builder()
                    .nullException()
                    .shouldRetry(true)
                    .backOfTime(Duration.ofMillis(ClientRetryPolicy.RetryIntervalInMS))
                    .build());

            Mockito.verify(endpointManager, Mockito.times(i + 1)).markEndpointUnavailableForRead(Mockito.any());
            Mockito.verify(endpointManager, Mockito.times(0)).markEndpointUnavailableForWrite(Mockito.any());
        }
    }

    @Test(groups = "unit")
    public void networkFailureOnWrite() throws Exception {
        RetryOptions retryOptions = new RetryOptions();
        GlobalEndpointManager endpointManager = Mockito.mock(GlobalEndpointManager.class);
        Mockito.doReturn(new URL("http://localhost")).when(endpointManager).resolveServiceEndpoint(Mockito.any(RxDocumentServiceRequest.class));
        Mockito.doReturn(Completable.complete()).when(endpointManager).refreshLocationAsync(Mockito.eq(null), Mockito.eq(false));
        ClientRetryPolicy clientRetryPolicy = new ClientRetryPolicy(endpointManager, true, retryOptions);

        Exception exception = ReadTimeoutException.INSTANCE;

        RxDocumentServiceRequest dsr = RxDocumentServiceRequest.createFromName(
                OperationType.Create, "/dbs/db/colls/col/docs/docId", ResourceType.Document);
        dsr.requestContext = Mockito.mock(DocumentServiceRequestContext.class);

        clientRetryPolicy.onBeforeSendRequest(dsr);
        for (int i = 0; i < 10; i++) {
            Single<IRetryPolicy.ShouldRetryResult> shouldRetry = clientRetryPolicy.shouldRetry(exception);
            //  We don't want to retry writes on network failure
            validateSuccess(shouldRetry, ShouldRetryValidator.builder()
                    .nullException()
                    .shouldRetry(false)
                    .build());

            Mockito.verify(endpointManager, Mockito.times(0)).markEndpointUnavailableForRead(Mockito.any());
            Mockito.verify(endpointManager, Mockito.times(i + 1)).markEndpointUnavailableForWrite(Mockito.any());
        }
    }

    @Test(groups = "unit")
    public void networkFailureOnUpsert() throws Exception {
        RetryOptions retryOptions = new RetryOptions();
        GlobalEndpointManager endpointManager = Mockito.mock(GlobalEndpointManager.class);
        Mockito.doReturn(new URL("http://localhost")).when(endpointManager).resolveServiceEndpoint(Mockito.any(RxDocumentServiceRequest.class));
        Mockito.doReturn(Completable.complete()).when(endpointManager).refreshLocationAsync(Mockito.eq(null), Mockito.eq(false));
        ClientRetryPolicy clientRetryPolicy = new ClientRetryPolicy(endpointManager, true, retryOptions);

        Exception exception = ReadTimeoutException.INSTANCE;

        RxDocumentServiceRequest dsr = RxDocumentServiceRequest.createFromName(
            OperationType.Upsert, "/dbs/db/colls/col/docs/docId", ResourceType.Document);
        dsr.requestContext = Mockito.mock(DocumentServiceRequestContext.class);

        clientRetryPolicy.onBeforeSendRequest(dsr);
        for (int i = 0; i < 10; i++) {
            Single<IRetryPolicy.ShouldRetryResult> shouldRetry = clientRetryPolicy.shouldRetry(exception);
            //  We don't want to retry writes on network failure
            validateSuccess(shouldRetry, ShouldRetryValidator.builder()
                                                             .nullException()
                                                             .shouldRetry(false)
                                                             .build());

            Mockito.verify(endpointManager, Mockito.times(0)).markEndpointUnavailableForRead(Mockito.any());
            Mockito.verify(endpointManager, Mockito.times(i + 1)).markEndpointUnavailableForWrite(Mockito.any());
        }
    }

    @Test(groups = "unit")
    public void networkFailureOnDelete() throws Exception {
        RetryOptions retryOptions = new RetryOptions();
        GlobalEndpointManager endpointManager = Mockito.mock(GlobalEndpointManager.class);
        Mockito.doReturn(new URL("http://localhost")).when(endpointManager).resolveServiceEndpoint(Mockito.any(RxDocumentServiceRequest.class));
        Mockito.doReturn(Completable.complete()).when(endpointManager).refreshLocationAsync(Mockito.eq(null), Mockito.eq(false));
        ClientRetryPolicy clientRetryPolicy = new ClientRetryPolicy(endpointManager, true, retryOptions);

        Exception exception = ReadTimeoutException.INSTANCE;

        RxDocumentServiceRequest dsr = RxDocumentServiceRequest.createFromName(
            OperationType.Delete, "/dbs/db/colls/col/docs/docId", ResourceType.Document);
        dsr.requestContext = Mockito.mock(DocumentServiceRequestContext.class);

        clientRetryPolicy.onBeforeSendRequest(dsr);
        for (int i = 0; i < 10; i++) {
            Single<IRetryPolicy.ShouldRetryResult> shouldRetry = clientRetryPolicy.shouldRetry(exception);
            //  We don't want to retry writes on network failure
            validateSuccess(shouldRetry, ShouldRetryValidator.builder()
                                                             .nullException()
                                                             .shouldRetry(false)
                                                             .build());

            Mockito.verify(endpointManager, Mockito.times(0)).markEndpointUnavailableForRead(Mockito.any());
            Mockito.verify(endpointManager, Mockito.times(i + 1)).markEndpointUnavailableForWrite(Mockito.any());
        }
    }

    @Test(groups = "unit")
    public void onBeforeSendRequestNotInvoked() {
        RetryOptions retryOptions = new RetryOptions();
        GlobalEndpointManager endpointManager = Mockito.mock(GlobalEndpointManager.class);

        Mockito.doReturn(Completable.complete()).when(endpointManager).refreshLocationAsync(Mockito.eq(null), Mockito.eq(false));
        ClientRetryPolicy clientRetryPolicy = new ClientRetryPolicy(endpointManager, true, retryOptions);

        Exception exception = ReadTimeoutException.INSTANCE;

        RxDocumentServiceRequest dsr = RxDocumentServiceRequest.createFromName(
                OperationType.Create, "/dbs/db/colls/col/docs/docId", ResourceType.Document);
        dsr.requestContext = Mockito.mock(DocumentServiceRequestContext.class);

        Single<IRetryPolicy.ShouldRetryResult> shouldRetry = clientRetryPolicy.shouldRetry(exception);
        validateSuccess(shouldRetry, ShouldRetryValidator.builder()
                .withException(exception)
                .shouldRetry(false)
                .build());

        Mockito.verifyZeroInteractions(endpointManager);
    }

    public static void validateSuccess(Single<IRetryPolicy.ShouldRetryResult> single,
                                       ShouldRetryValidator validator) {

        validateSuccess(single, validator, TIMEOUT);
    }

    public static void validateSuccess(Single<IRetryPolicy.ShouldRetryResult> single,
                                       ShouldRetryValidator validator,
                                       long timeout) {
        TestSubscriber<IRetryPolicy.ShouldRetryResult> testSubscriber = new TestSubscriber<>();

        single.toObservable().subscribe(testSubscriber);
        testSubscriber.awaitTerminalEvent(timeout, TimeUnit.MILLISECONDS);
        testSubscriber.assertCompleted();
        testSubscriber.assertNoErrors();
        testSubscriber.assertValueCount(1);
        validator.validate(testSubscriber.getOnNextEvents().get(0));
    }
}
