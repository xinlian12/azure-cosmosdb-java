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

import com.microsoft.azure.cosmosdb.rx.internal.RxDocumentServiceRequest;
import rx.Scheduler;
import rx.Single;
import rx.schedulers.Schedulers;

import java.net.URI;

public abstract class TransportClient implements AutoCloseable {

    // Uses requests's ResourceOperation to determine the operation
    public Single<StoreResponse> invokeResourceOperationAsync(Uri physicalAddress, RxDocumentServiceRequest request) {
        return this.invokeStoreAsync(physicalAddress, new ResourceOperation(request.getOperationType(), request.getResourceType()), request)
                .observeOn(Schedulers.computation());
    }

    protected abstract Single<StoreResponse> invokeStoreAsync(
            Uri physicalAddress,
            ResourceOperation resourceOperation,
            RxDocumentServiceRequest request);
}
