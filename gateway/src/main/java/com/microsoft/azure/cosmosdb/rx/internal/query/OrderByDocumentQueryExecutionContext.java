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
package com.microsoft.azure.cosmosdb.rx.internal.query;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Function;

import org.apache.commons.lang3.NotImplementedException;
import org.apache.commons.lang3.tuple.ImmutablePair;

import com.microsoft.azure.cosmosdb.BridgeInternal;
import com.microsoft.azure.cosmosdb.DocumentClientException;
import com.microsoft.azure.cosmosdb.FeedOptions;
import com.microsoft.azure.cosmosdb.FeedResponse;
import com.microsoft.azure.cosmosdb.PartitionKeyRange;
import com.microsoft.azure.cosmosdb.Resource;
import com.microsoft.azure.cosmosdb.SqlQuerySpec;
import com.microsoft.azure.cosmosdb.internal.HttpConstants;
import com.microsoft.azure.cosmosdb.internal.RequestChargeTracker;
import com.microsoft.azure.cosmosdb.internal.ResourceType;
import com.microsoft.azure.cosmosdb.internal.query.PartitionedQueryExecutionInfo;
import com.microsoft.azure.cosmosdb.internal.query.QueryItem;
import com.microsoft.azure.cosmosdb.internal.query.SortOrder;
import com.microsoft.azure.cosmosdb.internal.query.orderbyquery.OrderByRowResult;
import com.microsoft.azure.cosmosdb.internal.query.orderbyquery.OrderbyRowComparer;
import com.microsoft.azure.cosmosdb.internal.routing.Range;
import com.microsoft.azure.cosmosdb.rx.internal.IDocumentClientRetryPolicy;
import com.microsoft.azure.cosmosdb.rx.internal.RxDocumentServiceRequest;
import com.microsoft.azure.cosmosdb.rx.internal.Utils;
import com.microsoft.azure.cosmosdb.QueryMetrics;
import com.microsoft.azure.cosmosdb.rx.internal.Utils.ValueHolder;

import rx.Observable;
import rx.Observable.Transformer;
import rx.functions.Func0;
import rx.functions.Func1;
import rx.functions.Func3;

/**
 * While this class is public, but it is not part of our published public APIs.
 * This is meant to be internally used only by our sdk.
 */
public class OrderByDocumentQueryExecutionContext<T extends Resource>
        extends ParallelDocumentQueryExecutionContextBase<T> {
    private final String FormatPlaceHolder = "{documentdb-formattableorderbyquery-filter}";
    private final String True = "true";
    private final String collectionRid;
    private final OrderbyRowComparer<T> consumeComparer;
    private final RequestChargeTracker tracker;
    private final ConcurrentMap<String, QueryMetrics> queryMetricMap;
    private Observable<OrderByRowResult<T>> orderByObservable;
    private final Map<String, OrderByContinuationToken> targetRangeToOrderByContinuationTokenMap;

    private OrderByDocumentQueryExecutionContext(
            IDocumentQueryClient client,
            List<PartitionKeyRange> partitionKeyRanges,
            ResourceType resourceTypeEnum,
            Class<T> klass,
            SqlQuerySpec query,
            FeedOptions feedOptions,
            String resourceLink,
            String rewrittenQuery,
            boolean isContinuationExpected,
            boolean getLazyFeedResponse,
            OrderbyRowComparer<T> consumeComparer,
            String collectionRid,
            UUID correlatedActivityId) {
        super(client, partitionKeyRanges, resourceTypeEnum, klass, query, feedOptions, resourceLink, rewrittenQuery,
                isContinuationExpected, getLazyFeedResponse, correlatedActivityId);
        this.collectionRid = collectionRid;
        this.consumeComparer = consumeComparer;
        this.tracker = new RequestChargeTracker();
        this.queryMetricMap = new ConcurrentHashMap<>();
        targetRangeToOrderByContinuationTokenMap = new HashMap<>();
    }

    public static <T extends Resource> Observable<IDocumentQueryExecutionComponent<T>> createAsync(
            IDocumentQueryClient client,
            ResourceType resourceTypeEnum,
            Class<T> resourceType,
            SqlQuerySpec expression,
            FeedOptions feedOptions,
            String resourceLink,
            String collectionRid,
            PartitionedQueryExecutionInfo partitionedQueryExecutionInfo,
            List<PartitionKeyRange> partitionKeyRanges,
            int initialPageSize,
            boolean isContinuationExpected,
            boolean getLazyFeedResponse,
            UUID correlatedActivityId) {

        OrderByDocumentQueryExecutionContext<T> context = new OrderByDocumentQueryExecutionContext<T>(client,
                partitionKeyRanges,
                resourceTypeEnum,
                resourceType,
                expression,
                feedOptions,
                resourceLink,
                partitionedQueryExecutionInfo.getQueryInfo().getRewrittenQuery(),
                isContinuationExpected,
                getLazyFeedResponse,
                new OrderbyRowComparer<T>(partitionedQueryExecutionInfo.getQueryInfo().getOrderBy()),
                collectionRid,
                correlatedActivityId);

        try {
            context.initialize(partitionKeyRanges,
                    partitionedQueryExecutionInfo.getQueryInfo().getOrderBy(),
                    partitionedQueryExecutionInfo.getQueryInfo().getOrderByExpressions(),
                    initialPageSize,
                    feedOptions.getRequestContinuation());

            return Observable.just(context);
        } catch (DocumentClientException dce) {
            return Observable.error(dce);
        }
    }

    private void initialize(
            List<PartitionKeyRange> partitionKeyRanges,
            List<SortOrder> sortOrders,
            Collection<String> orderByExpressions,
            int initialPageSize,
            String continuationToken) throws DocumentClientException {
        if (continuationToken == null) {
            // First iteration so use null continuation tokens and "true" filters
            Map<PartitionKeyRange, String> partitionKeyRangeToContinuationToken = new HashMap<PartitionKeyRange, String>();
            for (PartitionKeyRange partitionKeyRange : partitionKeyRanges) {
                partitionKeyRangeToContinuationToken.put(partitionKeyRange,
                        null);
            }

            super.initialize(collectionRid,
                    partitionKeyRangeToContinuationToken,
                    initialPageSize,
                    new SqlQuerySpec(querySpec.getQueryText().replace(FormatPlaceHolder,
                            True),
                            querySpec.getParameters()));
        } else {
            // Check to see if order by continuation token is a valid JSON.
            OrderByContinuationToken orderByContinuationToken;
            ValueHolder<OrderByContinuationToken> outOrderByContinuationToken = new ValueHolder<OrderByContinuationToken>();
            if (!OrderByContinuationToken.tryParse(continuationToken,
                    outOrderByContinuationToken)) {
                String message = String.format("Invalid JSON in continuation token %s for OrderBy~Context",
                        continuationToken);
                throw new DocumentClientException(HttpConstants.StatusCodes.BADREQUEST,
                        message);
            }

            orderByContinuationToken = outOrderByContinuationToken.v;

            CompositeContinuationToken compositeContinuationToken = orderByContinuationToken
                    .getCompositeContinuationToken();
            // Check to see if the ranges inside are valid
            if (compositeContinuationToken.getRange().isEmpty()) {
                String message = String.format("Invalid Range in the continuation token %s for OrderBy~Context.",
                        continuationToken);
                throw new DocumentClientException(HttpConstants.StatusCodes.BADREQUEST,
                        message);
            }

            // At this point the token is valid.
            ImmutablePair<Integer, FormattedFilterInfo> targetIndexAndFilters = this.GetFiltersForPartitions(
                    orderByContinuationToken,
                    partitionKeyRanges,
                    sortOrders,
                    orderByExpressions);

            int targetIndex = targetIndexAndFilters.left;
            targetRangeToOrderByContinuationTokenMap.put(String.valueOf(targetIndex), orderByContinuationToken);
            FormattedFilterInfo formattedFilterInfo = targetIndexAndFilters.right;

            // Left
            String filterForRangesLeftOfTheTargetRange = formattedFilterInfo.getFilterForRangesLeftOfTheTargetRange();
            this.initializeRangeWithContinuationTokenAndFilter(partitionKeyRanges,
                    /* startInclusive */ 0,
                    /* endExclusive */ targetIndex,
                    /* continuationToken */ null,
                    filterForRangesLeftOfTheTargetRange,
                    initialPageSize);

            // Target
            String filterForTargetRange = formattedFilterInfo.getFilterForTargetRange();
            this.initializeRangeWithContinuationTokenAndFilter(partitionKeyRanges,
                    /* startInclusive */ targetIndex,
                    /* endExclusive */ targetIndex + 1,
                    null,
                    filterForTargetRange,
                    initialPageSize);

            // Right
            String filterForRangesRightOfTheTargetRange = formattedFilterInfo.getFilterForRangesRightOfTheTargetRange();
            this.initializeRangeWithContinuationTokenAndFilter(partitionKeyRanges,
                    /* startInclusive */ targetIndex + 1,
                    /* endExclusive */ partitionKeyRanges.size(),
                    /* continuationToken */ null,
                    filterForRangesRightOfTheTargetRange,
                    initialPageSize);
        }

        orderByObservable = OrderByUtils.orderedMerge(resourceType,
                consumeComparer,
                tracker,
                documentProducers,
                queryMetricMap,
                targetRangeToOrderByContinuationTokenMap);
    }

    private void initializeRangeWithContinuationTokenAndFilter(
            List<PartitionKeyRange> partitionKeyRanges,
            int startInclusive,
            int endExclusive,
            String continuationToken,
            String filter,
            int initialPageSize) {
        Map<PartitionKeyRange, String> partitionKeyRangeToContinuationToken = new HashMap<PartitionKeyRange, String>();
        for (int i = startInclusive; i < endExclusive; i++) {
            PartitionKeyRange partitionKeyRange = partitionKeyRanges.get(i);
            partitionKeyRangeToContinuationToken.put(partitionKeyRange,
                    continuationToken);
        }

        super.initialize(collectionRid,
                partitionKeyRangeToContinuationToken,
                initialPageSize,
                new SqlQuerySpec(querySpec.getQueryText().replace(FormatPlaceHolder,
                        filter),
                        querySpec.getParameters()));
    }

    private ImmutablePair<Integer, FormattedFilterInfo> GetFiltersForPartitions(
            OrderByContinuationToken orderByContinuationToken,
            List<PartitionKeyRange> partitionKeyRanges,
            List<SortOrder> sortOrders,
            Collection<String> orderByExpressions) throws DocumentClientException {
        // Find the partition key range we left off on
        int startIndex = this.FindTargetRangeAndExtractContinuationTokens(partitionKeyRanges,
                orderByContinuationToken.getCompositeContinuationToken().getRange());

        // Get the filters.
        FormattedFilterInfo formattedFilterInfo = this.GetFormattedFilters(orderByExpressions,
                orderByContinuationToken.getOrderByItems(),
                sortOrders,
                orderByContinuationToken.getInclusive());

        return new ImmutablePair<Integer, FormattedFilterInfo>(startIndex,
                formattedFilterInfo);
    }

    private OrderByDocumentQueryExecutionContext<T>.FormattedFilterInfo GetFormattedFilters(
            Collection<String> orderByExpressionCollection,
            QueryItem[] orderByItems,
            Collection<SortOrder> sortOrderCollection,
            boolean inclusive) {
        // Convert to arrays
        SortOrder[] sortOrders = new SortOrder[sortOrderCollection.size()];
        sortOrderCollection.toArray(sortOrders);

        String[] expressions = new String[orderByExpressionCollection.size()];
        orderByExpressionCollection.toArray(expressions);

        // Validate the inputs
        if (expressions.length != sortOrders.length) {
            throw new IllegalArgumentException("expressions.size() != sortOrders.size()");
        }

        if (expressions.length != orderByItems.length) {
            throw new IllegalArgumentException("expressions.size() != orderByItems.length");
        }

        // When we run cross partition queries,
        // we only serialize the continuation token for the partition that we left off
        // on.
        // The only problem is that when we resume the order by query,
        // we don't have continuation tokens for all other partitions.
        // The saving grace is that the data has a composite sort order(query sort
        // order, partition key range id)
        // so we can generate range filters which in turn the backend will turn into rid
        // based continuation tokens,
        // which is enough to get the streams of data flowing from all partitions.
        // The details of how this is done is described below:

        int numOrderByItems = expressions.length;
        boolean isSingleOrderBy = numOrderByItems == 1;
        StringBuilder left = new StringBuilder();
        StringBuilder target = new StringBuilder();
        StringBuilder right = new StringBuilder();

        if (isSingleOrderBy) {
            // For a single order by query we resume the continuations in this manner
            // Suppose the query is SELECT* FROM c ORDER BY c.string ASC
            // And we left off on partition N with the value "B"
            // Then
            // All the partitions to the left will have finished reading "B"
            // Partition N is still reading "B"
            // All the partitions to the right have let to read a "B
            // Therefore the filters should be
            // > "B" , >= "B", and >= "B" respectively
            // Repeat the same logic for DESC and you will get
            // < "B", <= "B", and <= "B" respectively
            // The general rule becomes
            // For ASC
            // > for partitions to the left
            // >= for the partition we left off on
            // >= for the partitions to the right
            // For DESC
            // < for partitions to the left
            // <= for the partition we left off on
            // <= for the partitions to the right
            String expression = expressions[0];
            SortOrder sortOrder = sortOrders[0];
            QueryItem orderByItem = orderByItems[0];
            Object rawItem = orderByItem.getItem();
            String orderByItemToString;
            if (rawItem instanceof String) {
                orderByItemToString = "\"" + rawItem.toString().replaceAll("\"",
                        "\\\"") + "\"";
            } else {
                orderByItemToString = rawItem.toString();
            }

            left.append(String.format("%s %s %s",
                    expression,
                    (sortOrder == SortOrder.Descending ? "<" : ">"),
                    orderByItemToString));

            if (inclusive) {
                target.append(String.format("%s %s %s",
                        expression,
                        (sortOrder == SortOrder.Descending ? "<=" : ">="),
                        orderByItemToString));
            } else {
                target.append(String.format("%s %s %s",
                        expression,
                        (sortOrder == SortOrder.Descending ? "<" : ">"),
                        orderByItemToString));
            }

            right.append(String.format("%s %s %s",
                    expression,
                    (sortOrder == SortOrder.Descending ? "<=" : ">="),
                    orderByItemToString));
        } else {
            // This code path needs to be implemented, but it's error prone and needs
            // testing.
            // You can port the implementation from the .net SDK and it should work if
            // ported right.
            throw new NotImplementedException(
                    "Resuming a multi order by query from a continuation token is not supported yet.");
        }

        return new FormattedFilterInfo(left.toString(),
                target.toString(),
                right.toString());
    }

    protected OrderByDocumentProducer<T> createDocumentProducer(
            String collectionRid,
            PartitionKeyRange targetRange,
            String continuationToken,
            int initialPageSize,
            FeedOptions feedOptions,
            SqlQuerySpec querySpecForInit,
            Map<String, String> commonRequestHeaders,
            Func3<PartitionKeyRange, String, Integer, RxDocumentServiceRequest> createRequestFunc,
            Func1<RxDocumentServiceRequest, Observable<FeedResponse<T>>> executeFunc,
            Func0<IDocumentClientRetryPolicy> createRetryPolicyFunc) {
        return new OrderByDocumentProducer<T>(consumeComparer,
                client,
                collectionRid,
                feedOptions,
                createRequestFunc,
                executeFunc,
                targetRange,
                collectionRid,
                () -> client.getResetSessionTokenRetryPolicy().getRequestPolicy(),
                resourceType,
                correlatedActivityId,
                initialPageSize,
                continuationToken,
                top,
                this.targetRangeToOrderByContinuationTokenMap);
    }

    private static class ItemToPageTransformer<T extends Resource>
            implements Transformer<OrderByRowResult<T>, FeedResponse<T>> {
        private final static int DEFAULT_PAGE_SIZE = 100;
        private final RequestChargeTracker tracker;
        private final int maxPageSize;
        private final ConcurrentMap<String, QueryMetrics> queryMetricMap;
        private final Function<OrderByRowResult<T>, String> orderByContinuationTokenCallback;
        private volatile FeedResponse<OrderByRowResult<T>> previousPage;

        public ItemToPageTransformer(
                RequestChargeTracker tracker,
                int maxPageSize,
                ConcurrentMap<String, QueryMetrics> queryMetricsMap,
                Function<OrderByRowResult<T>, String> orderByContinuationTokenCallback) {
            this.tracker = tracker;
            this.maxPageSize = maxPageSize > 0 ? maxPageSize : DEFAULT_PAGE_SIZE;
            this.queryMetricMap = queryMetricsMap;
            this.orderByContinuationTokenCallback = orderByContinuationTokenCallback;
            this.previousPage = null;
        }

        private static Map<String, String> headerResponse(
                double requestCharge) {
            return Utils.immutableMapOf(HttpConstants.HttpHeaders.REQUEST_CHARGE,
                    String.valueOf(requestCharge));
        }

        private FeedResponse<OrderByRowResult<T>> addOrderByContinuationToken(
                FeedResponse<OrderByRowResult<T>> page,
                String orderByContinuationToken) {
            Map<String, String> headers = new HashMap<>(page.getResponseHeaders());
            headers.put(HttpConstants.HttpHeaders.CONTINUATION,
                    orderByContinuationToken);
            return BridgeInternal.createFeedResponseWithQueryMetrics(page.getResults(),
                    headers,
                    page.getQueryMetrics());
        }

        @Override
        public Observable<FeedResponse<T>> call(
                Observable<OrderByRowResult<T>> source) {
            return source
                    // .windows: creates an observable of observable where inner observable
                    // emits max maxPageSize elements
                    .window(maxPageSize).map(o -> o.toList())
                    // flattens the observable<Observable<List<OrderByRowResult<T>>>> to
                    // Observable<List<OrderByRowResult<T>>>
                    .flatMap(resultListObs -> resultListObs,
                            1)
                    // translates Observable<List<OrderByRowResult<T>>> to
                    // Observable<FeedResponsePage<OrderByRowResult<T>>>>
                    .map(orderByRowResults -> {
                        // construct a page from result with request charge
                        FeedResponse<OrderByRowResult<T>> feedResponse = BridgeInternal.createFeedResponse(
                                orderByRowResults,
                                headerResponse(tracker.getAndResetCharge()));
                        if (!queryMetricMap.isEmpty()) {
                            for (String key : queryMetricMap.keySet()) {
                                BridgeInternal.putQueryMetricsIntoMap(feedResponse,
                                        key,
                                        queryMetricMap.get(key));
                            }
                        }
                        return feedResponse;
                    })
                    // Emit an empty page so the downstream observables know when there are no more
                    // results.
                    .concatWith(Observable.defer(() -> {
                        return Observable.just(BridgeInternal.createFeedResponse(Utils.immutableListOf(),
                                null));
                    }))
                    // Create pairs from the stream to allow the observables downstream to "peek"
                    // 1, 2, 3, null -> (null, 1), (1, 2), (2, 3), (3, null)
                    .map(orderByRowResults -> {
                        ImmutablePair<FeedResponse<OrderByRowResult<T>>, FeedResponse<OrderByRowResult<T>>> previousCurrent = new ImmutablePair<FeedResponse<OrderByRowResult<T>>, FeedResponse<OrderByRowResult<T>>>(
                                this.previousPage,
                                orderByRowResults);
                        this.previousPage = orderByRowResults;
                        return previousCurrent;
                    })
                    // remove the (null, 1)
                    .skip(1)
                    // Add the continuation token based on the current and next page.
                    .map(currentNext -> {
                        FeedResponse<OrderByRowResult<T>> current = currentNext.left;
                        FeedResponse<OrderByRowResult<T>> next = currentNext.right;

                        FeedResponse<OrderByRowResult<T>> page;
                        if (next.getResults().size() == 0) {
                            // No more pages no send current page with null continuation token
                            page = current;
                            page = this.addOrderByContinuationToken(page,
                                    null);
                        } else {
                            // Give the first page but use the first value in the next page to generate the
                            // continuation token
                            page = current;
                            List<OrderByRowResult<T>> results = next.getResults();
                            OrderByRowResult<T> firstElementInNextPage = results.get(0);
                            String orderByContinuationToken = this.orderByContinuationTokenCallback
                                    .apply(firstElementInNextPage);
                            page = this.addOrderByContinuationToken(page,
                                    orderByContinuationToken);
                        }

                        return page;
                    }).map(feedOfOrderByRowResults -> {
                        // FeedResponse<OrderByRowResult<T>> to FeedResponse<T>
                        List<T> unwrappedResults = new ArrayList<T>();
                        for (OrderByRowResult<T> orderByRowResult : feedOfOrderByRowResults.getResults()) {
                            unwrappedResults.add(orderByRowResult.getPayload());
                        }

                        return BridgeInternal.createFeedResponseWithQueryMetrics(unwrappedResults,
                                feedOfOrderByRowResults.getResponseHeaders(),
                                feedOfOrderByRowResults.getQueryMetrics());
                    }).switchIfEmpty(Observable.defer(() -> {
                        // create an empty page if there is no result
                        return Observable.just(BridgeInternal.createFeedResponse(Utils.immutableListOf(),
                                headerResponse(tracker.getAndResetCharge())));
                    }));
        }
    }

    @Override
    public Observable<FeedResponse<T>> drainAsync(
            int maxPageSize) {
        //// In order to maintain the continuation token for the user we must drain with
        //// a few constraints
        //// 1) We always drain from the partition, which has the highest priority item
        //// first
        //// 2) If multiple partitions have the same priority item then we drain from
        //// the left most first
        //// otherwise we would need to keep track of how many of each item we drained
        //// from each partition
        //// (just like parallel queries).
        //// Visually that look the following case where we have three partitions that
        //// are numbered and store letters.
        //// For teaching purposes I have made each item a tuple of the following form:
        //// <item stored in partition, partition number>
        //// So that duplicates across partitions are distinct, but duplicates within
        //// partitions are indistinguishable.
        //// |-------| |-------| |-------|
        //// | <a,1> | | <a,2> | | <a,3> |
        //// | <a,1> | | <b,2> | | <c,3> |
        //// | <a,1> | | <b,2> | | <c,3> |
        //// | <d,1> | | <c,2> | | <c,3> |
        //// | <d,1> | | <e,2> | | <f,3> |
        //// | <e,1> | | <h,2> | | <j,3> |
        //// | <f,1> | | <i,2> | | <k,3> |
        //// |-------| |-------| |-------|
        //// Now the correct drain order in this case is:
        //// <a,1>,<a,1>,<a,1>,<a,2>,<a,3>,<b,2>,<b,2>,<c,2>,<c,3>,<c,3>,<c,3>,
        //// <d,1>,<d,1>,<e,1>,<e,2>,<f,1>,<f,3>,<h,2>,<i,2>,<j,3>,<k,3>
        //// In more mathematical terms
        //// 1) <x, y> always comes before <z, y> where x < z
        //// 2) <i, j> always come before <i, k> where j < k
        return this.orderByObservable.compose(new ItemToPageTransformer<T>(tracker,
                maxPageSize,
                this.queryMetricMap,
                (
                        orderByRowResult) -> {
                    return this.getContinuationToken(orderByRowResult);
                }));
    }

    @Override
    public Observable<FeedResponse<T>> executeAsync() {
        return drainAsync(feedOptions.getMaxItemCount());
    }

    private String getContinuationToken(
            OrderByRowResult<T> orderByRowResult) {
        // rid
        String rid = orderByRowResult.getResourceId();

        // CompositeContinuationToken
        String backendContinuationToken = orderByRowResult.getSourceBackendContinuationToken();
        Range<String> range = orderByRowResult.getSourcePartitionKeyRange().toRange();

        boolean inclusive = true;
        CompositeContinuationToken compositeContinuationToken = new CompositeContinuationToken(backendContinuationToken,
                range);

        // OrderByItems
        QueryItem[] orderByItems = new QueryItem[orderByRowResult.getOrderByItems().size()];
        orderByRowResult.getOrderByItems().toArray(orderByItems);

        return new OrderByContinuationToken(compositeContinuationToken,
                orderByItems,
                rid,
                inclusive).toJson();
    }

    private final class FormattedFilterInfo {
        private final String filterForRangesLeftOfTheTargetRange;
        private final String filterForTargetRange;
        private final String filterForRangesRightOfTheTargetRange;

        public FormattedFilterInfo(
                String filterForRangesLeftOfTheTargetRange,
                String filterForTargetRange,
                String filterForRangesRightOfTheTargetRange) {
            if (filterForRangesLeftOfTheTargetRange == null) {
                throw new IllegalArgumentException("filterForRangesLeftOfTheTargetRange must not be null.");
            }

            if (filterForTargetRange == null) {
                throw new IllegalArgumentException("filterForTargetRange must not be null.");
            }

            if (filterForRangesRightOfTheTargetRange == null) {
                throw new IllegalArgumentException("filterForRangesRightOfTheTargetRange must not be null.");
            }

            this.filterForRangesLeftOfTheTargetRange = filterForRangesLeftOfTheTargetRange;
            this.filterForTargetRange = filterForTargetRange;
            this.filterForRangesRightOfTheTargetRange = filterForRangesRightOfTheTargetRange;
        }

        /**
         * @return the filterForRangesLeftOfTheTargetRange
         */
        public String getFilterForRangesLeftOfTheTargetRange() {
            return filterForRangesLeftOfTheTargetRange;
        }

        /**
         * @return the filterForTargetRange
         */
        public String getFilterForTargetRange() {
            return filterForTargetRange;
        }

        /**
         * @return the filterForRangesRightOfTheTargetRange
         */
        public String getFilterForRangesRightOfTheTargetRange() {
            return filterForRangesRightOfTheTargetRange;
        }
    }
}
