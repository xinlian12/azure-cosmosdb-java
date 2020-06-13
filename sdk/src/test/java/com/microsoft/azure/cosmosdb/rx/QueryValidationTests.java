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
package com.microsoft.azure.cosmosdb.rx;

import com.microsoft.azure.cosmosdb.Database;
import com.microsoft.azure.cosmosdb.Document;
import com.microsoft.azure.cosmosdb.DocumentCollection;
import com.microsoft.azure.cosmosdb.FeedOptions;
import com.microsoft.azure.cosmosdb.FeedResponse;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Factory;
import org.testng.annotations.Test;
import rx.Observable;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

public class QueryValidationTests extends TestSuiteBase {
    private static final int NUM_DOCUMENTS = 10;
    private static final int PARTITION1_NUM = 4;
    private static final int PARTITION2_NUM = 5;
    private Random random;
    private Database createdDatabase;
    private DocumentCollection createdCollection;
    private List<Document> createdDocuments = new ArrayList<>();
    private Random randomGenerator = new Random();


    private AsyncDocumentClient client;

    @Factory(dataProvider = "clientBuildersWithDirectSession")
    public QueryValidationTests(AsyncDocumentClient.Builder clientBuilder) {
        super(clientBuilder);
        random = new Random();
    }

    @Test(groups = {"simple"}, timeOut = TIMEOUT)
    public void orderByQuery() {
        /*
        The idea here is to query documents in pages, query all the documents(with pagesize as num_documents and compare
         the results.
         */
        String query = "select * from c order by c.propInt ASC";
        List<Document> documentsPaged = queryWithContinuationTokens(query, 100);

        List<Document> allDocuments = queryWithContinuationTokens(query, NUM_DOCUMENTS);

        Comparator<Integer> validatorComparator = Comparator.nullsFirst(Comparator.<Integer>naturalOrder());
        List<String> expectedResourceIds = sortDocumentsAndCollectResourceIds(createdDocuments,
                                                                              "propInt",
                                                                              d -> d.getInt("propInt"),
                                                                              validatorComparator);

        List<String> docIds1 = documentsPaged.stream().map(Document::getId).collect(Collectors.toList());
        List<String> docIds2 = allDocuments.stream().map(Document::getId).collect(Collectors.toList());

        assertThat(docIds2).containsExactlyInAnyOrderElementsOf(expectedResourceIds);
        assertThat(docIds1).containsExactlyElementsOf(docIds2);

    }

    @Test(groups = {"simple"}, timeOut = TIMEOUT)
    public void orderByQueryForAdobe() {
        /*
        The idea here is to query documents in pages, query all the documents(with pagesize as num_documents and compare
         the results.
         */
        String query = "SELECT * FROM c WHERE c.mypk IN ('6b36e130-c5d7-11e9-949c-0da8d50fcac1','7dad4e50-dbc9-11e9-aeff-db213dcc941f','ade028c0-f174-11e9-b395-132c994d1db1','7f550ce0-3608-11ea-a3dc-33f5bc717ff3','8e3dca60-6daf-11ea-aee0-4db17ee10871') ORDER BY c.name DESC";
        String responseContinuationToken = null;
        List<Document> totalDocuments = new ArrayList<>();

        do {
            int requiredPageSize = 6;
           // boolean shouldContinue = true;

            FeedResponse<Document> documentsPaged = singlePageQueryWithContinuationToken(query, requiredPageSize, responseContinuationToken);
            List<Document> reformattedDocuments = Optional.ofNullable(documentsPaged.getResults())
                    .orElse(Collections.emptyList())
                    .stream()
                    .collect(Collectors.toList());
            totalDocuments.addAll(reformattedDocuments);

            responseContinuationToken = documentsPaged.getResponseContinuation();
            logger.info("result {} --- {}", documentsPaged.getResults(), documentsPaged.getResponseContinuation());


//            while(shouldContinue) {
//                FeedResponse<Document> documentsPaged = singlePageQueryWithContinuationToken(query, requiredPageSize, responseContinuationToken);
//                List<Document> reformattedDocuments = Optional.ofNullable(documentsPaged.getResults())
//                        .orElse(Collections.emptyList())
//                        .stream()
//                        .collect(Collectors.toList());
//                totalDocuments.addAll(reformattedDocuments);
//
//                responseContinuationToken = documentsPaged.getResponseContinuation();
//                System.out.println(responseContinuationToken);
//                if (requiredPageSize > reformattedDocuments.size() && !Strings.isNullOrEmpty(responseContinuationToken)) {
//                    requiredPageSize = requiredPageSize - reformattedDocuments.size();
//                    logger.info("balance item size = [{}] required to be fetched from next partition", requiredPageSize);
//                } else {
//                    shouldContinue = false;
//                }
//            }
        } while (responseContinuationToken != null);


          assertThat(totalDocuments.size()).isEqualTo(PARTITION1_NUM + PARTITION2_NUM);

    }

    private FeedResponse<Document> singlePageQueryWithContinuationToken(String query, int pageSize, String continuationToken) {
        logger.info("querying: " + query);

        FeedOptions options = new FeedOptions();
        options.setEmitVerboseTracesInQuery(true);
        options.setMaxItemCount(pageSize);
        options.setEnableCrossPartitionQuery(true);

        //options.setMaxDegreeOfParallelism(10);
        options.setRequestContinuation(continuationToken);
        Observable<FeedResponse<Document>> queryObservable = client.queryDocuments(getCollectionLink(), query,
                options);

        // Observable to Iterator
        final Iterator<FeedResponse<Document>> it = queryObservable.toBlocking().getIterator();
        //final FeedResponse<Document> currentPage = queryObservable.first().toBlocking().single();
//        while(it.hasNext()) {
//            final FeedResponse<Document> currentPage = it.next();
//            logger.info("results: {} --- {}", currentPage.getResults(), currentPage.getResponseContinuation());
//        }
        final FeedResponse<Document> currentPage = it.next();
        return currentPage;
    }

    private List<Document> queryWithContinuationTokens(String query, int pageSize) {
        logger.info("querying: " + query);
        String requestContinuation = null;

        List<String> continuationTokens = new ArrayList<String>();
        List<Document> receivedDocuments = new ArrayList<Document>();
        do {
            FeedOptions options = new FeedOptions();
            options.setEmitVerboseTracesInQuery(true);

            options.setMaxItemCount(pageSize);
            options.setEnableCrossPartitionQuery(true);

            //options.setMaxDegreeOfParallelism(10);
            options.setRequestContinuation(requestContinuation);
            Observable<FeedResponse<Document>> queryObservable = client.queryDocuments(getCollectionLink(), query,
                                                                                       options);

            FeedResponse<Document> firstPage = queryObservable.first().toBlocking().single();
            requestContinuation = firstPage.getResponseContinuation();
            receivedDocuments.addAll(firstPage.getResults());
            continuationTokens.add(requestContinuation);
        } while (requestContinuation != null);

        return receivedDocuments;
    }

    @BeforeMethod(groups = {"simple"})
    public void beforeMethod() throws Exception {
        // add a cool off time
        TimeUnit.SECONDS.sleep(10);
    }

    @BeforeClass(groups = {"simple"}, timeOut = SETUP_TIMEOUT)
    public void beforeClass() throws Exception {
        client = this.clientBuilder().build();
        createdDatabase = SHARED_DATABASE;
        createdCollection = SHARED_MULTI_PARTITION_COLLECTION;
        truncateCollection(SHARED_MULTI_PARTITION_COLLECTION);

        List<Document> documentsToInsert = new ArrayList<>();

//        for (int i = 0; i < NUM_DOCUMENTS; i++) {
//            documentsToInsert.add(getDocumentDefinition(UUID.randomUUID().toString()));
//        }

        for (int i = 0; i < PARTITION1_NUM; i++) {
            documentsToInsert.add(getDocumentDefinition(UUID.randomUUID().toString(), "6b36e130-c5d7-11e9-949c-0da8d50fcac1"));
        }

        for (int i = 0; i < PARTITION2_NUM; i++) {
            documentsToInsert.add(getDocumentDefinition(UUID.randomUUID().toString(), "7dad4e50-dbc9-11e9-aeff-db213dcc941f"));
        }

        createdDocuments = bulkInsertBlocking(client, getCollectionLink(), documentsToInsert);

        int numberOfPartitions = client
                                         .readPartitionKeyRanges(getCollectionLink(), null)
                                         .flatMap(p -> Observable.from(p.getResults())).toList().toBlocking().single()
                                         .size();

        waitIfNeededForReplicasToCatchUp(this.clientBuilder());
    }

    private Document getDocumentDefinition(String documentId, String partitionKeyId) {
        String sampleDocument = "{ " +
                "            \"createdAt\": 1590495459390," +
                "            \"updatedAt\": 1590495459390," +
                "            \"createdBy\": \"ac@AdobeID\"," +
                "            \"updatedBy\": \"ac@AdobeID\"," +
                "            \"createdClient\": \"ac\"," +
                "            \"updatedClient\": \"ac\"," +
                "            \"mypk\": \"" + partitionKeyId + "\"," +
                "            \"sandName\": \"prod\"," +
                "            \"id\": \"" + documentId + "\"," +
                "            \"name\": \"IT-S3 Flow\"," +
                "            \"description\": \"Flow created during integration tests.\"," +
                "            \"spec\": { " +
                "                \"id\": \"feae26d8-4968-4074-b2ca-d182fc050729\"," +
                "                \"version\": \"1.0\"" +
                "            }," +
                "            \"state\": \"enabled\"," +
                "            \"version\": \"8002d1c0-0000-0200-0000-5ecd08e30000\"," +
                "            \"etag\": \"8002d1c0-0000-0200-0000-5ecd08e30000\"," +
                "            \"sids\": [" +
                "                \"16849dc3-522f-41b2-849d-c3522f91b29d\"" +
                "            ]," +
                "            \"tids\": [" +
                "                \"737c65a8-b272-4465-bc65-a8b272646558\"" +
                "            ],\n" +
                "            \"attri\": {" +
                "                \"scs\": [" +
                "                    {\n" +
                "                        \"id\": \"16849dc3-522f-41b2-849d-c3522f91b29d\"," +
                "                        \"connectionSpec\": {" +
                "                            \"id\": \"ecadc60c-7455-4d87-84dc-2a0e293d997b\"," +
                "                            \"version\": \"1.0\"" +
                "                        }" +
                "                    }" +
                "                ]," +
                "                \"tcs\": [" +
                "                    {" +
                "                        \"id\": \"737c65a8-b272-4465-bc65-a8b272646558\"," +
                "                        \"connectionSpec\": {" +
                "                            \"id\": \"c604ff05-7f1a-43c0-8e18-33bf874cb11c\"," +
                "                            \"version\": \"1.0\"" +
                "                        }" +
                "                    }" +
                "                ]" +
                "            }," +
                "            \"params\": {" +
                "                \"backfill\": true," +
                "                \"startTime\": \"1573891635\"," +
                "                \"interval\": 527040," +
                "                \"frequency\": \"minutes\"" +
                "            }," +
                "            \"formations\": [" +
                "                {" +
                "                    \"name\": \"Copy\"," +
                "                    \"params\": {}" +
                "                }," +
                "                {" +
                "                    \"name\": \"Mapping\"," +
                "                    \"params\": {" +
                "                        \"mappingId\": \"debfc0a0f41646e0bf6ad6a3636010d2\"," +
                "                        \"mappingVersion\": \"0\"" +
                "                    }" +
                "                }" +
                "            ]," +
                "            \"abc\": \"/flows/499ec129-e60c-4c6f-9ec1-29e60cac6f24/runs\"" +
                "        }";
//        Document doc = new Document(String.format("{ "
//                                                          + "\"id\": \"%s\", "
//                                                          + "\"pkey\": \"%s\", "
//                                                          + "\"propInt\": %s, "
//                                                          + "\"sgmts\": [[6519456, 1471916863], [2498434, 1455671440]]"
//                                                          + "}"
//                , documentId, uuid, random.nextInt(NUM_DOCUMENTS/2)));
        // Doing NUM_DOCUMENTS/2 just to ensure there will be good number of repetetions.
        Document doc = new Document(sampleDocument);
        return doc;
    }

    private Document getDocumentDefinition(String documentId) {
        ArrayList<String> partitionKeyList = new ArrayList<String>();
        partitionKeyList.add("6b36e130-c5d7-11e9-949c-0da8d50fcac1");
        partitionKeyList.add("7dad4e50-dbc9-11e9-aeff-db213dcc941f");
        partitionKeyList.add("ade028c0-f174-11e9-b395-132c994d1db1");
        partitionKeyList.add("7f550ce0-3608-11ea-a3dc-33f5bc717ff3");
        partitionKeyList.add("8e3dca60-6daf-11ea-aee0-4db17ee10871");

        int index = randomGenerator.nextInt(partitionKeyList.size());
        String uuid = partitionKeyList.get(index);
//         String uuid = UUID.randomUUID().toString();
        String sampleDocument = "{ " +
                "            \"createdAt\": 1590495459390," +
                "            \"updatedAt\": 1590495459390," +
                "            \"createdBy\": \"ac@AdobeID\"," +
                "            \"updatedBy\": \"ac@AdobeID\"," +
                "            \"createdClient\": \"ac\"," +
                "            \"updatedClient\": \"ac\"," +
                "            \"mypk\": \"" + uuid + "\"," +
                "            \"sandName\": \"prod\"," +
                "            \"id\": \"" + documentId + "\"," +
                "            \"name\": \"IT-S3 Flow\"," +
                "            \"description\": \"Flow created during integration tests.\"," +
                "            \"spec\": { " +
                "                \"id\": \"feae26d8-4968-4074-b2ca-d182fc050729\"," +
                "                \"version\": \"1.0\"" +
                "            }," +
                "            \"state\": \"enabled\"," +
                "            \"version\": \"8002d1c0-0000-0200-0000-5ecd08e30000\"," +
                "            \"etag\": \"8002d1c0-0000-0200-0000-5ecd08e30000\"," +
                "            \"sids\": [" +
                "                \"16849dc3-522f-41b2-849d-c3522f91b29d\"" +
                "            ]," +
                "            \"tids\": [" +
                "                \"737c65a8-b272-4465-bc65-a8b272646558\"" +
                "            ],\n" +
                "            \"attri\": {" +
                "                \"scs\": [" +
                "                    {\n" +
                "                        \"id\": \"16849dc3-522f-41b2-849d-c3522f91b29d\"," +
                "                        \"connectionSpec\": {" +
                "                            \"id\": \"ecadc60c-7455-4d87-84dc-2a0e293d997b\"," +
                "                            \"version\": \"1.0\"" +
                "                        }" +
                "                    }" +
                "                ]," +
                "                \"tcs\": [" +
                "                    {" +
                "                        \"id\": \"737c65a8-b272-4465-bc65-a8b272646558\"," +
                "                        \"connectionSpec\": {" +
                "                            \"id\": \"c604ff05-7f1a-43c0-8e18-33bf874cb11c\"," +
                "                            \"version\": \"1.0\"" +
                "                        }" +
                "                    }" +
                "                ]" +
                "            }," +
                "            \"params\": {" +
                "                \"backfill\": true," +
                "                \"startTime\": \"1573891635\"," +
                "                \"interval\": 527040," +
                "                \"frequency\": \"minutes\"" +
                "            }," +
                "            \"formations\": [" +
                "                {" +
                "                    \"name\": \"Copy\"," +
                "                    \"params\": {}" +
                "                }," +
                "                {" +
                "                    \"name\": \"Mapping\"," +
                "                    \"params\": {" +
                "                        \"mappingId\": \"debfc0a0f41646e0bf6ad6a3636010d2\"," +
                "                        \"mappingVersion\": \"0\"" +
                "                    }" +
                "                }" +
                "            ]," +
                "            \"abc\": \"/flows/499ec129-e60c-4c6f-9ec1-29e60cac6f24/runs\"" +
                "        }";
//        Document doc = new Document(String.format("{ "
//                                                          + "\"id\": \"%s\", "
//                                                          + "\"pkey\": \"%s\", "
//                                                          + "\"propInt\": %s, "
//                                                          + "\"sgmts\": [[6519456, 1471916863], [2498434, 1455671440]]"
//                                                          + "}"
//                , documentId, uuid, random.nextInt(NUM_DOCUMENTS/2)));
        // Doing NUM_DOCUMENTS/2 just to ensure there will be good number of repetetions.
        Document doc = new Document(sampleDocument);
        return doc;
    }

    public String getCollectionLink() {
        return Utils.getCollectionNameLink(createdDatabase.getId(), createdCollection.getId());
    }

    private <T> List<String> sortDocumentsAndCollectResourceIds(
            List<Document> createdDocuments, String propName,
            Function<Document, T> extractProp, Comparator<T> comparer) {
        return createdDocuments.stream()
                       .filter(d -> d.getHashMap().containsKey(propName)) // removes undefined
                       .sorted((d1, d2) -> comparer.compare(extractProp.apply(d1), extractProp.apply(d2)))
                       .map(d -> d.getId()).collect(Collectors.toList());
    }

}
