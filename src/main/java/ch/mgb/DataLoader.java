/*
 * (c) 2013 panter llc, Zurich, Switzerland.
 */
package ch.mgb;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Deque;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import lombok.Cleanup;

import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.count.CountResponse;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.query.QueryBuilders;

/**
 * TODO: add type comment.
 * 
 */
public class DataLoader {

    /**
     * @param args
     */
    public static void main(final String[] args) {
        @Cleanup
        final Client client = new TransportClient().addTransportAddress(new InetSocketTransportAddress("localhost", 9300));
        try {
            System.out.println("Count before delete: " + countAll(client).getCount());
            client.prepareDeleteByQuery("index").setQuery(QueryBuilders.matchAllQuery()).execute().actionGet();
            System.out.println("Count after delete: " + countAll(client).getCount());
        } catch (final Throwable t) {
            System.err.println("Probleme beim löschen der bestehenden Einträge. Ev. existiert Index noch nicht");
            t.printStackTrace();
        }

        final Random random = new Random();
        final List<String> customer = Arrays.asList("Acndmo", "Fuidic", "Unjcyi", "Cqcrdfrc");
        final ExecutorService threadPool = Executors.newFixedThreadPool(10);
        final ArrayList<Runnable> insertTasks = new ArrayList<Runnable>();
        final Deque<ActionFuture<BulkResponse>> futures = new LinkedList<ActionFuture<BulkResponse>>();
        final long startTime = System.currentTimeMillis();
        final int recordCount = 1000000;
        try {
            for (int i = 0; i < recordCount;) {
                final BulkRequestBuilder bulk = client.prepareBulk();
                for (int j = 0; j < 10 && i < recordCount; j++, i++) {

                    final String id = UUID.randomUUID().toString();
                    final String v1 = generateValue(random) + " " + generateValue(random);
                    final String v2 = generateValue(random);
                    final String customerName = customer.get(random.nextInt(customer.size()));
                    final XContentBuilder source = XContentFactory.jsonBuilder().startObject().field("v1", v1).field("v2", v2)
                            .field("customer", customerName).endObject();
                    bulk.add(new IndexRequestBuilder(client).setIndex("index").setType("type").setId(id).setSource(source));
                }
                while (futures.size() > 4) {
                    waitToFuture(futures.removeFirst());

                }
                // bulk.execute().actionGet();
                futures.addLast(bulk.execute());

                // final Runnable task = new Runnable() {
                // public void run() {
                // try {
                //
                // final IndexRequestBuilder index = client.prepareIndex("index", "type", id);
                // index.setSource(source);
                // index.execute().actionGet();
                // } catch (final ElasticSearchException e) {
                // // TODO Auto-generated catch block
                // e.printStackTrace();
                // } catch (final IOException e) {
                // // TODO Auto-generated catch block
                // e.printStackTrace();
                // }
                // }
                // };
                // insertTasks.add(task);
            }
            for (final ActionFuture<BulkResponse> actionFuture : futures) {
                waitToFuture(actionFuture);
            }
            // System.out.println("Data prepared");
            for (final Runnable runnable : insertTasks) {
                threadPool.submit(runnable);
            }
            threadPool.shutdown();
            threadPool.awaitTermination(5, TimeUnit.MINUTES);
            final long endTime = System.currentTimeMillis();
            final double insertTime = (endTime - startTime) / 1000.0;
            System.out.println("Count after insert: " + countAll(client).getCount());
            System.out.println(recordCount + " inserts in " + insertTime + " s -> " + (recordCount / insertTime) + " insert/s ");
        } catch (final Throwable t) {
            t.printStackTrace();
        }
    }

    private static CountResponse countAll(final Client client) {
        final CountResponse countResponse = client.prepareCount("index").setQuery(QueryBuilders.matchAllQuery()).execute().actionGet();
        return countResponse;
    }

    /**
     * @param random
     * @return
     */
    private static String generateValue(final Random random) {
        final StringBuffer sb = new StringBuffer();
        final int length = random.nextInt(5) + 5;
        final boolean upcase = random.nextBoolean();
        for (int i = 0; i < length; i++) {
            final int characterIndex = random.nextInt('z' - 'a');
            if (upcase && i == 0) {
                sb.append((char) ('A' + characterIndex));
            } else {
                sb.append((char) ('a' + characterIndex));
            }
        }
        return sb.toString();
    }

    private static void waitToFuture(final ActionFuture<BulkResponse> future) {
        final BulkResponse bulkResponse = future.actionGet();
        if (bulkResponse.hasFailures()) {
            System.out.println(bulkResponse.buildFailureMessage());
        }
    }

}
