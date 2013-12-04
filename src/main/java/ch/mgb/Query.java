/*
 * (c) 2013 panter llc, Zurich, Switzerland.
 */
package ch.mgb;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import lombok.Cleanup;

import org.elasticsearch.action.admin.indices.alias.get.IndicesGetAliasesRequest;
import org.elasticsearch.action.admin.indices.alias.get.IndicesGetAliasesResponse;
import org.elasticsearch.action.count.CountResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHitField;
import org.elasticsearch.search.SearchHits;

/**
 * TODO: add type comment.
 * 
 */
public class Query {

  /**
   * @param args
   */
  public static void main(final String[] args) {
    @Cleanup
    final Client client = new TransportClient().addTransportAddress(new InetSocketTransportAddress("localhost", 9300));

    final IndicesGetAliasesResponse get = client.admin().indices().getAliases(new IndicesGetAliasesRequest("index")).actionGet();
    System.out.println(get.getAliases());

    final QueryBuilder query = QueryBuilders.boolQuery().must(QueryBuilders.queryString("alib*").field("v1").field("v2"))
        .must(QueryBuilders.fieldQuery("customer", "Fuidic"));
    // matchPhrasePrefixQuery("_type", "type");
    System.out.println(query.toString());
    final CountResponse response = client.prepareCount("index").setQuery(QueryBuilders.matchAllQuery()).execute().actionGet();
    System.out.println("Count: " + response.getCount());
    //
    final SearchResponse searchResponse = client.prepareSearch("index").setTypes("type").setSearchType(SearchType.QUERY_AND_FETCH).setQuery(query)
        .addFields("v2", "v1").execute().actionGet();
    System.out.println("Took " + searchResponse.getTook());
    System.out.println("Status: " + searchResponse.status());
    final SearchHits hits = searchResponse.getHits();
    System.out.println("Hit-Count: " + hits.getTotalHits());
    for (final SearchHit hit : hits.getHits()) {
      final Map<String, String> map = new HashMap<String, String>();
      final Map<String, SearchHitField> fields = hit.getFields();
      if (fields != null) {
        for (final Entry<String, SearchHitField> searchHitField : fields.entrySet()) {
          map.put(searchHitField.getKey(), searchHitField.getValue().getValue().toString());
        }
      }
      System.out.println(hit.getId());
      System.out.println(map);
    }
  }

}
