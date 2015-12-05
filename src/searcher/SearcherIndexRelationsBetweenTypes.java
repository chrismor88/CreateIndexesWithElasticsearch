package searcher;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.Client;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;

public class SearcherIndexRelationsBetweenTypes {
	
	private final static String index_name = "index_relation_between_types";
	private final static String index_type = "relation";

	public SearcherIndexRelationsBetweenTypes() {

	}


	public List<String> searchRelationsFromTypes(Client client, String type1, String type2){
		
		List<String> relationsFound = new LinkedList<String>();
		
		QueryBuilder queryBuilder = QueryBuilders.boolQuery()
				.must(QueryBuilders.matchQuery("type1",type1)).must(QueryBuilders.matchQuery("type2",type2));

		SearchResponse response = client.prepareSearch(index_name)
				.setTypes(index_type)
				.setSearchType(SearchType.DFS_QUERY_AND_FETCH)
				.setQuery(queryBuilder)
				.execute().actionGet();

		SearchHit[] results = response.getHits().getHits();
		for (SearchHit hit : results) {
			relationsFound.add((String)hit.getSource().get("predicate"));
		}

		return relationsFound;

	}
}