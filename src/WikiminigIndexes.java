import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.Map;

import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.node.Node;
import org.elasticsearch.node.NodeBuilder;

public class WikiminigIndexes {

	private Node node;
	private Client client;
	private static final String INDEX_WIKID_MID = "index_mapping_wikid_mid";
	private static final String TYPE1 = "wikid_mid";
	private static final String INDEX_REDIRECT = "index_redirect";
	private static final String TYPE2 = "redirect_wikid";
	private static final String INDEX_KB = "index_kb";
	private static final String TYPE3 = "kb";
	private static final String INDEX_RELATION_BETWEEN_TYPES = "index_relation_between_types";
	private static final String TYPE4 = "relation";
	
	
	public WikiminigIndexes() {
		Settings settings = Settings.settingsBuilder().put("node.name", "node-1").build();
		NodeBuilder builder = new NodeBuilder();
		builder.settings(settings);
		node = builder.node();
		try {
			client = TransportClient.builder().build()
			        .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("127.0.0.1"), 9300));
		} catch (UnknownHostException e) {
			e.printStackTrace();
		}
	}
	
	public WikiminigIndexes(String remoteIP) {
		Settings settings = Settings.settingsBuilder().put("node.name", "node-1").build();
		NodeBuilder builder = new NodeBuilder();
		builder.settings(settings);
		node = builder.node();
		try {
			client = TransportClient.builder().build()
			        .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName(remoteIP), 9300));
		} catch (UnknownHostException e) {
			e.printStackTrace();
		}
	}
	
	
	
	
	public void insertRecordIntoIndexWikidMid(String wikid, String mid){
		client.prepareIndex(INDEX_WIKID_MID,TYPE1).setSource(putJsonWikidMid(wikid, mid)).execute().actionGet();
	}
	
	public void insertRecordIntoIndexRedirect(String redirect, String wikid){
		client.prepareIndex(INDEX_REDIRECT,TYPE2).setSource(putJsonRedirectWikid(redirect, wikid)).execute().actionGet();
	}
	
	public void insertRecordKB(String mid1,String mid2,String predicate,String title1, String title2, String types1, String types2){
		client.prepareIndex(INDEX_KB, TYPE3).setSource(putJsonKB(mid1, mid2, predicate, title1, title2, types1, types2)).execute().actionGet();
	}
	
	public void insertRecordRelationBetweenTypes(String type1, String predicate, String type2) {
		client.prepareIndex(INDEX_RELATION_BETWEEN_TYPES, TYPE4).setSource(putJsonRelationBetweenTypes(type1, predicate, type2)).execute().actionGet();
		
	}
	
	public void close(){
		node.close();
	}
	
	
	private Map<String, Object> putJsonWikidMid(String wikid, String mid){
		Map<String, Object> jsonDocument = new HashMap<String, Object>();
		jsonDocument.put("wikid", wikid);
		jsonDocument.put("mid", mid);
		return jsonDocument;
	}
	
	
	private Map<String, Object> putJsonRedirectWikid(String redirect, String wikid){
		Map<String, Object> jsonDocument = new HashMap<String, Object>();
		jsonDocument.put("redirect", redirect);
		jsonDocument.put("wikid", wikid);
		return jsonDocument;
	}
	
	private Map<String, Object> putJsonKB(String mid1,String mid2,String predicate,String title1, String title2, String types1, String types2){
		Map<String, Object> jsonDocument = new HashMap<String, Object>();
		jsonDocument.put("mid1", mid1);
		jsonDocument.put("mid2", mid2);
		jsonDocument.put("predicate", predicate);
		jsonDocument.put("title1", title1);
		jsonDocument.put("title2", title2);
		jsonDocument.put("types1", types1);
		jsonDocument.put("types2", types2);
		return jsonDocument;
	}

	private Map<String, Object> putJsonRelationBetweenTypes(String type1, String predicate,String type2){
		Map<String, Object> jsonDocument = new HashMap<String, Object>();
		jsonDocument.put("type1", type1);
		jsonDocument.put("predicate", predicate);
		jsonDocument.put("type2", type2);
		return jsonDocument;
	}

	public void setKeywordAnalyzer() {
		try {
			XContentBuilder settingsBuilder = XContentFactory.jsonBuilder()
			        .startObject()
			            .startObject("analysis")
			            	.startObject("analyzer")
			            		.startObject("default")
			            			.field("type","keyword")
			            		.endObject()
			            	.endObject()
						.endObject()
					.endObject();
			
			
			client.admin().indices()
			.prepareCreate(INDEX_KB).setSettings(settingsBuilder).get();
			
			client.admin().indices()
			.prepareCreate(INDEX_WIKID_MID).setSettings(settingsBuilder).get();
			
			client.admin().indices()
			.prepareCreate(INDEX_REDIRECT).setSettings(settingsBuilder).get();
			
			client.admin().indices()
			.prepareCreate(INDEX_RELATION_BETWEEN_TYPES).setSettings(settingsBuilder).get();
			
			
			
		} catch (IOException e) {
			e.printStackTrace();
		}	
		            	
	}	            	
		            	
		            	
	
	
	
}
