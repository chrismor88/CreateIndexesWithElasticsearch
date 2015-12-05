import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.Map;

import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.node.Node;
import org.elasticsearch.node.NodeBuilder;

public class TestCreateIndex {
	

	public static void main(String[] args) {
		Client client = createConnection();
		client.prepareIndex("test_index","index").setSource(putJsonDocument("m.3290efwed", "B")).execute().actionGet();
		client.prepareIndex("test_index","index").setSource(putJsonDocument("m.3290efasa", "A")).execute().actionGet();
		client.prepareIndex("test_index","index").setSource(putJsonDocument("m.3290efwed", "C")).execute().actionGet();
		client.prepareIndex("test_index","index").setSource(putJsonDocument("m.3290efwed", "m.3290efasa")).execute().actionGet();
		
	}
	
	
	private static Map<String, Object> putJsonDocument(String field, String value){
		Map<String, Object> jsonDocument = new HashMap<String, Object>();
		jsonDocument.put("first", field);
		jsonDocument.put("second", value);
		return jsonDocument;
	}
	
	

	private static Client createConnection() {
		Client client = null;
		Settings settings = Settings.settingsBuilder().put("node.name", "node-1").build();
		NodeBuilder builder = new NodeBuilder();
		builder.settings(settings);
		Node node = builder.node();
		try {
			client = TransportClient.builder().build()
			        .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("127.0.0.1"), 9300));
		} catch (UnknownHostException e) {
			e.printStackTrace();
		}
		
		return client;
	}
	
}
