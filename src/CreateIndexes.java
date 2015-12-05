import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import org.elasticsearch.index.aliases.IndexAliasesService;

public class CreateIndexes {

//	public static String path_file_kb = "/home/ubunt/input/annotated_nested_core_datagraph.tsv";
//	public static String path_file_kb = "/home/chris88/Documenti/Tesi/componenti/annotated_nested_core_datagraph.tsv";
	
	
	
//	public static String path_file_mapping = "/home/ubuntu/input/title_wikid_mid.txt";
//	public static String path_file_mapping = "/home/chris88/Documenti/Tesi/componenti/title_wikid_mid.txt";
	
//	public static String path_file_redirect = "/home/ubuntu/input/wiki_redirect.txt";
//	public static String path_file_redirect = "/home/chris88/Documenti/Tesi/componenti/wiki_redirect.txt";
	
	public static String path_file_relations_between_types = "/home/ubuntu/input/relations_expected_types_Matteo.tsv";
//	public static String path_file_relations_between_types = "/home/chris88/Documenti/Tesi/componenti/relations_expected_types_Matteo.tsv";
	
	
	public static String path_timestamp = "/home/ubuntu/output/mentions_freebase/timestamp_elasticsearch_relations_between_types.txt";
//	public static String path_timestamp = "/home/chris88/Scrivania/timestamp_elasticsearch_local_03122015.txt";

	
	
	public static void main(String[] args) {

		try {
			BlockingQueue<String> recordBuffer = new LinkedBlockingQueue<String>(1000);
			BlockingQueue<String> responseBuffer = new LinkedBlockingQueue<String>();

			WikiminigIndexes index = new WikiminigIndexes();
			
			BufferedReader reader1 , reader2 ,reader3, reader4 = null;
			
			BufferedWriter writerTimestamp = null;
			int cores = Runtime.getRuntime().availableProcessors();

			
			DateFormat dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
			Date startDate = new Date();
			double startTime = (double)(System.currentTimeMillis()) / 1000;
/*
			//creazione indice per dump KB
			InsertRecordKBThread[] insertRecordKBThreads = new InsertRecordKBThread[cores];
			for(int i=0;i<cores;i++){
				insertRecordKBThreads[i] = new InsertRecordKBThread(recordBuffer, responseBuffer, index);
				insertRecordKBThreads[i].start();
			}


			int counter = 0;
			reader1 = new BufferedReader(new FileReader(new File(path_file_kb)));
			String line = "";
			while((line=reader1.readLine())!=null && counter<10000){
				recordBuffer.put(line);
				counter++;
			}
			recordBuffer.put(Message.FINISHED_PRODUCER);

			int counter_finished_thread = 0;
			while(counter_finished_thread < cores){
				String message = responseBuffer.take();
				if(message.equals(Message.FINISHED_CONSUMER))
					counter_finished_thread++;
			}


			recordBuffer.clear();
			responseBuffer.clear();
			reader1.close();
			
			
			
			//creazione indice wiki -> mid

			InsertWikidMidThread[] insertRecordThreads = new InsertWikidMidThread[cores];
			for(int i=0;i<cores;i++){
				insertRecordThreads[i] = new InsertWikidMidThread(recordBuffer, responseBuffer, index);
				insertRecordThreads[i].start();
			}


			counter = 0;
			reader2 = new BufferedReader(new FileReader(new File(path_file_mapping)));
			line = "";
			while((line=reader2.readLine())!=null && counter < 10000){
				recordBuffer.put(line);
				counter++;
			}
			recordBuffer.put(Message.FINISHED_PRODUCER);
			reader2.close();

			counter_finished_thread = 0;
			while(counter_finished_thread<cores){
				String message = responseBuffer.take();
				if(message.equals(Message.FINISHED_CONSUMER))
					counter_finished_thread++;
			}
			

			recordBuffer.clear();
			responseBuffer.clear();



			//creazione indice redirect
			InsertRedirectWikidThread[] insertRedirectWikidThreads = new InsertRedirectWikidThread[cores];
			for(int i=0;i<cores;i++){
				insertRedirectWikidThreads[i] = new InsertRedirectWikidThread(recordBuffer, responseBuffer, index);
				insertRedirectWikidThreads[i].start();
			}

			counter = 0;
			reader3 = new BufferedReader(new FileReader(new File(path_file_redirect)));
			while((line=reader3.readLine())!=null && counter<10000){
				recordBuffer.put(line);
				counter++;
			}
			recordBuffer.put(Message.FINISHED_PRODUCER);
			reader3.close();

			counter_finished_thread = 0;
			while(counter_finished_thread<cores){
				String message = responseBuffer.take();
				if(message.equals(Message.FINISHED_CONSUMER))
					counter_finished_thread++;
			}


			recordBuffer.clear();
			responseBuffer.clear();
*/


			//creazione indice relation between types
			InsertRecordRelationBetweenTypesThread[] insertRelationBetweenTypesThreads = new InsertRecordRelationBetweenTypesThread[cores];
			for(int i=0;i<cores;i++){
				insertRelationBetweenTypesThreads[i] = new InsertRecordRelationBetweenTypesThread(recordBuffer, responseBuffer, index);
				insertRelationBetweenTypesThreads[i].start();
			}

			String line = "";
			reader4 = new BufferedReader(new FileReader(new File(path_file_relations_between_types)));
			while((line=reader4.readLine())!=null ){
				recordBuffer.put(line);
			}
			recordBuffer.put(Message.FINISHED_PRODUCER);
			reader4.close();

			int counter_finished_thread = 0;
			while(counter_finished_thread<cores){
				String message = responseBuffer.take();
				if(message.equals(Message.FINISHED_CONSUMER))
					counter_finished_thread++;
			}

			double endTime = (double)(System.currentTimeMillis()) / 1000;
			Date endDate = new Date();
			double totalTime = endTime-startTime;

			writerTimestamp = new BufferedWriter(new FileWriter(new File(path_timestamp)));
			writerTimestamp.write("Start at: "+dateFormat.format(startDate)+"\n");
			writerTimestamp.write("End at: "+dateFormat.format(endDate)+"\n");
			writerTimestamp.write("Total time: " +totalTime+"\n");
			writerTimestamp.close();

			index.close();


		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}

	}

}
