import java.util.concurrent.BlockingQueue;

public class InsertRedirectWikidThread extends Thread {

	private BlockingQueue<String> inputBuffer; //buffer in cui vengono trasmessi e prelevati le stringhe di tipo json
	private BlockingQueue<String> outputBuffer; //buffer per comunicare al produttore la terminazione dei consumatori
	private WikiminigIndexes index;

	public InsertRedirectWikidThread(BlockingQueue<String> inputBuffer, BlockingQueue<String> outputBuffer, WikiminigIndexes index) {
		this.inputBuffer = inputBuffer;
		this.outputBuffer = outputBuffer;
		this.index = index;
	}

	@Override
	public void run() {
		super.run();

		while(true){
			try {
				String message = inputBuffer.take();
				if(!message.equals(Message.FINISHED_PRODUCER)){
					String[] fieldsMessage = message.split("\t");
					index.insertRecordIntoIndexRedirect(fieldsMessage[0],fieldsMessage[1]);
				}
				else{
					inputBuffer.put(message);
					outputBuffer.put(Message.FINISHED_CONSUMER);
					break;
				}

			} catch (InterruptedException e) {
				e.printStackTrace();
			}

		}
	}
}
