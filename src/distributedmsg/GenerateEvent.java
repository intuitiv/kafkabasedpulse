package distributedmsg;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Arrays;
import java.util.Date;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import distributedmsg.PulseRecord.EVENTTYPE;

public class GenerateEvent {
	public static void main(String[] args) throws IOException {
		BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
		String input = br.readLine();
		String[] list = input.split(",");
		for (int i = 1; i < list.length; i++) {
			ProcessPulse pulseProc = new ProcessPulse(list[i], list[0]);
			pulseProc.start();
		}
		GeneratePulse pulseGen = new GeneratePulse();
		pulseGen.start();
		while (true) {

		}
	}
}

class ProcessPulse extends Thread {

	String type;
	String id;

	public ProcessPulse(String type, String groupID) {
		this.type = type;
		id = groupID;
	}

	@Override
	public void run() {
		Properties props = new Properties();

		props.put("bootstrap.servers", "localhost:9092");
		props.put("group.id", id);
		props.put("consumer.id", id);
		props.put("enable.auto.commit", "true");
		props.put("auto.commit.interval.ms", "1000");
		props.put("session.timeout.ms", "30000");
		props.put("key.deserializer", StringDeserializer.class);
		props.put("value.deserializer", PulseRecordDeserializer.class);
		KafkaConsumer<String, PulseRecord> consumer = new KafkaConsumer<String, PulseRecord>(props);

		consumer.subscribe(Arrays.asList(type));
		while (true) {
			ConsumerRecords<String, PulseRecord> records = consumer.poll(100);
			for (ConsumerRecord<String, PulseRecord> record : records) {
				System.out.printf("Processing %s: offset = %d, key = %s, value = %" + "s\n", type, record.offset(),
						record.key(), record.value());
			}
		}

	}

}

class GeneratePulse extends Thread {
	public void run() {
		Properties props = new Properties();
		props.put("bootstrap.servers", "localhost:9092");
		props.put("acks", "all");
		props.put("retries", 0);
		props.put("batch.size", 16384);
		props.put("linger.ms", 1);
		props.put("buffer.memory", 33554432);
		props.put("key.serializer", StringSerializer.class);
		props.put("value.serializer", PulseRecordSerializer.class);
		Producer<String, PulseRecord> producer = new KafkaProducer<String, PulseRecord>(props);
		try {
			BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
			String input = br.readLine();
			while (!input.equals("q")) {
				String arr[] = input.split(" ");
				PulseRecord pr = new PulseRecord(arr[1] + "-" + new Date(), EVENTTYPE.CREATE);
				producer.send(new ProducerRecord<String, PulseRecord>(arr[0], arr[1], pr),
						new Callback() {
							public void onCompletion(RecordMetadata metadata, Exception e) {
								if (e != null)
									e.printStackTrace();
								System.out.println("The offset of the record we just sent is: " + metadata.offset());
							}
						});
				input = br.readLine();
			}
		} catch (Exception e) {
		} finally {
			producer.close();
			System.out.println("Exiting...");
		}
	}
}