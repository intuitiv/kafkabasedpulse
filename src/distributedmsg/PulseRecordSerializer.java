package distributedmsg;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.util.Map;

import org.apache.commons.lang3.SerializationUtils;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Serializer;

public class PulseRecordSerializer implements Serializer<PulseRecord> {

	public PulseRecordSerializer() {
	}

	@Override
	public void close() {
		// TODO Auto-generated method stub

	}

	@Override
	public void configure(Map<String, ?> configs, boolean isKey) {

	}

	@Override
	public byte[] serialize(String topic, PulseRecord data) {
		if (data == null) {
			return null;
		} else {
			return SerializationUtils.serialize(data);
		}
	}

}