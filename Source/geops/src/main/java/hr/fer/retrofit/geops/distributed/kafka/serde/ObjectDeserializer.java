package hr.fer.retrofit.geops.distributed.kafka.serde;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.Map;
import org.apache.kafka.common.serialization.Deserializer;

public class ObjectDeserializer implements Deserializer<Object> {

    @Override
    public Object deserialize(String topic, byte[] buf) {
        try (ObjectInputStream ois = new ObjectInputStream(new ByteArrayInputStream(buf))) {
            return ois.readObject();
        } catch (IOException | ClassNotFoundException ex) {
            return null;
        }
    }

    @Override
    public void configure(Map<String, ?> map, boolean bln) {
    }

    @Override
    public void close() {
    }
}
