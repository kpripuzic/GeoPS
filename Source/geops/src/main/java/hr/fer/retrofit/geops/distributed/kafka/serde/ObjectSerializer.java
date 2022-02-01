package hr.fer.retrofit.geops.distributed.kafka.serde;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.util.Map;
import org.apache.kafka.common.serialization.Serializer;

public class ObjectSerializer implements Serializer<Object> {

    @Override
    public byte[] serialize(String topic, Object object) {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try (ObjectOutputStream oos = new ObjectOutputStream(baos)) {
            oos.writeObject(object);
            oos.flush();
        } catch (IOException ex) {
            return null;
        }
        return baos.toByteArray();
    }

    @Override
    public void configure(Map<String, ?> map, boolean bln) {
    }

    @Override
    public void close() {
    }
}
