package service;

import dto.Transaction;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;

import java.io.IOException;

public class JSONValueDeserializationSchema implements DeserializationSchema<Transaction> {

    @Override
    public Transaction deserialize(byte[] bytes) throws IOException {
        return null;
    }

    @Override
    public boolean isEndOfStream(Transaction transaction) {
        return false;
    }

    @Override
    public TypeInformation<Transaction> getProducedType() {
        return null;
    }
}
