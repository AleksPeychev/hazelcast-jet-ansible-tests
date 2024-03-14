package com.hazelcast.jet.tests.kafka.connect.stream;

import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.data.Values;
import org.apache.kafka.connect.source.SourceRecord;

public class TestUtil {

    private TestUtil(){
    }

    static String getDocId(SourceRecord rec) {
        Struct value = (Struct) rec.value();
        return value.getString("docId");
    }
    static String convertToString(SourceRecord rec) {
        return Values.convertToString(rec.valueSchema(), rec.value());
    }

}
