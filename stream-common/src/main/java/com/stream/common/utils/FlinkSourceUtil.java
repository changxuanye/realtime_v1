package com.stream.common.utils;

import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;

import java.io.IOException;
import java.util.Properties;

public class FlinkSourceUtil {
    public static KafkaSource<String> getKafkaSource(String topic,String groupid){

        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers("cdh01:9092,cdh02:9092,cdh03:9092")
                .setTopics(topic)
                .setGroupId(groupid)
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new DeserializationSchema<String>() {
                    @Override
                    public String deserialize(byte[] bytes) throws IOException {
                        if (bytes != null) {
                            return new String(bytes);
                        }
                        return null;
                    }

                    @Override
                    public boolean isEndOfStream(String s) {
                        return false;
                    }

                    @Override
                    public TypeInformation<String> getProducedType() {
                        return TypeInformation.of(String.class);
                    }
                })
                .build();
        return source;
    }
    //获取mysqlsource
    public static MySqlSource<String> getMySqlSource( String database ,String tablename){
        Properties properties = new Properties();
        properties.setProperty("decimal.handling.mode","string");
        properties.setProperty("time.precision.mode","connect");


        MySqlSource<String> mySqlSource = MySqlSource.<String>builder()
                .hostname("10.39.48.36")
                .startupOptions(StartupOptions.initial())
                .debeziumProperties(properties)
                .port(3306)
                .databaseList(database)
                .tableList(database+"."+tablename)
                .username("root")
                .password("Zh1028,./")
                .deserializer(new JsonDebeziumDeserializationSchema()) // 将 SourceRecord 转换为 JSON 字符串
                .build();
        return mySqlSource;
    }
}
