package dwd;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.stream.common.utils.Constat;
import com.stream.common.utils.KafkaUtils;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.time.Duration;
import java.util.Date;

public class UserjoinOrderjoinpage {
    public static void main(String[] args) throws Exception {
        // 1.创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 2.从kafka中读取数据,并设置时间戳
        SingleOutputStreamOperator<String> kafkaCdcDbSourceOrder = env.fromSource(
                KafkaUtils.buildKafkaSource(
                        Constat.KAFKA_BROKERS,
                        "xuanye_chang_order_base_label",
                        new Date().toString(),
                        OffsetsInitializer.earliest()
                ),
                WatermarkStrategy.<String>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                        .withTimestampAssigner((event, timestamp) -> {
                                    JSONObject jsonObject = JSONObject.parseObject(event);
                                    if (event != null && jsonObject.containsKey("ts_ms")){
                                        try {
                                            return JSONObject.parseObject(event).getLong("ts_ms");
                                        }catch (Exception e){
                                            e.printStackTrace();
                                            System.err.println("Failed to parse event as JSON or get ts_ms: " + event);
                                            return 0L;
                                        }
                                    }
                                    return 0L;
                                }
                        ),
                "kafka_cdc_db_source"
        ).uid("kafka_cdc_db_source1").name("kafka_cdc_db_source");


        SingleOutputStreamOperator<String> kafkaCdcDbSourceuser = env.fromSource(
                KafkaUtils.buildKafkaSource(
                        Constat.KAFKA_BROKERS,
                        "xuanye_chang_user_base_label",
                        new Date().toString(),
                        OffsetsInitializer.earliest()
                ),
                WatermarkStrategy.<String>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                        .withTimestampAssigner((event, timestamp) -> {
                                    JSONObject jsonObject = JSONObject.parseObject(event);
                                    if (event != null && jsonObject.containsKey("ts_ms")){
                                        try {
                                            return JSONObject.parseObject(event).getLong("ts_ms");
                                        }catch (Exception e){
                                            e.printStackTrace();
                                            System.err.println("Failed to parse event as JSON or get ts_ms: " + event);
                                            return 0L;
                                        }
                                    }
                                    return 0L;
                                }
                        ),
                "kafka_cdc_db_source"
        ).uid("kafka_cdc_db_source2").name("kafka_cdc_db_source");

//

        SingleOutputStreamOperator<String> kafkaCdcDbSourcepage = env.fromSource(
                KafkaUtils.buildKafkaSource(
                        Constat.KAFKA_BROKERS,
                        "xuanye_chang_page_base_label",
                        new Date().toString(),
                        OffsetsInitializer.earliest()
                ),
                WatermarkStrategy.<String>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                        .withTimestampAssigner((event, timestamp) -> {
                                    JSONObject jsonObject = JSONObject.parseObject(event);
                                    if (event != null && jsonObject.containsKey("ts_ms")){
                                        try {
                                            return JSONObject.parseObject(event).getLong("ts_ms");
                                        }catch (Exception e){
                                            e.printStackTrace();
                                            System.err.println("Failed to parse event as JSON or get ts_ms: " + event);
                                            return 0L;
                                        }
                                    }
                                    return 0L;
                                }
                        ),
                "kafka_cdc_db_source"
        ).uid("kafka_cdc_db_source3").name("kafka_cdc_db_source");
//        kafkaCdcDbSourceOrder.print();
//        kafkaCdcDbSourceuser.print();
//        kafkaCdcDbSourcepage.print();
        SingleOutputStreamOperator<JSONObject> mapBase6LabelDs = kafkaCdcDbSourceOrder.map(JSON::parseObject);
        SingleOutputStreamOperator<JSONObject> mapBase4LabelDs = kafkaCdcDbSourceuser.map(JSON::parseObject);
        SingleOutputStreamOperator<JSONObject> mapBase2LabelDs = kafkaCdcDbSourcepage.map(JSON::parseObject);

        SingleOutputStreamOperator<JSONObject> join2_4Ds = mapBase2LabelDs.keyBy(o -> o.getString("uid"))
                .intervalJoin(mapBase4LabelDs.keyBy(o -> o.getString("uid")))
                .between(Time.hours(-24), Time.hours(24))
                .process(new ProcessJoinBase2And4BaseFunc());
        join2_4Ds.print();
        env.execute();



    }
}
