package dwd;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.stream.common.utils.Constat;
import com.stream.common.utils.JdbcUtils;
import com.stream.common.utils.KafkaUtils;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.sql.Connection;
import java.time.Duration;
import java.util.Date;
import java.util.List;

public class DbCdcorderinfoBaseLable {

    private static final double time_rate_weight_coefficient = 0.1;    // 时间权重系数
    private static final double amount_rate_weight_coefficient = 0.15;    // 价格权重系数
    private static final double brand_rate_weight_coefficient = 0.2;    // 品牌权重系数
    private static final double category_rate_weight_coefficient = 0.3; // 类目权重系数
    private static Connection connection;
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        SingleOutputStreamOperator<String> kafkaCdcDbSource = env.fromSource(
                KafkaUtils.buildKafkaSource(
                        Constat.KAFKA_BROKERS,
                        Constat.TOPIC_DB,
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
        ).uid("kafka_cdc_db_source").name("kafka_cdc_db_source");
        SingleOutputStreamOperator<JSONObject> datamapJSON = kafkaCdcDbSource.map(JSON::parseObject);

        //订单表
        SingleOutputStreamOperator<JSONObject> cdcOrderInfoDs = datamapJSON.filter(json -> json.getJSONObject("source").getString("table").equals("order_info"))
                .uid("kafka_cdc_db_order_source").name("kafka_cdc_db_order_source");
        //订单明细表
        SingleOutputStreamOperator<JSONObject> cdcOrderDetailDs = datamapJSON.filter(json -> json.getJSONObject("source").getString("table").equals("order_detail"))
                .uid("kafka_cdc_db_order_detail_source").name("kafka_cdc_db_order_source");
//        filterdataOrderDetil.print();

        SingleOutputStreamOperator<JSONObject> mapCdcOrderInfoDs = cdcOrderInfoDs.map(new MapOrderInfoDataFunc());
        SingleOutputStreamOperator<JSONObject> mapCdcOrderDetailDs = cdcOrderDetailDs.map(new MapOrderDetailFunc());

        SingleOutputStreamOperator<JSONObject> filterNotNullCdcOrderInfoDs = mapCdcOrderInfoDs.filter(data -> data.getString("id") != null && !data.getString("id").isEmpty());
        SingleOutputStreamOperator<JSONObject> filterNotNullCdcOrderDetailDs = mapCdcOrderDetailDs.filter(data -> data.getString("order_id") != null && !data.getString("order_id").isEmpty());

        KeyedStream<JSONObject, String> keyedStreamCdcOrderInfoDs = filterNotNullCdcOrderInfoDs.keyBy(data -> data.getString("id"));
        KeyedStream<JSONObject, String> keyedStreamCdcOrderDetailDs = filterNotNullCdcOrderDetailDs.keyBy(data -> data.getString("order_id"));

        SingleOutputStreamOperator<JSONObject> processIntervalJoinOrderInfoAndDetailDs = keyedStreamCdcOrderInfoDs.intervalJoin(keyedStreamCdcOrderDetailDs)
                .between(Time.minutes(-2), Time.minutes(2))
                .process(new IntervalDbOrderInfoJoinOrderDetailProcessFunc());

        SingleOutputStreamOperator<JSONObject> processDuplicateOrderInfoAndDetailDs = processIntervalJoinOrderInfoAndDetailDs.keyBy(data -> data.getString("detail_id"))
                .process(new processOrderInfoAndDetailFunc());
        //processDuplicateOrderInfoAndDetailDs.print();

        List<DimBaseCategory> dim_base_categories;
        try {
            connection = JdbcUtils.getMySQLConnection(
                    "jdbc:mysql://10.160.60.17:3306/realtime_v1",
                    "root",
                    "Zh1028,./");
            String sql = "select b3.id,                          \n" +
                    "            b3.name as b3name,              \n" +
                    "            b2.name as b2name,              \n" +
                    "            b1.name as b1name               \n" +
                    "     from realtime_v1.base_category3 as b3  \n" +
                    "     join realtime_v1.base_category2 as b2  \n" +
                    "     on b3.category2_id = b2.id             \n" +
                    "     join realtime_v1.base_category1 as b1  \n" +
                    "     on b2.category1_id = b1.id";
            dim_base_categories = JdbcUtils.queryList2(connection, sql, DimBaseCategory.class, false);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        SingleOutputStreamOperator<JSONObject> mapOrderInfoAndDetailModelDs = processDuplicateOrderInfoAndDetailDs.map(new MapOrderAndDetailRateModelFunc(dim_base_categories, time_rate_weight_coefficient, amount_rate_weight_coefficient, brand_rate_weight_coefficient, category_rate_weight_coefficient));
        mapOrderInfoAndDetailModelDs.print();
        mapOrderInfoAndDetailModelDs.map(data -> data.toJSONString())
                .sinkTo(
                        KafkaUtils.buildKafkaSink(Constat.KAFKA_BROKERS,Constat.TOPIC_DWD_order_base_label)
                );
        env.execute();
    }
}
