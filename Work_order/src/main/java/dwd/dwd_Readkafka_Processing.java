package dwd;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.dwd.ealtime_dwd_base_log.DwdBaseLog;
import com.dwd.realtime_dwd_trade_cart_add.DwdTradeCartAdd;
import com.stream.common.base.BaseSQLApp;
import com.stream.common.constant.Constant;
import com.stream.common.utils.Constat;
import com.stream.common.utils.FlinkSourceUtil;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.apache.jasper.tagplugins.jstl.core.Out;

public class dwd_Readkafka_Processing  {
    public static void main(String[] args) throws Exception {

        //TODO 1.基本环境准备
        //1.1 指定流处理环境
        Configuration conf = new Configuration();
        conf.set(RestOptions.PORT,10013);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        //1.2 设置并行度
        env.setParallelism(4);
        //1.3 指定表执行环境
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        //TODO 2.检查点相关的设置
        env.enableCheckpointing(5000L, CheckpointingMode.EXACTLY_ONCE);
        env.setRestartStrategy(RestartStrategies.failureRateRestart(3, Time.days(30),Time.seconds(3)));

        KafkaSource<String> kafkaSource1 = FlinkSourceUtil.getKafkaSource("xuanye_chang_Business", "chAndGroupId");
        OutputTag<String> dirtyTag = new OutputTag<String>("dirtyTag"){};
        //3.3消费数据 封装为流
        DataStreamSource<String> kafkaSourceDS = env.fromSource(kafkaSource1, WatermarkStrategy.noWatermarks(), "sourceName");
        kafkaSourceDS.print();
        SingleOutputStreamOperator<JSONObject> processDS = kafkaSourceDS.process(new ProcessFunction<String, JSONObject>() {
            @Override
            public void processElement(String s, ProcessFunction<String, JSONObject>.Context context, Collector<JSONObject> collector) throws Exception {

                try {
                    JSONObject jsonObj = JSON.parseObject(s);
                    //如果转换的时候没有异常，说明是标准的json间数据传递到下游
                    collector.collect(jsonObj);
                } catch (Exception e) {
                    //如果转换的时候发生异常，属于脏数据，将其放到侧输出流中
                    context.output(dirtyTag, s);
                }
            }
        });


        //设置测流，
        OutputTag<String> order_infoTag = new OutputTag<String>("order_info") {};
        OutputTag<String> user_infoTag = new OutputTag<String>("user_info") {};
        OutputTag<String> order_detailTag = new OutputTag<String>("order_detail") {};
        OutputTag<String> comment_infoTag = new OutputTag<String>("comment_info") {};
        OutputTag<String> cart_infoTag = new OutputTag<String>("cart_info") {};
        OutputTag<String> favor_infoTag = new OutputTag<String>("favor_info") {};
        OutputTag<String> user_info_sup_msgTag = new OutputTag<String>("user_info_sup_msg") {};

        processDS


        env.execute();


    }

}
