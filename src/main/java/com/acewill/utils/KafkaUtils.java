package com.acewill.utils;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;

import java.util.Arrays;
import java.util.List;
import java.util.Properties;

public class KafkaUtils {

    //--input-topic torder --bootstrap.servers zys01:9092,zys02:9092,zys03:9092 --group.id g2 --ck-path  file:///E:/Develop/scala-workspace/shark_flink/ck01
    //  用于接收参数
    private static ParameterTool params;

    // Flink的执行环境变量

    private static StreamExecutionEnvironment env;


    public static DataStreamSource createKafkaDataStream(String[] args) throws Exception {
        // 获取参数
        params = ParameterTool.fromArgs(args);


//        if (params.getNumberOfParameters() < 3) {
//            System.out.println("Missing parameters!\n"
//                    + "Usage: Kafka --input-topic <topic>"
//                    + "--bootstrap.servers <kafka brokers> "
//                    + "--group.id <some id> [--prefix <prefix>]");
//               return;
//        }


        //创建env
        env = StreamExecutionEnvironment.getExecutionEnvironment();

        //创建checkpoint的相关信息

        env.enableCheckpointing(params.getLong("checkpoint-interval", 5000L), CheckpointingMode.EXACTLY_ONCE);

        //把状态保存在check-point文件夹，与checkpoint同时使用

        env.setStateBackend(new FsStateBackend(params.getRequired("checkpoint-dir")));


        //设置如果没有记录偏移量就从最开始度，有偏移量就接着读
        Properties kafkaparams = params.getProperties();

        kafkaparams.setProperty("auto.offset.reset", params.get("auto.offset.reset", "earliest"));

        //topics可以设置为一个list集合
        String topics = params.getRequired("input-topic");

        List<String> topicList = Arrays.asList(topics.split(","));


        //创建一个KafkaConsumer
        FlinkKafkaConsumer011 kafkasource = new FlinkKafkaConsumer011(
                topicList,
                new SimpleStringSchema(),
                kafkaparams
        );

        // 这两个参数是默认指定的
        //kafkaSource.setStartFromGroupOffsets();
        //kafkaSource.setCommitOffsetsOnCheckpoints(true);


//       创建kafka flink数据流
        DataStreamSource kafkadatastream = env.addSource(kafkasource);

        return kafkadatastream;


    }


    //    get方法
    public static ParameterTool getParmas() {
        return params;
    }

    public static StreamExecutionEnvironment getEnv() {
        return env;
    }

}
