package com.acewill.apps;

import com.acewill.sink.PgSink;
import com.acewill.utils.KafkaUtils;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.commons.lang3.time.FastDateFormat;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.util.Date;

public class Test {

    public static void main(String[] args) throws Exception {

        DataStreamSource kafkasource = KafkaUtils.createKafkaDataStream(args);

        //对binlog根据分类进行测流输出，考虑公司的业务数据基本只有插入的数据
        OutputTag<String> insert = new OutputTag<String>("insert") {
        };
        OutputTag<String> alter = new OutputTag<String>("alter") {
        };


        SingleOutputStreamOperator mainDataStream = kafkasource.process(new ProcessFunction<String, String>() {
            @Override
            public void processElement(String str, Context context, Collector<String> collector) {

                //输出主流，保留原始的流
                collector.collect(str);


                //分流，非主流

                JSONObject obj = JSON.parseObject(str);
                String type = obj.getString("type");

                if (type != null && type.equals("INSERT")) {
                    context.output(insert, str);
                } else if (type != null && type.equals("UPDATE")) {
                    context.output(alter, str);
                }
            }
        });

        DataStream insertsideOutput = mainDataStream.getSideOutput(insert);
        DataStream altersideOutput = mainDataStream.getSideOutput(alter);


        //分别对插入的操作和修改的操作执行业务逻辑

        SingleOutputStreamOperator<Tuple5<String, String, String,Double,Integer>> namedatemoneyandone = insertsideOutput.map(new MapFunction<String, Tuple5<String, String, String,Double, Integer>>() {

            FastDateFormat secondefastDateFormat = FastDateFormat.getInstance("yyyy-MM-dd HH-mm-ss");
            FastDateFormat datefastDateFormat = FastDateFormat.getInstance("yyyy-MM-dd");

            @Override
            public Tuple5<String, String, String, Double, Integer> map(String s) throws Exception {
                JSONObject obj = JSON.parseObject(s);

                Double money = obj.getDouble("money");

                Long committime = obj.getLong("committime");

                Date date = new Date(committime);

                String username = obj.getString("username");

                String secondStr = secondefastDateFormat.format(date);
                String dateStr = datefastDateFormat.format(date);

                return Tuple5.of(username, dateStr, secondStr,money, 1);
            }
        });


          // money的计算
        SingleOutputStreamOperator<Tuple2<String, Double>> moneyCaculate = namedatemoneyandone.keyBy(1).sum(3).map(new MapFunction<Tuple5<String, String, String, Double, Integer>, Tuple2<String, Double>>() {
            @Override
            public Tuple2<String, Double> map(Tuple5<String, String, String, Double, Integer> value) throws Exception {

                return Tuple2.of(value.f1, value.f3);
            }
        });

        moneyCaculate.addSink(new PgSink());


         // 条数的计算
        SingleOutputStreamOperator<Tuple2<String ,Integer>> userdateandone = namedatemoneyandone.keyBy(0,1).sum(4).map(new MapFunction<Tuple5<String, String, String, Double, Integer>, Tuple2<String, Integer>>() {

            @Override
            public Tuple2<String, Integer> map(Tuple5<String, String, String, Double, Integer> value) throws Exception {
                return Tuple2.of(value.f1, 1);
            }
        });

        userdateandone.keyBy(0).sum(1).addSink(new PgSink());


        KafkaUtils.getEnv().execute("Test");



    }
}
