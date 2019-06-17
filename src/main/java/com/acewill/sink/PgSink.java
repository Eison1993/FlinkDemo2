package com.acewill.sink;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import scala.Tuple2;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.util.Properties;

public class PgSink<A,B> extends RichSinkFunction<Tuple2<A,B>> {

    private static final long serialVersionUID = 1L;

    private Connection connection;

    private PreparedStatement preparedStatement;

    @Override
    public void open(Configuration param) throws Exception {

        Properties p = new Properties();
        p.load(PgSink.class.getClassLoader().getResourceAsStream("application.properties"));


        String URL = p.getProperty("db.url");

        String USERNAME = p.getProperty("db.user");

        String PASSWORD = p.getProperty("db.password");

        String driverClass = p.getProperty("db.driver");


//      String USERNAME = "postgres";
//      String PASSWORD = "passwd";
//      String driverClass = "org.postgresql.Driver";
//      String URL = "jdbc:postgresql://192.168.108.01:5432/flink";
        //加载jdbc的驱动
        Class.forName(driverClass);
        //获取数据库的连接
        connection = DriverManager.getConnection(URL, USERNAME, PASSWORD);
        String sql = "insert into flink(word, num) values (?,?)";
        preparedStatement = connection.prepareStatement(sql);
        super.open(param);

    }

    @Override
    public void invoke(Tuple2<A, B> value, Context context) throws Exception {
        try {
            String word = value._1.toString();
            int num = Integer.parseInt(value._2.toString());
            preparedStatement.setString(1, word);
            preparedStatement.setInt(2, num);
            preparedStatement.executeUpdate();
        } catch (Exception e) {
            e.printStackTrace();
        }

    }


    @Override
    public void close() throws Exception {
        if (preparedStatement != null) {
            preparedStatement.close();
        }
        if (connection != null) {
            connection.close();
        }
        super.close();
    }


}
