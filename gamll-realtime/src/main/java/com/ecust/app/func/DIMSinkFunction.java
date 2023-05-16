package com.ecust.app.func;

import com.alibaba.druid.pool.DruidDataSource;
import com.alibaba.druid.pool.DruidPooledConnection;
import com.alibaba.fastjson.JSONObject;
import com.ecust.utils.DruidDSUtil;
import com.ecust.utils.PhoenixUtil;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.sql.SQLException;
// {"sinkTable":"dim_base_trademark","database":"gmall","xid":1959,"data":{"tm_name":"SHANGHAI","id":122},"old":{"tm_name":"shanghai"},"commit":true,"type":"update","table":"base_trademark","ts":1592199920}
public class DIMSinkFunction extends RichSinkFunction<JSONObject> {
    private DruidDataSource dataSource;

    @Override
    public void open(Configuration parameters) throws Exception {
        // 创建连接池
        dataSource = DruidDSUtil.createDataSource();
    }

    @Override
    public void invoke(JSONObject value, Context context) throws Exception {
        // 获取连接
        DruidPooledConnection connection = dataSource.getConnection();

        // 写出数据
        String sinkTable = value.getString("sinkTable");
        JSONObject data = value.getJSONObject("data");
        PhoenixUtil.upsertValues(connection, sinkTable, data);

        // 归还连接
        connection.close();
    }
}
