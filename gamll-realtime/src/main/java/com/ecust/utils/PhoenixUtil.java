package com.ecust.utils;

import com.alibaba.druid.pool.DruidPooledConnection;
import com.alibaba.fastjson.JSONObject;
import com.ecust.common.GmallConfig;
import org.apache.commons.lang.StringUtils;


import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Collection;
import java.util.Set;

public class PhoenixUtil {
    /**
     * 1.拼接SQL
     * 2.预编译
     * 3.执行
     * 4.释放资源
     *
     * @param connection Phoenix 连接
     * @param sinkTable  表名
     * @param data       数据
     */
    public static void upsertValues(DruidPooledConnection connection, String sinkTable, JSONObject data) throws SQLException {
        //1.拼接SQL upsert into db.tn(id,name,sex) values(xxx,xx,xxx);
        Set<String> columns = data.keySet();
        Collection<Object> values = data.values();
        String sql = "upsert into " + GmallConfig.HBASE_SCHEMA + "." + sinkTable + "(" +
                StringUtils.join(columns, ",") + ") values ('" +
                StringUtils.join(values, "','") + "')";
        // 2.预编译SQL
        PreparedStatement preparedStatement = connection.prepareStatement(sql);
        preparedStatement.execute();
        connection.commit();
        preparedStatement.close();
    }
}
