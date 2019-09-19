package com.ecer.kafka.connect.oracle.scn;

import com.ecer.kafka.connect.oracle.OracleSourceConnectorConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * 读取resource文件夹中配置文件工具类
 */
public class PropertiesUtil implements Serializable{
    private static Logger log = LoggerFactory.getLogger(PropertiesUtil.class);
    private static Properties pps = new Properties();


    public static Map<String, String> getConfigMap(String fileName){
        Map<String, String> map = new HashMap<String, String>();

        InputStream resourceAsStream = null;
        try {
            resourceAsStream = new FileInputStream(fileName);
            pps.load(new InputStreamReader(resourceAsStream, "UTF-8"));

            map.put(OracleSourceConnectorConfig.DB_NAME_ALIAS,
                    pps.getProperty(OracleSourceConnectorConfig.DB_NAME_ALIAS));
            map.put(OracleSourceConnectorConfig.TOPIC_CONFIG,
                    pps.getProperty(OracleSourceConnectorConfig.TOPIC_CONFIG));
            map.put(OracleSourceConnectorConfig.DB_NAME_CONFIG,
                    pps.getProperty(OracleSourceConnectorConfig.DB_NAME_CONFIG));
            map.put(OracleSourceConnectorConfig.DB_HOST_NAME_CONFIG,
                    pps.getProperty(OracleSourceConnectorConfig.DB_HOST_NAME_CONFIG));
            map.put(OracleSourceConnectorConfig.DB_PORT_CONFIG,
                    pps.getProperty(OracleSourceConnectorConfig.DB_PORT_CONFIG));
            map.put(OracleSourceConnectorConfig.DB_USER_CONFIG,
                    pps.getProperty(OracleSourceConnectorConfig.DB_USER_CONFIG));
            map.put(OracleSourceConnectorConfig.DB_USER_PASSWORD_CONFIG,
                    pps.getProperty(OracleSourceConnectorConfig.DB_USER_PASSWORD_CONFIG));
            map.put(OracleSourceConnectorConfig.TABLE_WHITELIST,
                    pps.getProperty(OracleSourceConnectorConfig.TABLE_WHITELIST));
            map.put(OracleSourceConnectorConfig.PARSE_DML_DATA,
                    pps.getProperty(OracleSourceConnectorConfig.PARSE_DML_DATA));
            map.put(OracleSourceConnectorConfig.DB_FETCH_SIZE,
                    pps.getProperty(OracleSourceConnectorConfig.DB_FETCH_SIZE));
            map.put(OracleSourceConnectorConfig.RESET_OFFSET,
                    pps.getProperty(OracleSourceConnectorConfig.RESET_OFFSET));
            map.put(OracleSourceConnectorConfig.START_SCN,
                    pps.getProperty(OracleSourceConnectorConfig.START_SCN));
            map.put(OracleSourceConnectorConfig.MULTITENANT,
                    pps.getProperty(OracleSourceConnectorConfig.MULTITENANT));

        } catch (IOException e) {
            log.error("加载文件流异常,文件是{}", fileName, e);
        }finally {
            if (resourceAsStream != null){
                try {
                    resourceAsStream.close();
                } catch (IOException e) {
                    log.error("关闭resourceAsStream异常", e);
                }
            }
        }
        return map;
    }

}
