package com.ecer.kafka.connect.oracle.scn;

import com.ecer.kafka.connect.oracle.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.Map;

import static com.ecer.kafka.connect.oracle.OracleConnectorSchema.*;

/**
 * 当前类用于获取oracle最近时间的commitSCN
 * 只有获取commitSCN才能准确地用sqoop抽取commitSCN时间点的全量到hive中
 * 然后使用oracle-connect-kafka从(commitSCN+1)处继续抽取增量
 * 整个抽数的大致过程如下：
 * 0，mvn package打包，放在KAFKA_HOME/lib/
 *    将OracleSourceConnector.properties文件放在KAFKA_HOME/config/
 * 1，在oracle中执行：
 *        select scn,to_char(time_dp,'yyyy-mm-dd hh24:mi:ss')
 *        from sys.smon_scn_time
 *        order by scn;
 *     选取最新的一个scn的值，然后将scn值配置在OracleSourceConnector.properties文件中
 * 2，执行当前类，就在KAFKA_HOME/lib/下，因为需要依赖ojdbc7.jar
 *    java -classpath "./*" com.ecer.kafka.connect.oracle.scn.ScnMain OracleSourceConnector.properties
 *    得到commitSCN
 * 3，将步骤2中的commitSCN配置在sqoop脚本中，运行sqoop开始抽数
 * 4，将步骤2中的commitSCN加1， 然后将加1后的scn值配置在OracleSourceConnector.properties文件中
 * 5，开始运行oracle-connect-kafka即可准确获取增量
 * .KAFKA_HOME/bin/connect-standalone.sh \
 *          KAFKA_HOME/config/connect-standalone.properties \
 *          KAFKA_HOME/config/OracleSourceConnector.properties
 */

/**
 * 获取commit scn, 在linux终端，相同目录下添加ojdbc.jar
 * 运行： java -classpath "./*" com.ecer.kafka.connect.oracle.scn.ScnMain OracleSourceConnector.properties
 */
public class ScnMain {


    static final Logger log = LoggerFactory.getLogger(ScnMain.class);
    public static OracleSourceConnectorConfig config;
    private static OracleSourceConnectorUtils utils;
    private static Connection dbConn;
    static String logMinerSelectSql;
    static PreparedStatement logMinerSelect;
    static ResultSet logMinerData;
    static CallableStatement logMinerStartStmt=null;
    static String logMinerOptions= OracleConnectorSQL.LOGMINER_START_OPTIONS;
    static String logMinerStartScr=OracleConnectorSQL.START_LOGMINER_CMD;

    public static void main(String[] args) {
        if (args == null || args.length < 1 || args[0].equals("")){
            System.err.println("the args(0):propertiesFile is empty, exit!");
            return;
        }

        Map<String, String> map = PropertiesUtil.getConfigMap(args[0]);
        config=new OracleSourceConnectorConfig(map);
        String startSCN = config.getStartScn();
        try {
            System.out.println("Connecting to database");
            ConnectorSQL sql = new ConnectorSQL();
            dbConn = new OracleConnection().connect(config);
            utils = new OracleSourceConnectorUtils(dbConn, config, sql);
            logMinerSelectSql = utils.getLogMinerSelectSql();



            log.info("Starting LogMiner Session");
            logMinerStartScr=logMinerStartScr+logMinerOptions+") \n; end;";

            log.info("thisisbyzs : logMinerStartScr="+logMinerStartScr);
            logMinerStartStmt=dbConn.prepareCall(logMinerStartScr);

            logMinerStartStmt.setLong(1,  Long.valueOf(startSCN));
            logMinerStartStmt.execute();

            System.out.println("thisisbyzs : logMinerSelectSql=" + logMinerSelectSql);
            logMinerSelect=dbConn.prepareCall(logMinerSelectSql);
            logMinerSelect.setFetchSize(config.getDbFetchSize());

            System.out.println("thisisbyzs : startSCN=" + startSCN);
            logMinerSelect.setLong(1, Long.valueOf(startSCN));
            logMinerData=logMinerSelect.executeQuery();
            System.out.println("Logminer started successfully"+logMinerData);
            while(logMinerData.next()) {
                Long scn = logMinerData.getLong(SCN_FIELD);
                Long commitScn = logMinerData.getLong(COMMIT_SCN_FIELD);
                String rowId = logMinerData.getString(ROW_ID_FIELD);
                boolean contSF = logMinerData.getBoolean(CSF_FIELD);
                String segOwner = logMinerData.getString(SEG_OWNER_FIELD);
                String segName = logMinerData.getString(TABLE_NAME_FIELD);
                String sqlRedo = logMinerData.getString(SQL_REDO_FIELD);

                System.out.println("/////////////////////////////////////////");
                System.out.println("scn="+scn);
                System.out.println("commitScn="+commitScn);
                System.out.println("rowId="+rowId);
                System.out.println("contSF="+contSF);
                System.out.println("segOwner="+segOwner);
                System.out.println("segName="+segName);
                System.out.println("sqlRedo="+sqlRedo);
                System.out.println("/////////////////////////////////////////");
                break;
            }

            System.out.println("Logminer stop successfully"+logMinerData);
        }catch(SQLException e){
            System.err.println("Error at database tier, Please check : "+e.toString());
        }finally {
            try {
                log.info("Logminer session cancel");
                logMinerSelect.cancel();
                if (dbConn!=null){
                    log.info("Closing database connection.Last SCN ");
                    logMinerSelect.close();
                    logMinerStartStmt.close();
                    dbConn.close();
                }
            } catch (SQLException e) {}
        }
    }

}
