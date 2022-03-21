package com.alibaba.otter.canal.connector.kafka.producer;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.serializer.SerializerFeature;
import com.alibaba.otter.canal.common.utils.ExecutorTemplate;
import com.alibaba.otter.canal.connector.core.producer.AbstractMQProducer;
import com.alibaba.otter.canal.connector.core.producer.MQDestination;
import com.alibaba.otter.canal.connector.core.producer.MQMessageUtils;
import com.alibaba.otter.canal.connector.core.producer.MQMessageUtils.EntryRowData;
import com.alibaba.otter.canal.connector.core.spi.CanalMQProducer;
import com.alibaba.otter.canal.connector.core.spi.SPI;
import com.alibaba.otter.canal.connector.core.util.Callback;
import com.alibaba.otter.canal.connector.core.util.CanalMessageSerializerUtil;
import com.alibaba.otter.canal.connector.kafka.config.KafkaConstants;
import com.alibaba.otter.canal.connector.kafka.config.KafkaProducerConfig;
import com.alibaba.otter.canal.connector.kafka.util.ClickHouseClient;
import com.alibaba.otter.canal.protocol.FlatMessage;
import com.alibaba.otter.canal.protocol.Message;
import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.collection.mutable.StringBuilder;

import java.io.File;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

/**
 * kafka producer SPI 实现
 *
 * @author rewerma 2018-6-11 下午05:30:49
 * @version 1.0.0
 */
@SuppressWarnings({"rawtypes", "unchecked"})
@SPI("kafka")
public class CanalKafkaProducer extends AbstractMQProducer implements CanalMQProducer {

    private static final Logger logger = LoggerFactory.getLogger(CanalKafkaProducer.class);

    private static final String PREFIX_KAFKA_CONFIG = "kafka.";

    private Producer<String, byte[]> producer;


    @Override
    public void init(Properties properties) {
        KafkaProducerConfig kafkaProducerConfig = new KafkaProducerConfig();
        this.mqProperties = kafkaProducerConfig;
        super.init(properties);
        // load properties
        this.loadKafkaProperties(properties);

        Properties kafkaProperties = new Properties();
        kafkaProperties.putAll(kafkaProducerConfig.getKafkaProperties());
        kafkaProperties.put("max.in.flight.requests.per.connection", 1);
        kafkaProperties.put("key.serializer", StringSerializer.class);
        if (kafkaProducerConfig.isKerberosEnabled()) {
            File krb5File = new File(kafkaProducerConfig.getKrb5File());
            File jaasFile = new File(kafkaProducerConfig.getJaasFile());
            if (krb5File.exists() && jaasFile.exists()) {
                // 配置kerberos认证，需要使用绝对路径
                System.setProperty("java.security.krb5.conf", krb5File.getAbsolutePath());
                System.setProperty("java.security.auth.login.config", jaasFile.getAbsolutePath());
                System.setProperty("javax.security.auth.useSubjectCredsOnly", "false");
                kafkaProperties.put("security.protocol", "SASL_PLAINTEXT");
                kafkaProperties.put("sasl.kerberos.service.name", "kafka");
            } else {
                String errorMsg = "ERROR # The kafka kerberos configuration file does not exist! please check it";
                logger.error(errorMsg);
                throw new RuntimeException(errorMsg);
            }
        }
        kafkaProperties.put("value.serializer", KafkaMessageSerializer.class);
        producer = new KafkaProducer<>(kafkaProperties);
    }

    private void loadKafkaProperties(Properties properties) {
        KafkaProducerConfig kafkaProducerConfig = (KafkaProducerConfig) this.mqProperties;
        Map<String, Object> kafkaProperties = kafkaProducerConfig.getKafkaProperties();
        // 兼容下<=1.1.4的mq配置
        doMoreCompatibleConvert("canal.mq.servers", "kafka.bootstrap.servers", properties);
        doMoreCompatibleConvert("canal.mq.acks", "kafka.acks", properties);
        doMoreCompatibleConvert("canal.mq.compressionType", "kafka.compression.type", properties);
        doMoreCompatibleConvert("canal.mq.retries", "kafka.retries", properties);
        doMoreCompatibleConvert("canal.mq.batchSize", "kafka.batch.size", properties);
        doMoreCompatibleConvert("canal.mq.lingerMs", "kafka.linger.ms", properties);
        doMoreCompatibleConvert("canal.mq.maxRequestSize", "kafka.max.request.size", properties);
        doMoreCompatibleConvert("canal.mq.bufferMemory", "kafka.buffer.memory", properties);
        doMoreCompatibleConvert("canal.mq.kafka.kerberos.enable", "kafka.kerberos.enable", properties);
        doMoreCompatibleConvert("canal.mq.kafka.kerberos.krb5.file", "kafka.kerberos.krb5.file", properties);
        doMoreCompatibleConvert("canal.mq.kafka.kerberos.jaas.file", "kafka.kerberos.jaas.file", properties);

        for (Map.Entry<Object, Object> entry : properties.entrySet()) {
            String key = (String) entry.getKey();
            Object value = entry.getValue();
            if (key.startsWith(PREFIX_KAFKA_CONFIG) && value != null) {
                key = key.substring(PREFIX_KAFKA_CONFIG.length());
                kafkaProperties.put(key, value);
            }
        }
        String kerberosEnabled = properties.getProperty(KafkaConstants.CANAL_MQ_KAFKA_KERBEROS_ENABLE);
        if (!StringUtils.isEmpty(kerberosEnabled)) {
            kafkaProducerConfig.setKerberosEnabled(Boolean.parseBoolean(kerberosEnabled));
        }
        String krb5File = properties.getProperty(KafkaConstants.CANAL_MQ_KAFKA_KERBEROS_KRB5_FILE);
        if (!StringUtils.isEmpty(krb5File)) {
            kafkaProducerConfig.setKrb5File(krb5File);
        }
        String jaasFile = properties.getProperty(KafkaConstants.CANAL_MQ_KAFKA_KERBEROS_JAAS_FILE);
        if (!StringUtils.isEmpty(jaasFile)) {
            kafkaProducerConfig.setJaasFile(jaasFile);
        }
    }

    @Override
    public void stop() {
        try {
            logger.info("## stop the kafka producer");
            if (producer != null) {
                producer.close();
            }
            super.stop();
        } catch (Throwable e) {
            logger.warn("##something goes wrong when stopping kafka producer:", e);
        } finally {
            logger.info("## kafka producer is down.");
        }
    }

    @Override
    public void send(MQDestination mqDestination, Message message, Callback callback) {
        ExecutorTemplate template = new ExecutorTemplate(sendExecutor);

        try {
            List result;
            if (!StringUtils.isEmpty(mqDestination.getDynamicTopic())) {
                // 动态topic路由计算,只是基于schema/table,不涉及proto数据反序列化
                Map<String, Message> messageMap = MQMessageUtils.messageTopics(message,
                        mqDestination.getTopic(),
                        mqDestination.getDynamicTopic());

                // 针对不同的topic,引入多线程提升效率
                for (Map.Entry<String, Message> entry : messageMap.entrySet()) {
                    final String topicName = entry.getKey().replace('.', '_');
                    final Message messageSub = entry.getValue();
                    template.submit((Callable) () -> {
                        try {
                            return send(mqDestination, topicName, messageSub, mqProperties.isFlatMessage());
                        } catch (Exception e) {
                            throw new RuntimeException(e);
                        }
                    });
                }

                result = template.waitForResult();
            } else {
                result = new ArrayList();
                List<Future> futures = send(mqDestination,
                        mqDestination.getTopic(),
                        message,
                        mqProperties.isFlatMessage());
                result.add(futures);
            }

            // 一个批次的所有topic和分区的队列，都采用异步的模式进行多线程批量发送
            // 最后在集结点进行flush等待，确保所有数据都写出成功
            // 注意：kafka的异步模式如果要保证顺序性，需要设置max.in.flight.requests.per.connection=1，确保在网络异常重试时有排他性
            producer.flush();
            // flush操作也有可能是发送失败,这里需要异步关注一下发送结果,针对有异常的直接触发rollback
            for (Object obj : result) {
                List<Future> futures = (List<Future>) obj;
                for (Future future : futures) {
                    try {
                        future.get();
                    } catch (InterruptedException | ExecutionException e) {
                        throw new RuntimeException(e);
                    }
                }
            }

            callback.commit();
        } catch (SQLException e) {
            logger.error(e.getMessage(), e);
            logger.error("DELETE语句执行出错，程序异常终止！！！");
            System.exit(-1);
        } catch (Throwable e) {
            logger.error(e.getMessage(), e);
            callback.rollback();
        } finally {
            template.clear();
        }
    }

    private List<Future> send(MQDestination mqDestination, String topicName, Message message, boolean flat) throws SQLException, InterruptedException {
        List<ProducerRecord<String, byte[]>> records = new ArrayList<>();
        // 获取当前topic的分区数
        Integer partitionNum = MQMessageUtils.parseDynamicTopicPartition(topicName, mqDestination.getDynamicTopicPartitionNum());
        if (partitionNum == null) {
            partitionNum = mqDestination.getPartitionsNum();
        }
        if (!flat) {
            if (mqDestination.getPartitionHash() != null && !mqDestination.getPartitionHash().isEmpty()) {
                // 并发构造
                EntryRowData[] datas = MQMessageUtils.buildMessageData(message, buildExecutor);
                // 串行分区
                Message[] messages = MQMessageUtils.messagePartition(datas,
                        message.getId(),
                        partitionNum,
                        mqDestination.getPartitionHash(),
                        this.mqProperties.isDatabaseHash());
                int length = messages.length;
                for (int i = 0; i < length; i++) {
                    Message messagePartition = messages[i];
                    if (messagePartition != null) {
                        records.add(new ProducerRecord<>(topicName,
                                i,
                                null,
                                CanalMessageSerializerUtil.serializer(messagePartition,
                                        mqProperties.isFilterTransactionEntry())));
                    }
                }
            } else {
                final int partition = mqDestination.getPartition() != null ? mqDestination.getPartition() : 0;
                records.add(new ProducerRecord<>(topicName,
                        partition,
                        null,
                        CanalMessageSerializerUtil.serializer(message, mqProperties.isFilterTransactionEntry())));
            }
        } else {
            // 发送扁平数据json
            // 并发构造
            EntryRowData[] datas = MQMessageUtils.buildMessageData(message, buildExecutor);
            // 串行分区
            List<FlatMessage> flatMessages = MQMessageUtils.messageConverter(datas, message.getId());
            for (FlatMessage flatMessage : flatMessages) {
                if (mqDestination.getPartitionHash() != null && !mqDestination.getPartitionHash().isEmpty()) { //多分区
                    FlatMessage[] partitionFlatMessage = MQMessageUtils.messagePartition(flatMessage,
                            partitionNum,
                            mqDestination.getPartitionHash(),
                            this.mqProperties.isDatabaseHash());
                    int length = partitionFlatMessage.length;
                    for (int i = 0; i < length; i++) {
                        FlatMessage flatMessagePart = partitionFlatMessage[i];
                        if (flatMessagePart != null) {
                            if (!this.mqProperties.isFlatMessageOnlyData()) {       //发送完整数据
                                records.add(new ProducerRecord<>(topicName, i, null, JSON.toJSONBytes(flatMessagePart,
                                        SerializerFeature.WriteMapNullValue)));
                            } else {                                                //只发送data数据，用于支持Clickhouse同步kafka
                                if (flatMessagePart.getType().equalsIgnoreCase("DELETE")) {   //delete情况下,需要额外处理
                                    if (isFrequentTable(flatMessagePart)) {
                                        //判断要delete的表是否为频繁delete的表（频繁delete的表用CollapsingMergeTree引擎）
                                        List<Map<String, String>> flatMessagePartData = flatMessagePart.getData();
                                        if (flatMessagePartData != null) {
                                            for (Map<String, String> partData : flatMessagePartData) {
                                                partData.put("sign", "-1");      //CollapsingMergeTree加入-1字段表示删除
                                                records.add(new ProducerRecord<>(topicName, i, null, JSON.toJSONBytes(partData,
                                                        SerializerFeature.WriteMapNullValue)));
                                            }
                                        }
                                    } else {
                                        execDelete(flatMessagePart);
                                    }
                                } else {                                       //insert和update情况下只发送data的数据
                                    List<Map<String, String>> flatMessagePartData = flatMessagePart.getData();
                                    if (flatMessagePartData != null &&
                                            (flatMessagePart.getType().equalsIgnoreCase("INSERT")
                                                    || flatMessagePart.getType().equalsIgnoreCase("UPDATE"))) {
                                        for (Map<String, String> partData : flatMessagePartData) {
                                            if(isFrequentTable(flatMessagePart)&&flatMessagePart.getType().equalsIgnoreCase("UPDATE")) {
                                                partData.put("sign", "-1");        //CollapsingMergeTree的update先删除再插入
                                                records.add(new ProducerRecord<>(topicName, i, null, JSON.toJSONBytes(partData,
                                                        SerializerFeature.WriteMapNullValue)));
                                            }
                                            if(isFrequentTable(flatMessagePart)) partData.put("sign", "1");
                                            records.add(new ProducerRecord<>(topicName, i, null, JSON.toJSONBytes(partData,
                                                    SerializerFeature.WriteMapNullValue)));
                                        }
                                    }
                                }
                            }
                        }
                    }
                } else {    //消息队列单分区
                    final int partition = mqDestination.getPartition() != null ? mqDestination.getPartition() : 0;
                    if (!this.mqProperties.isFlatMessageOnlyData()) {
                        records.add(new ProducerRecord<>(topicName, partition, null, JSON.toJSONBytes(flatMessage,
                                SerializerFeature.WriteMapNullValue)));
                    } else {
                        if (flatMessage.getType().equalsIgnoreCase("DELETE")) {   //delete情况下去ck执行delete
                            if (isFrequentTable(flatMessage)) {
                                //判断要delete的表是否为频繁delete的表（频繁delete的表用CollapsingMergeTree引擎）
                                List<Map<String, String>> flatMessagePartData = flatMessage.getData();
                                if (flatMessagePartData != null) {
                                    for (Map<String, String> partData : flatMessagePartData) {
                                        partData.put("sign", "-1");      //CollapsingMergeTree加入-1字段表示删除
                                        records.add(new ProducerRecord<>(topicName, partition, null, JSON.toJSONBytes(partData,
                                                SerializerFeature.WriteMapNullValue)));
                                    }
                                }
                            }
                            else{
                                execDelete(flatMessage);
                            }
                        } else {
                            List<Map<String, String>> flatMessagePartData = flatMessage.getData();
                            if (flatMessagePartData != null && (
                                    flatMessage.getType().equalsIgnoreCase("INSERT")
                                            || flatMessage.getType().equalsIgnoreCase("UPDATE"))) {
                                for (Map<String, String> partData : flatMessagePartData) {
                                    if(isFrequentTable(flatMessage)&&flatMessage.getType().equalsIgnoreCase("UPDATE")) {
                                        partData.put("sign", "-1");        //CollapsingMergeTree的update先删除再插入
                                        records.add(new ProducerRecord<>(topicName, partition, null, JSON.toJSONBytes(partData,
                                                SerializerFeature.WriteMapNullValue)));
                                    }
                                    if(isFrequentTable(flatMessage)) partData.put("sign", "1");
                                    records.add(new ProducerRecord<>(topicName, partition, null, JSON.toJSONBytes(partData,
                                            SerializerFeature.WriteMapNullValue)));
                                }
                            }
                        }
                    }
                }
            }
        }

        return produce(records);
    }

    private boolean isFrequentTable(FlatMessage message) {
        String[] deleteTables = this.mqProperties.getCkFrequentDeleteTables().split(",");
        String tbFullName = message.getDatabase() + "." + message.getTable();
        return ArrayUtils.contains(deleteTables, tbFullName);
    }


    private String appendCondition(List<String> pkNames, String deletePrefix, Map<String, String> data) {
        StringBuilder deleteBuilder = new StringBuilder();
        deleteBuilder.append(deletePrefix);
        for (String pkName : pkNames) {  //添加主键筛选条件
            deleteBuilder.append(pkName).append("='" + data.get(pkName) + "' and ");
        }
        int len = deleteBuilder.length();
        deleteBuilder.delete(len - 4, len);
        return deleteBuilder.toString();
    }

    /**
     * @Author XieChuangJian
     * @Description ReplacingMergeTree表的删除操作
     * @Date 2022/3/11
     */
    private void execDelete(FlatMessage flatMessagePart) throws SQLException, InterruptedException {
        if (ClickHouseClient.dataSource == null) {
            ClickHouseClient.init(this.mqProperties.getCkURL(),
                    this.mqProperties.getCkUsername(),
                    this.mqProperties.getCkPassword());
        }
        Connection connection = ClickHouseClient.dataSource.getConnection(60000);
        List<String> pkNames = flatMessagePart.getPkNames();
        List<Map<String, String>> dataList = flatMessagePart.getData();
        String database = flatMessagePart.getDatabase();
        String table = flatMessagePart.getTable() + "_local";   //默认更改Clickhouse的本地表
        String cluster = this.mqProperties.getCkClusterName();
        if (!ClickHouseClient.isExist(database, table, connection)) {    //判断该表是否存在，若不存在则返回
            return;
        }
        String selectPrefix = "select 1 from " + database + "." + flatMessagePart.getTable() + " where ";
        String deletePrefix = "alter table " + database + "." + table + " on cluster " + cluster + " delete where ";
        for (Map<String, String> data : dataList) {
            String selectSQL = appendCondition(pkNames, selectPrefix, data);
            String deleteSQL = appendCondition(pkNames, deletePrefix, data);
            int cnt = 0;
            while (ClickHouseClient.isEmpty(selectSQL, connection)) {    //循环一分钟判断要删除的行是否存在
                cnt++;
                Thread.sleep(1000);
                if (cnt >= 60) {
                    throw new SQLException("执行" + selectSQL + "结果为空，要delete的数据行不存在，请检测数据同步情况！！！");
                }
            }
            logger.warn("执行DELETE，SQL：" + deleteSQL);
            ClickHouseClient.executeSQL(deleteSQL, connection);
        }
        connection.close();
    }


    private List<Future> produce(List<ProducerRecord<String, byte[]>> records) {
        List<Future> futures = new ArrayList<>();
        // 异步发送，因为在partition hash的时候已经按照每个分区合并了消息，走到这一步不需要考虑单个分区内的顺序问题
        for (ProducerRecord record : records) {
            futures.add(producer.send(record));
        }
        return futures;
    }

}
