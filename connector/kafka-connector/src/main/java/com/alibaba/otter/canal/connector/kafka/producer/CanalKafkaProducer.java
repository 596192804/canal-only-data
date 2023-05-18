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

    private static String[] frequentDeleteTables;


    @Override
    public void init(Properties properties) {
        KafkaProducerConfig kafkaProducerConfig = new KafkaProducerConfig();
        this.mqProperties = kafkaProducerConfig;
        Object frequentDeleteTablesProp = properties.get("canal.ck.frequent.delete.tables");
        if (frequentDeleteTablesProp != null) {
            frequentDeleteTables = frequentDeleteTablesProp.toString().split(",");
        }

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
        if (!flat) { //以protobuf格式发送数据
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
                        addToKafka(topicName, records, i, flatMessagePart);
                    }
                } else {    //消息队列单分区
                    final int partition = mqDestination.getPartition() != null ? mqDestination.getPartition() : 0;
                    addToKafka(topicName, records, partition, flatMessage);
                }
            }
        }

        return produce(records);
    }

    /**
     * @param topicName   Kafka topic
     * @param records     存放Kafka Record的数据结构
     * @param partition   kafka分区编号
     * @param flatMessage 存放消息的数据结构
     * @Author XieChuangJian
     * @Description 将FlatMessage数据以JSON形式序列化添加到Kafka Records中
     * @Date 2022/4/19
     */
    private void addToKafka(String topicName, List<ProducerRecord<String, byte[]>> records, int partition, FlatMessage flatMessage) throws SQLException, InterruptedException {
        if (flatMessage != null) {
            if (!this.mqProperties.isFlatMessageOnlyData()) {       //发送完整数据
                records.add(new ProducerRecord<>(topicName, partition, null, JSON.toJSONBytes(flatMessage,
                        SerializerFeature.WriteMapNullValue)));
            } else {                                                //只发送data数据，用于支持Clickhouse同步kafka
                String messageType = flatMessage.getType();
                List<Map<String, String>> flatMessagePartData = flatMessage.getData();
                if (flatMessagePartData != null) {
                    if (messageType.equalsIgnoreCase("DELETE")) {   //delete情况下,需要额外处理
                        if (isFrequentTable(flatMessage)) {                    //CollapsingMergeTree加入-1字段表示删除
                            for (Map<String, String> partData : flatMessagePartData) {
                                partData.put("sign", "-1");
                                records.add(new ProducerRecord<>(topicName, partition, null, JSON.toJSONBytes(partData,
                                        SerializerFeature.WriteMapNullValue)));
                            }
                        } else {
                            execDelete(flatMessage);                         //非CollapsingMergeTree则需要通过执行SQL来delete
                        }
                    } else {
                        if (messageType.equalsIgnoreCase("INSERT") || messageType.equalsIgnoreCase("UPDATE")) {    //只处理insert和update
                            for (Map<String, String> partData : flatMessagePartData) {
                                if (isFrequentTable(flatMessage)) {          //CollapsingMergeTree需要额外处理
                                    if (messageType.equalsIgnoreCase("UPDATE")) {
                                        partData.put("sign", "-1");
                                        records.add(new ProducerRecord<>(topicName, partition, null, JSON.toJSONBytes(partData,
                                                SerializerFeature.WriteMapNullValue)));
                                    }
                                    partData.put("sign", "1");
                                }
                                records.add(new ProducerRecord<>(topicName, partition, null, JSON.toJSONBytes(partData,
                                        SerializerFeature.WriteMapNullValue)));
                            }
                        }
                    }
                }
            }
        }
    }


    /**
     * @Author XieChuangJian
     * @Description 判断insert前是否需要Delete
     * @Date 2022/3/21
     */
    private boolean needDeleteBeforeInsert(FlatMessage message, Map<String, String> partData) throws SQLException {
        if (ClickHouseClient.dataSource == null) {
            ClickHouseClient.init(this.mqProperties.getCkURL(),
                    this.mqProperties.getCkUsername(),
                    this.mqProperties.getCkPassword());
        }
        Connection connection = ClickHouseClient.dataSource.getConnection(60000);
        List<String> pkNames = message.getPkNames();
        String database = message.getDatabase();
        String selectPrefix = "select 1 from " + database + "." + message.getTable() + " where ";
        String selectSQL = appendCondition(pkNames, selectPrefix, partData);
        return !ClickHouseClient.isEmpty(selectSQL, connection);
    }

    /**
     * @Author XieChuangJian
     * @Description 判断是否是配置文件里配置的canal.ck.frequent.delete.tables
     * @Date 2022/3/21
     */
    private boolean isFrequentTable(FlatMessage message) {
        String tbFullName = message.getDatabase() + "." + message.getTable();
        for (String frequentDeleteTable : frequentDeleteTables) {
            if (frequentDeleteTable.equals(tbFullName))
                return true;
        }
        return false;
    }

    /**
     * @Author XieChuangJian
     * @Description 添加主键筛选条件
     * @Date 2022/3/21
     */
    private String appendCondition(List<String> pkNames, String deletePrefix, Map<String, String> data) {
        StringBuilder deleteBuilder = new StringBuilder();
        deleteBuilder.append(deletePrefix);
        for (String pkName : pkNames) {
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
        String table = flatMessagePart.getTable();
//        String localTable = flatMessagePart.getTable() + "_local";   //默认更改Clickhouse的本地表
//        String cluster = this.mqProperties.getCkClusterName();
        if (!ClickHouseClient.isExist(database, table, connection)) {    //判断该表是否存在，若不存在则返回
            return;
        }
        String selectPrefix = "select 1 from " + database + "." + table + " where ";
        String deletePrefix = "alter table " + database + "." + table + " delete where ";
        for (Map<String, String> data : dataList) {
            String selectSQL = appendCondition(pkNames, selectPrefix, data);
            String deleteSQL = appendCondition(pkNames, deletePrefix, data);
            int cnt = 0;
            while (ClickHouseClient.isEmpty(selectSQL, connection)) {    //一分钟内每隔3秒循环判断要删除的行是否存在
                cnt++;
                Thread.sleep(3000);
                if (cnt >= 20) {
                    logger.error("执行{}结果为空，要delete的数据行不存在，请检测数据同步情况！！！", selectSQL);
                    break;
//                    throw new SQLException("执行" + selectSQL + "结果为空，要delete的数据行不存在，请检测数据同步情况！！！");
                }
            }
            logger.warn("执行DELETE，SQL：{}", deleteSQL);
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
