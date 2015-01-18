package kafkaHadoop;

import java.io.Closeable;
import java.io.IOException;
import java.util.*;

import org.json.JSONArray;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.exception.ZkMarshallingError;
import org.I0Itec.zkclient.exception.ZkNoNodeException;
import org.I0Itec.zkclient.serialize.ZkSerializer;
import org.apache.hadoop.conf.Configuration;

/**
 * Created by wangxufeng on 2014/9/11.
 */
public class ZkUtils implements Closeable {

    private static Logger LOG = LoggerFactory.getLogger(ZkUtils.class);

    private static final String CONSUMERS_PATH = "/consumers";
    private static final String BROKER_IDS_PATH = "/brokers/ids";
    private static final String BROKER_TOPICS_PATH = "/brokers/topics";

//    class ZKGroupDirs(val group: String) {
//      def consumerDir = ZkUtils.ConsumersPath
//      def consumerGroupDir = consumerDir + "/" + group
//      def consumerRegistryDir = consumerGroupDir + "/ids"
//    }
//
//    class ZKGroupTopicDirs(group: String, topic: String) extends ZKGroupDirs(group) {
//      def consumerOffsetDir = consumerGroupDir + "/offsets/" + topic
//      def consumerOwnerDir = consumerGroupDir + "/owners/" + topic
//    }

    /**
     * zookeeper客户端对象
     */
    private ZkClient client;
    /**
     * kafka brokers列表
     */
    Map<String, String> brokers;


    public ZkUtils(Configuration config) {
        connect(config);
    }

    /**
     * 连接zookeeper
     * @param config
     */
    private void connect(Configuration config) {
        String zk = config.get("kafka.zk.connect");
        int stimeout = config.getInt("kafka.zk.sessiontimeout.ms", 10000);
        int ctimeout = config.getInt("kafka.zk.connectiontimeout.ms", 10000);

        client = new ZkClient(zk, stimeout, ctimeout, new StringSerializer() );
    }

    /**
     * 获取broker列表
     * @param id
     * @return
     */
    public String getBroker(String id) {
        if (brokers == null) {
            brokers = new HashMap<String, String>();
            List<String> brokerIds = getChildrenParentMayNotExist(BROKER_IDS_PATH);

            for(String bid: brokerIds) {
                String data = client.readData(BROKER_IDS_PATH + "/" + bid);
                LOG.info("Broker " + bid + " " + data);
//                brokers.put(bid, data.split(":", 2)[1]);
//                String[] binfo = data.split(",");
//                String host_str = binfo[2];
//                String[] hostinfo = host_str.split(":");
//                String host = hostinfo[1];
//                brokers.put(bid, host + ":" + data.split(":", 6)[5]);
                JSONObject jsonObject = new JSONObject(data);
                String host = jsonObject.getString("host");
                Integer port = jsonObject.getInt("port");
                brokers.put(bid, host + ":" + port.toString());
            }
        }
        return brokers.get(id);
    }

    /**
     * 获取分区信息
     * @param topic
     * @return
     */
    public List<String> getPartitions(String topic) {
        List<String> partitions = new ArrayList<String>();

        String topicRegInfo = client.readData(BROKER_TOPICS_PATH + "/" + topic);
        JSONObject jsonObject = new JSONObject(topicRegInfo);
        JSONObject partitionObject = jsonObject.getJSONObject("partitions");
        Iterator partitionIterator = partitionObject.keys();
        while (partitionIterator.hasNext()) {
            String partitionId = (String) partitionIterator.next();
            JSONArray pvalue = partitionObject.getJSONArray(partitionId);

            for (int i = 0; i < pvalue.length(); i++) {
                Integer brokerId = (Integer) pvalue.get(i);
                partitions.add(brokerId + "-" + partitionId);
            }
        }
        return partitions;
//System.out.println(client.readData(BROKER_TOPICS_PATH+"/test2"));
////System.out.println(client.readData(BROKER_IDS_PATH+"/0"));
//
////String temp = "testtesttest";
////client.createPersistent("/aabbccddee", temp);
////String readBytes = client.readData("/aabbccddee");
////System.out.println(readBytes);
//
//        List<String> partitions = new ArrayList<String>();
//        List<String> brokersTopics = getChildrenParentMayNotExist(BROKER_TOPICS_PATH + "/" + topic);
//System.out.println("----------------------1------------------------");
//System.out.println("+++++" + BROKER_TOPICS_PATH + "/" + topic);
//System.out.println(brokersTopics);
//System.out.println("----------------------2------------------------");
//
//
//        for(String broker: brokersTopics) {
//            String parts = client.readData(BROKER_TOPICS_PATH + "/" + topic + "/" + broker);
//            //String parts = client.readData(BROKER_TOPICS_PATH + "/" + topic + "/partitions/" + broker);
//System.out.println("----------------------3------------------------");
//System.out.println(BROKER_TOPICS_PATH + "/" + topic + "/" + broker);
//System.out.println(parts);
//System.out.println("----------------------4------------------------");
//            for(int i =0; i< Integer.valueOf(parts); i++) {
//                partitions.add(broker + "-" + i);
//            }
//        }
//System.out.println("^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^");
//System.out.println(partitions);
//        return partitions;
    }

    /**
     * 获取zookeeper最近提交的偏移值
     * @param group
     * @param topic
     * @param partition
     * @return
     */
    public long getLastCommit(String group, String topic, String partition) {
        String znode = getOffsetsPath(group ,topic ,partition);
        String offset = client.readData(znode, true);

        if (offset == null) {
            return -1L;
        }
        return Long.valueOf(offset);
    }

    /**
     * zookeeper设置最新偏移值
     * @param group
     * @param topic
     * @param partition
     * @param commit
     * @param temp
     */
    public void setLastCommit(String group, String topic, String partition, long commit, boolean temp) {
        String path = temp? getTempOffsetsPath(group ,topic ,partition)
                : getOffsetsPath(group ,topic ,partition);
        if (!client.exists(path)) {
            client.createPersistent(path, true);            //zookeeper创建永久节点
        }
        client.writeData(path, commit);                     //zookeeper往节点写数据
    }

    /**
     * zookeeper提交偏移值
     * @param group
     * @param topic
     * @return
     */
    public boolean commit(String group, String topic) {
        List<String> partitions = getChildrenParentMayNotExist(getTempOffsetsPath(group, topic));
        for(String partition: partitions) {
            String path = getTempOffsetsPath(group, topic, partition);
            String offset = client.readData(path);

            setLastCommit(group, topic, partition, Long.valueOf(offset), false);
            client.delete(path);
        }
        return true;
    }

    @Override
    public void close() throws IOException {
        if (client != null) {
            client.close();
        }
    }


    /**
     * zookeeper获取偏移路径
     * @param group
     * @param topic
     * @param partition
     * @return
     */
    private String getOffsetsPath(String group, String topic, String partition) {
        return CONSUMERS_PATH + "/" + group + "/offsets/" + topic + "/" + partition;
    }

    /**
     * zookeeper获取临时偏移路径（精确到分区）
     * @param group
     * @param topic
     * @param partition
     * @return
     */
    private String getTempOffsetsPath(String group, String topic, String partition) {
        return CONSUMERS_PATH + "/" + group + "/offsets-temp/" + topic + "/" + partition;
    }

    /**
     * zookeeper获取临时偏移路径（精确到主题）
     * @param group
     * @param topic
     * @return
     */
    private String getTempOffsetsPath(String group, String topic) {
        return CONSUMERS_PATH + "/" + group + "/offsets-temp/" + topic ;
    }

    /**
     * zookeeper获取对应路径的儿子节点
     * @param path
     * @return
     */
    private List<String> getChildrenParentMayNotExist(String path) {
        try {
            List<String> children = client.getChildren(path);
            return children;
        } catch (ZkNoNodeException e) {
            return new ArrayList<String>();
        }
    }


    static class StringSerializer implements ZkSerializer {
        public StringSerializer() {}
        @Override
        public Object deserialize(byte[] data) throws ZkMarshallingError {
            if (data == null) return null;
            return new String(data);
        }

        @Override
        public byte[] serialize(Object data) throws ZkMarshallingError {
            return data.toString().getBytes();
        }

    }

}
