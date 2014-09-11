package kafkaHadoop;

import java.util.Properties;

import kafka.consumer.ConsumerConfig;
import org.apache.hadoop.conf.Configuration;

/**
 * Created by wangxufeng on 2014/9/11.
 */
public class ConsumerUtils {

    public static ConsumerConfig getConfiguration(Configuration conf) {
        Properties props = new Properties();
        props.put("zk.connect", conf.get("kafka.zk.connect","localhost:2182"));
        props.put("zk.connectiontimeout.ms", conf.get("kafka.zk.connectiontimeout.ms","1000000"));

        ConsumerConfig csConfig = new ConsumerConfig(props);
        return csConfig;
    }
}
