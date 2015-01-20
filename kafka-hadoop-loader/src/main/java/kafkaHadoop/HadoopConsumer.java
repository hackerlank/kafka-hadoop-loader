package kafkaHadoop;

import java.io.IOException;
import java.text.SimpleDateFormat;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.PosixParser;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * Created by wangxufeng on 2014/9/11.
 */
public class HadoopConsumer extends Configured implements Tool {

//    static {
//        Configuration.addDefaultResource("core-site.xml");
//        Configuration.addDefaultResource("mapred-site.xml");
//    }

    /**
     * Mapper for Kafka
     */
    public static class KafkaMapper extends Mapper<LongWritable, BytesWritable, LongWritable, Text> {
        @Override
        public void map(LongWritable key, BytesWritable value, Context context) throws IOException {
            Text out = new Text();
            try {
                out.set(value.getBytes(), 0, value.getLength());
                context.write(key, out);

            } catch (InterruptedException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        }
    }

    /**
     * Reducer for Kafka
     */
    public static class KafkaReducer extends Reducer<LongWritable, Text, LongWritable, Text> {
        private Text result = new Text();
        @Override
        public void reduce(LongWritable key, Iterable<Text> values, Context context) throws IOException {
            try {
                for (Text val : values) {
                    result.set(val);
                    context.write(key, result);
                }
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    public int run(String[] args) throws Exception {
//        ToolRunner.printGenericCommandUsage(System.err);
//
//        if (args.length < 2) {
//            ToolRunner.printGenericCommandUsage(System.err);
//            return -1;
//        }

        CommandLineParser parser = new PosixParser();
        Options options = buildOptions();

        CommandLine cmd = parser.parse(options, args);

        //String topic2process = cmd.getOptionValue("topic", "adrealtime,dailyremain,gbill,gcurrencypreserve,glogin," +
        //        "gloginreq,glvdist,gobj,gonline,greg,gscene,gtask");
        String topic2process = cmd.getOptionValue("topic", "gbill,mbill,mlevel, mlogin");

        String[] topicsList = topic2process.split(",");

        Boolean overall_success = true;
        Long taskStartTs = System.currentTimeMillis();
        SimpleDateFormat format = new SimpleDateFormat("yyyyMMdd");
        String subDir = format.format(taskStartTs);

        for (String topic: topicsList) {
            topic = topic.trim();

            System.out.println("=================================================================");
            System.out.println("==========Now Start Processing topic:" + topic + "==========");
            System.out.println("=================================================================");

            //        HelpFormatter formatter = new HelpFormatter();
            //        formatter.printHelp( "kafka.consumer.hadoop", options );

            //        Configuration conf = getConf();
            Configuration conf = new Configuration();

            String reducerNum = cmd.getOptionValue("reducer-num", "2");

            conf.set("kafka.topic", topic);
            conf.set("kafka.groupid", cmd.getOptionValue("consumer-group", "kafka2hdfa_id1"));
            conf.set("kafka.zk.connect", cmd.getOptionValue("zk-connect", "localhost:2182"));
            conf.set("Mapreduce.reducer.num", reducerNum);
            conf.set("Mapreduce.overalltask.startts", taskStartTs.toString());

            if (cmd.getOptionValue("autooffset-reset") != null)
                conf.set("kafka.autooffset.reset", cmd.getOptionValue("autooffset-reset"));
            conf.setInt("kafka.limit", Integer.valueOf(cmd.getOptionValue("limit", "-1")));

            conf.setBoolean("mapred.map.tasks.speculative.execution", true);

            Long curTs = System.currentTimeMillis();

            //        Job job = new Job(conf, "Kafka.Consumer");
            Job job = Job.getInstance(conf, "Kafka2hdfs_" + topic + "_" + curTs.toString());
            job.setJarByClass(getClass());
            job.setMapperClass(KafkaMapper.class);
            job.setCombinerClass(KafkaReducer.class);
            job.setReducerClass(KafkaReducer.class);
            // input
            job.setInputFormatClass(KafkaInputFormat.class);
            // output
            job.setOutputKeyClass(LongWritable.class);
            job.setOutputValueClass(Text.class);
            job.setOutputFormatClass(KafkaOutputFormat.class);

            job.setNumReduceTasks(Integer.parseInt(reducerNum));

            KafkaOutputFormat.setOutputPath(job, new Path(cmd.getArgs()[0] + '/' + subDir));

            boolean success = job.waitForCompletion(true);
            if (success) {
                String jobid = job.getJobID().toString();
                conf.set("Mapreduce.job.jobid", jobid);
                commit(conf);
            } else {
                overall_success = false;
                System.out.println("=================================================================");
                System.out.println("==========ERROR: topic process failure:" + topic + "==========");
                System.out.println("=================================================================");
            }
        }

        return overall_success ? 0: -1;
    }

    private void commit(Configuration conf) throws IOException {
        ZkUtils zk = new ZkUtils(conf);
        try {
            String topic = conf.get("kafka.topic");
            String group = conf.get("kafka.groupid");
            String startts = conf.get("Mapreduce.overalltask.startts");
            zk.commit(group, topic);
            zk.commitCurTs(topic, startts);
        } catch (Exception e) {
            rollback();
        } finally {
            zk.close();
        }
    }

    private void rollback() {
    }

    @SuppressWarnings("static-access")
    private Options buildOptions() {
        Options options = new Options();

        options.addOption(OptionBuilder.withArgName("topic")
                .withLongOpt("topic")
                .hasArg()
                .withDescription("kafka topic")
                .create("t"));
        options.addOption(OptionBuilder.withArgName("groupid")
                .withLongOpt("consumer-group")
                .hasArg()
                .withDescription("kafka consumer groupid")
                .create("g"));
        options.addOption(OptionBuilder.withArgName("zk")
                .withLongOpt("zk-connect")
                .hasArg()
                .withDescription("ZooKeeper connection String")
                .create("z"));

        options.addOption(OptionBuilder.withArgName("offset")
                .withLongOpt("autooffset-reset")
                .hasArg()
                .withDescription("Offset reset")
                .create("o"));

        options.addOption(OptionBuilder.withArgName("limit")
                .withLongOpt("limit")
                .hasArg()
                .withDescription("kafka limit")
                .create("l"));

        options.addOption(OptionBuilder.withArgName("reducernum")
                .withLongOpt("reducer-num")
                .hasArg()
                .withDescription("number of reducers to run")
                .create("rn"));

        return options;
    }

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new HadoopConsumer(), args);
        System.exit(exitCode);
    }

}

