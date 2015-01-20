package kafkaHadoop;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileAlreadyExistsException;
import org.apache.hadoop.mapred.InvalidJobConfException;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.security.TokenCache;

/**
 * Created by wangxufeng on 2014/9/11.
 */
public class KafkaOutputFormat<K, V> extends TextOutputFormat<K, V> {

    /**
     * 获取默认的输出路径和文件名称（见FileOutputFormat）
     * @param context
     * @param extension
     * @return
     * @throws IOException
     */
    public Path getDefaultWorkFile(TaskAttemptContext context,
                                   String extension) throws IOException {
        FileOutputCommitter committer =
                (FileOutputCommitter) getOutputCommitter(context);
        JobID jobId = context.getJobID();

        Configuration conf = context.getConfiguration();
        String topic = conf.get("kafka.topic");
        String startts = conf.get("Mapreduce.overalltask.startts");

        return new Path(committer.getWorkPath(),
                getUniqueFile(context, topic + "-" + startts + "-part-" + jobId.toString().replace("job_", ""),
                        extension));
    }

    /**
     * 判断输出目录是否存在;获取输出目录文件系统的授权token
     * @param job
     * @throws FileAlreadyExistsException
     * @throws IOException
     */
    public void checkOutputSpecs(JobContext job) throws FileAlreadyExistsException, IOException{
        Path outDir = getOutputPath(job);
        if (outDir == null) {
            throw new InvalidJobConfException("Output directory not set.");
        }

        // get delegation token for outDir's file system
        TokenCache.obtainTokensForNamenodes(job.getCredentials(),
                new Path[] {outDir},
                job.getConfiguration());

    }
}
