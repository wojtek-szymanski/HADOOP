package pl.com.sages.hadoop.mapreduce.hadoopinaction;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class InvertedListJob extends Configured implements Tool {

    public InvertedListJob() {
    }

    public InvertedListJob(Configuration conf) {
        super(conf);
    }

    @Override
    public int run(String[] args) throws Exception {
        Configuration configuration = getConf();
        configuration.set("mapreduce.input.keyvaluelinerecordreader.key.value.separator", ",");
        Job job = Job.getInstance(configuration, "Inverted List Job");
        Path in = new Path(args[0]);
        Path out = new Path(args[1]);
        FileInputFormat.setInputPaths(job, in);
        FileOutputFormat.setOutputPath(job, out);
        job.setMapperClass(InvertedListMapper.class);
        job.setReducerClass(InvertedListReducer.class);
        job.setInputFormatClass(KeyValueTextInputFormatWithHeader.class);
        // job.setOutputFormatClass(TextOutputFormat.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new InvertedListJob(), args);
        System.exit(res);
    }
}