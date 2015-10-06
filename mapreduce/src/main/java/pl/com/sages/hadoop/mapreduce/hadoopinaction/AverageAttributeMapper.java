package pl.com.sages.hadoop.mapreduce.hadoopinaction;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class AverageAttributeMapper extends Mapper<LongWritable, Text, Text, Text> {

    private Text countryText = new Text();
    private Text claimsText = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        Configuration configuration = context.getConfiguration();
        String fields[] = value.toString().split(",", -1);
        String country = fields[configuration.getInt("countryIndex", -1)];
        String claims = fields[configuration.getInt("claimsIndex", -1)];
        if (claims.length() > 0 && !claims.startsWith("\\")) {
            countryText.set(country);
            claimsText.set(claims + ",1");
            context.write(countryText, claimsText);
        }
    }
}
