package pl.com.sages.hadoop.mapreduce.hadoopinaction;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.KeyValueLineRecordReader;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;

import java.io.IOException;

public class KeyValueTextInputFormatWithHeader extends KeyValueTextInputFormat {

    @Override
    public RecordReader<Text, Text> createRecordReader(InputSplit genericSplit, TaskAttemptContext context) throws IOException {
        context.setStatus(genericSplit.toString());
        return new KeyValueLineRecordReaderWithHeader(context.getConfiguration());
    }

    private class KeyValueLineRecordReaderWithHeader extends KeyValueLineRecordReader {
        public KeyValueLineRecordReaderWithHeader(Configuration conf) throws IOException {
            super(conf);
        }

        /* reads all lines starting with '#' */
        @Override
        public void initialize(InputSplit genericSplit, TaskAttemptContext context) throws IOException {
            super.initialize(genericSplit, context);
            if (nextKeyValue()) {
                String header = getCurrentValue().toString();
                if (!header.startsWith("\"")) {
                    throw new IllegalArgumentException("No file header found");
                }
            }
        }
    }
}
