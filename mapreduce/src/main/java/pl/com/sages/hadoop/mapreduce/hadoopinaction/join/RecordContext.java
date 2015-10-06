package pl.com.sages.hadoop.mapreduce.hadoopinaction.join;

import org.apache.hadoop.contrib.utils.join.TaggedMapOutput;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class RecordContext extends TaggedMapOutput {

    private Text data = new Text("");

    // for serialization purposes
    public RecordContext() {
    }

    public RecordContext(Text data, Text tag) {
        this.data = data;
        this.tag = tag;
    }

    public RecordContext(String data, String tag) {
        this(new Text(data), new Text(tag));
    }

    @Override
    public Writable getData() {
        return data;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        this.tag.write(dataOutput);
        this.data.write(dataOutput);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.tag.readFields(dataInput);
        this.data.readFields(dataInput);
    }
}
