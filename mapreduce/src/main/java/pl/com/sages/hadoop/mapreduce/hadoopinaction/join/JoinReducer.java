package pl.com.sages.hadoop.mapreduce.hadoopinaction.join;

import org.apache.hadoop.contrib.utils.join.DataJoinReducerBase;
import org.apache.hadoop.contrib.utils.join.TaggedMapOutput;
import org.apache.hadoop.io.Text;

import java.util.Arrays;

public class JoinReducer extends DataJoinReducerBase {

    /**
     * Performs inner join on the following data sets:
     * tags = {“customers”, “orders”}
     * values = {“3,Jose Madriz,281-330-8004”, “3,A,12.95,02-Jun-2008”}
     */
    @Override
    protected TaggedMapOutput combine(Object[] tags, Object[] values) {
        if (tags.length < 2) {
            return null;
        }
        String customer = readRecord(tags, values, "customers");
        String order = readRecord(tags, values, "orders");
        String customerOrder = customer + "," + order;
        return new RecordContext(customerOrder, "customers_orders");
    }

    private String readRecord(Object[] tags, Object[] values, String name) {
        int index = indexOf(tags, name);
        String order = record(values[index]);
        String[] orderColumns = order.split(",", 2);
        return orderColumns[1];
    }

    private int indexOf(Object[] tags, String name) {
        return Arrays.asList(tags).indexOf(new Text(name));
    }

    private String record(Object value) {
        return ((RecordContext) value).getData().toString();
    }
}
