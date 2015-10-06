package pl.com.sages.hadoop.mapreduce.hadoopinaction.join;

import org.apache.commons.io.FilenameUtils;
import org.apache.hadoop.contrib.utils.join.DataJoinMapperBase;
import org.apache.hadoop.contrib.utils.join.TaggedMapOutput;
import org.apache.hadoop.io.Text;

public class JoinMapper extends DataJoinMapperBase {

    @Override
    protected Text generateInputTag(String inputFile) {
        String[] dataSource = inputFile.split("-");
        String baseFileName = FilenameUtils.getBaseName(dataSource[0]);
        return new Text(baseFileName);
    }

    @Override
    protected TaggedMapOutput generateTaggedMapOutput(Object data) {
        return new RecordContext((Text) data, inputTag);
    }

    @Override
    protected Text generateGroupKey(TaggedMapOutput taggedMapOutput) {
        String line = taggedMapOutput.getData().toString();
        String[] columns = line.split(",");
        String customerId = columns[0];
        return new Text(customerId);
    }
}
