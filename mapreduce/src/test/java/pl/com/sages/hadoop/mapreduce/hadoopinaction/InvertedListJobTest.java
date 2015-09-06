package pl.com.sages.hadoop.mapreduce.hadoopinaction;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class InvertedListJobTest {

    @Rule
    public TemporaryFolder inputFolder = new TemporaryFolder();
    @Rule
    public TemporaryFolder outputFolder = new TemporaryFolder();

    private String inputPath;
    private String outputPath;
    private FileSystem fileSystem;
    private Configuration configuration;

    @Before
    public void before() throws IOException {
        inputPath = inputFolder.getRoot().getAbsolutePath();
        outputPath = outputFolder.getRoot().getAbsolutePath();
        configuration = new Configuration();
        fileSystem = FileSystem.get(configuration);

        FileUtils.copyInputStreamToFile(getClass().getResourceAsStream("/cite75_99_input.txt"), inputFolder.newFile());
        fileSystem.delete(new Path(outputPath), true);
    }

    @Test
    public void shouldInvertWords() throws Exception {
        //when
        int exitCode = new InvertedListJob(configuration).run(new String[]{inputPath, outputPath});

        //then
        assertThat(exitCode).isEqualTo(0);
        assertTrue(fileSystem.exists(new Path(outputPath + "/_SUCCESS")));
        String outputFile = outputPath + "/part-r-00000";
        assertTrue(fileSystem.exists(new Path(outputFile)));

        File expectedFile = Paths.get(getClass().getResource("/cite75_99_expected.txt").toURI()).toFile();
        assertThat(new File(outputFile)).hasContentEqualTo(expectedFile);
    }
}