package pl.com.sages.hadoop.mapreduce.hadoopinaction.join;

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
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertTrue;

public class JoinJobTest {

    @Rule
    public TemporaryFolder inputFolder = new TemporaryFolder();
    @Rule
    public TemporaryFolder orderFolder = new TemporaryFolder();
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

        FileUtils.copyInputStreamToFile(getClass().getResourceAsStream("/customers.txt"), inputFolder.newFile("customers.txt"));
        FileUtils.copyInputStreamToFile(getClass().getResourceAsStream("/orders.txt"), inputFolder.newFile("orders.txt"));
        fileSystem.delete(new Path(outputPath), true);
    }

    @Test
    public void shouldJoinCustomersAndOrders() throws Exception {
        // given
        JoinJob job = new JoinJob(configuration);

        // when
        int exitCode = job.run(new String[]{inputPath, outputPath});

        // then
        assertThat(exitCode).isZero();
        assertTrue(fileSystem.exists(new Path(outputPath + "/_SUCCESS")));
        String outputFileName = outputPath + "/part-00000";
        assertTrue(fileSystem.exists(new Path(outputFileName)));

        File outputFile = new File(outputFileName);
        File expectedFile = Paths.get(getClass().getResource("/customers_orders_expected.txt").toURI()).toFile();

        // reducer output records may be in different order than records in expected file
        assertThat(readLines(outputFile)).containsOnly(readLines(expectedFile));
    }

    private String[] readLines(File file) throws IOException {
        List<String> lines = FileUtils.readLines(file);
        return lines.toArray(new String[lines.size()]);
    }
}