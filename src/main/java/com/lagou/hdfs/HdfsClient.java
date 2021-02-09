package com.lagou.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

public class HdfsClient {
    FileSystem fs;

    @Before
    public void init() throws URISyntaxException, IOException, InterruptedException {

        Configuration configuration = new Configuration();
        // 配置在集群上运行
        // configuration.set("fs.defaultFS", "hdfs://linux121:9000");
        // FileSystem fs = FileSystem.get(configuration);
        fs = FileSystem.get(new URI("hdfs://linux121:9000"), configuration, "root"); // 2 创建目录
    }

    @After
    public void destory() throws IOException {
        fs.close();
    }



    @Test
    public void testMkdirs() throws IOException {

        fs.mkdirs(new Path("/api_test/03"));

    }
}
