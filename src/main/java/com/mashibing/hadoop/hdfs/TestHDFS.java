package com.mashibing.hadoop.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.*;
import java.net.URI;

public class TestHDFS {

    public Configuration conf = null;
    public FileSystem fs = null;

    @Before
    public void connect() throws IOException, InterruptedException {
        conf = new Configuration(true);
//        fs = FileSystem.get(conf);
        fs = FileSystem.get(URI.create("hdfs://mycluster/"), conf, "god");
    }

    @Test
    public void mkdir() throws IOException {
        Path dir = new Path("/msb");
        if (fs.exists(dir)){
            fs.delete(dir, true);
        }
        fs.mkdirs(dir);
    }

    @Test
    public void upload() throws IOException {
        BufferedInputStream bufferedInputStream = new BufferedInputStream(new FileInputStream(new File("./data/hello.txt")));
        Path outFile = new Path("/msb/out.txt");
        FSDataOutputStream outPut = fs.create(outFile);
        IOUtils.copyBytes(bufferedInputStream, outPut, conf, true);
    }

    @After
    public void after() throws IOException {
        fs.close();
    }

}
