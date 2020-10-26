package com.flink.source;

import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.io.*;
import java.sql.Time;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.zip.GZIPInputStream;

/**
 * 自定义Flink Source
 * 读取gz数据模拟发送
 * @author tang
 */
public class GzpFileSource implements SourceFunction {
    // 文件路径
    private String filePath;

    public GzpFileSource(String path) {
        this.filePath = path;
    }

    private Random random = new Random();
    private InputStream source;
    private BufferedReader reader;

    @Override
    public void run(SourceContext sourceContext) throws Exception {
        // 发送数据
        // 读取gz文件
        source = new GZIPInputStream(new FileInputStream(filePath));
        reader = new BufferedReader(new InputStreamReader(source));
        String line = null;
        while ((line = reader.readLine()) != null) {
            // 模拟发送
            TimeUnit.MILLISECONDS.sleep(random.nextInt(500));
            // 发送数据
            sourceContext.collect(line);
        }
        reader.close();
        reader = null;
        source.close();
        source = null;
    }

    /**
     * job取消时关闭数据
     */
    @Override
    public void cancel() {
        // 关闭资源
        try {
            if (reader != null) {
                reader.close();
            }
            if (source != null) {
                source.close();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
