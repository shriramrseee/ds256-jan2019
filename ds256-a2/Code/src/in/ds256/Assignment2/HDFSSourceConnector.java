package in.ds256.Assignment2;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.common.utils.AppInfoParser;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.source.SourceConnector;

public class HDFSSourceConnector extends SourceConnector {
    public static final String TOPIC_CONFIG = "topic";
    public static final String FILE_CONFIG = "file";
    public static final String TASK_BATCH_SIZE_CONFIG = "batch.size";
    public static final int DEFAULT_TASK_BATCH_SIZE = 2000;
    private static final ConfigDef CONFIG_DEF;
    private String filename;
    private String topic;
    private int batchSize;
    private int delay;

    public HDFSSourceConnector() {
    }

    public String version() {
        return AppInfoParser.getVersion();
    }

    public void start(Map<String, String> props) {
        AbstractConfig parsedConfig = new AbstractConfig(CONFIG_DEF, props);
        this.filename = parsedConfig.getString("file");
        List<String> topics = parsedConfig.getList("topic");
        if (topics.size() != 1) {
            throw new ConfigException("'topic' in FileStreamSourceConnector configuration requires definition of a single topic");
        } else {
            this.topic = (String)topics.get(0);
            this.batchSize = parsedConfig.getInt("batch.size");
            this.delay = parsedConfig.getInt("delay");
        }
    }

    public Class<? extends Task> taskClass() {
        return HDFSSourceTask.class;
    }

    public List<Map<String, String>> taskConfigs(int maxTasks) {
        ArrayList<Map<String, String>> configs = new ArrayList();
        Map<String, String> config = new HashMap();
        if (this.filename != null) {
            config.put("file", this.filename);
        }

        config.put("topic", this.topic);
        config.put("batch.size", String.valueOf(this.batchSize));
        config.put("delay", String.valueOf(this.delay));
        configs.add(config);
        return configs;
    }

    public void stop() {
    }

    public ConfigDef config() {
        return CONFIG_DEF;
    }

    static {
        CONFIG_DEF = (new ConfigDef()).define("file", Type.STRING, (Object)null, Importance.HIGH, "Source filename. If not specified, the standard input will be used").define("topic", Type.LIST, Importance.HIGH, "The topic to publish data to").define("batch.size", Type.INT, 2000, Importance.LOW, "The maximum number of records the Source task can read from file one time").define("delay", Type.INT, 1000, Importance.HIGH, "The delay between two batches");
    }
}
