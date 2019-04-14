package in.ds256.Assignment2;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HDFSSourceTask extends SourceTask {
    private static final Logger log = LoggerFactory.getLogger(HDFSSourceTask.class);
    public static final String FILENAME_FIELD = "filename";
    public static final String POSITION_FIELD = "position";
    private static final Schema VALUE_SCHEMA;
    private String filename;
    private FSDataInputStream stream;
    private BufferedReader reader = null;
    private char[] buffer = new char[1024];
    private int offset = 0;
    private String topic = null;
    private int batchSize = 2000;
    private Long streamOffset;
    private FileSystem fs = null;
    private int delay = 1000;
    private FileStatus[] files = null;
    private int fileno = -1;

    public HDFSSourceTask() {
    }

    public String version() {
        return (new HDFSSourceConnector()).version();
    }

    public void start(Map<String, String> props) {
        this.filename = (String)props.get("file");
        Configuration conf = new Configuration();
        conf.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
        conf.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());
        Path path = new Path(this.filename);
        try {
            this.fs = path.getFileSystem(conf);
            this.files = fs.globStatus(path);
            this.streamOffset = null;
        } catch (IOException e) {
            log.error(e.getMessage());
        }
        this.topic = (String)props.get("topic");
        this.batchSize = Integer.parseInt((String)props.get("batch.size"));
        this.delay =  Integer.parseInt((String)props.get("delay"));
    }

    public List<SourceRecord> poll() throws InterruptedException {

        if(this.files.length == 0) {
            return null;
        }
        else if(this.fileno == -1){
            this.fileno++;
            try {
                this.stream = fs.open(this.files[fileno].getPath());
                this.reader = new BufferedReader(new InputStreamReader(this.stream, StandardCharsets.UTF_8));
            } catch (IOException e) {
                log.error(e.getMessage());
                return null;
            }
        }

        ArrayList<SourceRecord> records = new ArrayList<>();

        while(true) {
            String line = null;
            try {
                line = this.reader.readLine();
            } catch (IOException e) {
               return null;
            }
            if (line != null) {
                records.add(new SourceRecord(this.offsetKey(this.filename), this.offsetValue(this.streamOffset), this.topic, (Integer)null, (Schema)null, (Object)null, VALUE_SCHEMA, line, System.currentTimeMillis()));
                if (records.size() >= this.batchSize) {
                    synchronized (this) {
                        this.wait(this.delay);
                    }
                    return records;
                }
            }
            if (records.size() > 0 && line == null)
                return records;
            else if (line == null) {
               this.fileno++;
                if(this.fileno >= this.files.length) {
                    this.fileno = 0;
                    try {
                        this.stream = fs.open(this.files[fileno].getPath());
                        this.reader = new BufferedReader(new InputStreamReader(this.stream, StandardCharsets.UTF_8));
                    } catch (IOException e) {
                        log.error(e.getMessage());
                        return null;
                    }
                }
                else {
                    try {
                        this.stream = fs.open(this.files[fileno].getPath());
                        this.reader = new BufferedReader(new InputStreamReader(this.stream, StandardCharsets.UTF_8));
                    } catch (IOException e) {
                        log.error(e.getMessage());
                        return null;
                    }
                }
            }
        }
    }

    private String extractLine() {
        int until = -1;
        int newStart = -1;

        for(int i = 0; i < this.offset; ++i) {
            if (this.buffer[i] == '\n') {
                until = i;
                newStart = i + 1;
                break;
            }

            if (this.buffer[i] == '\r') {
                if (i + 1 >= this.offset) {
                    return null;
                }

                until = i;
                newStart = this.buffer[i + 1] == '\n' ? i + 2 : i + 1;
                break;
            }
        }

        if (until != -1) {
            String result = new String(this.buffer, 0, until);
            System.arraycopy(this.buffer, newStart, this.buffer, 0, this.buffer.length - newStart);
            this.offset -= newStart;
            if (this.streamOffset != null) {
                this.streamOffset = this.streamOffset + (long)newStart;
            }

            return result;
        } else {
            return null;
        }
    }

    public void stop() {
        log.trace("Stopping");
        synchronized(this) {
            try {
                if (this.stream != null && this.stream != System.in) {
                    this.stream.close();
                    log.trace("Closed input stream");
                }
            } catch (IOException var4) {
                log.error("Failed to close FileStreamSourceTask stream: ", var4);
            }

            this.notify();
        }
    }

    private Map<String, String> offsetKey(String filename) {
        return Collections.singletonMap("filename", filename);
    }

    private Map<String, Long> offsetValue(Long pos) {
        return Collections.singletonMap("position", pos);
    }

    private String logFilename() {
        return this.filename == null ? "stdin" : this.filename;
    }

    static {
        VALUE_SCHEMA = Schema.STRING_SCHEMA;
    }
}
