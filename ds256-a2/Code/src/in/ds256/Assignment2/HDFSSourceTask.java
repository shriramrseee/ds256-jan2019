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
    private InputStream stream;
    private BufferedReader reader = null;
    private char[] buffer = new char[1024];
    private int offset = 0;
    private String topic = null;
    private int batchSize = 2000;
    private Long streamOffset;

    public HDFSSourceTask() {
    }

    public String version() {
        return (new HDFSSourceConnector()).version();
    }

    public void start(Map<String, String> props) {
        this.filename = (String)props.get("file");
        if (this.filename == null || this.filename.isEmpty()) {
            this.stream = System.in;
            this.streamOffset = null;
            this.reader = new BufferedReader(new InputStreamReader(this.stream, StandardCharsets.UTF_8));
        }

        this.topic = (String)props.get("topic");
        this.batchSize = Integer.parseInt((String)props.get("batch.size"));
    }

    public List<SourceRecord> poll() throws InterruptedException {
        if (this.stream == null) {
            try {
                this.stream = Files.newInputStream(Paths.get(this.filename));
                Map<String, Object> offset = this.context.offsetStorageReader().offset(Collections.singletonMap("filename", this.filename));
                if (offset != null) {
                    Object lastRecordedOffset = offset.get("position");
                    if (lastRecordedOffset != null && !(lastRecordedOffset instanceof Long)) {
                        throw new ConnectException("Offset position is the incorrect type");
                    }

                    if (lastRecordedOffset != null) {
                        log.debug("Found previous offset, trying to skip to file offset {}", lastRecordedOffset);
                        long skipLeft = (Long)lastRecordedOffset;

                        while(skipLeft > 0L) {
                            try {
                                long skipped = this.stream.skip(skipLeft);
                                skipLeft -= skipped;
                            } catch (IOException var13) {
                                log.error("Error while trying to seek to previous offset in file {}: ", this.filename, var13);
                                throw new ConnectException(var13);
                            }
                        }

                        log.debug("Skipped to offset {}", lastRecordedOffset);
                    }

                    this.streamOffset = lastRecordedOffset != null ? (Long)lastRecordedOffset : 0L;
                } else {
                    this.streamOffset = 0L;
                }

                this.reader = new BufferedReader(new InputStreamReader(this.stream, StandardCharsets.UTF_8));
                log.debug("Opened {} for reading", this.logFilename());
            } catch (NoSuchFileException var15) {
                log.warn("Couldn't find file {} for FileStreamSourceTask, sleeping to wait for it to be created", this.logFilename());
                synchronized(this) {
                    this.wait(1000L);
                    return null;
                }
            } catch (IOException var16) {
                log.error("Error while trying to open file {}: ", this.filename, var16);
                throw new ConnectException(var16);
            }
        }

        try {
            BufferedReader readerCopy;
            synchronized(this) {
                readerCopy = this.reader;
            }

            if (readerCopy == null) {
                return null;
            } else {
                ArrayList<SourceRecord> records = null;
                int nread = 0;

                while(true) {
                    do {
                        if (!readerCopy.ready()) {
                            if (nread <= 0) {
                                synchronized(this) {
                                    this.wait(1000L);
                                }
                            }

                            return records;
                        }

                        nread = readerCopy.read(this.buffer, this.offset, this.buffer.length - this.offset);
                        log.trace("Read {} bytes from {}", nread, this.logFilename());
                    } while(nread <= 0);

                    this.offset += nread;
                    if (this.offset == this.buffer.length) {
                        char[] newbuf = new char[this.buffer.length * 2];
                        System.arraycopy(this.buffer, 0, newbuf, 0, this.buffer.length);
                        this.buffer = newbuf;
                    }

                    while(true) {
                        String line = this.extractLine();
                        if (line != null) {
                            log.trace("Read a line from {}", this.logFilename());
                            if (records == null) {
                                records = new ArrayList();
                            }

                            records.add(new SourceRecord(this.offsetKey(this.filename), this.offsetValue(this.streamOffset), this.topic, (Integer)null, (Schema)null, (Object)null, VALUE_SCHEMA, line, System.currentTimeMillis()));
                            if (records.size() >= this.batchSize) {
                                return records;
                            }
                        }

                        if (line == null) {
                            break;
                        }
                    }
                }
            }
        } catch (IOException var14) {
            return null;
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
