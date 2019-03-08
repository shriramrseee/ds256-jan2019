package in.ds256.Assignment1.Giraph.graphReader;

import org.apache.giraph.io.EdgeReader;
import org.apache.giraph.io.formats.TextEdgeInputFormat;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;
import java.util.regex.Pattern;



public class graphReaderThree extends TextEdgeInputFormat<LongWritable, NullWritable> {
    private static final Pattern SEPARATOR = Pattern.compile("[ ]");

    public graphReaderThree() {
    }

    public EdgeReader<LongWritable, NullWritable> createEdgeReader(InputSplit split, TaskAttemptContext context) throws IOException {
        return new graphReaderThree.graphReaderThreeReader();
    }

    public class graphReaderThreeReader extends TextEdgeInputFormat<LongWritable, NullWritable>.TextEdgeReaderFromEachLineProcessed<LongPair> {
        public graphReaderThreeReader() {
        }

        protected LongPair preprocessLine(Text line) throws IOException {
            String[] tokens = graphReaderThree.SEPARATOR.split(line.toString());
            try {
                if (tokens.length >= 2)
                    return new LongPair(Long.parseLong(tokens[1]), Long.parseLong(tokens[2]));
                else
                    return new LongPair(-1L, -1L);
            }
            catch (NumberFormatException n){
                return new LongPair(-1L, -1L);
            }

        }

        protected LongWritable getSourceVertexId(LongPair endpoints) throws IOException {
            return new LongWritable(endpoints.getFirst());
        }

        protected LongWritable getTargetVertexId(LongPair endpoints) throws IOException {
            return new LongWritable(endpoints.getSecond());
        }

        protected NullWritable getValue(LongPair endpoints) throws IOException {
            return NullWritable.get();
        }
    }

}

