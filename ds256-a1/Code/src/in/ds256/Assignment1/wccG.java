package in.ds256.Assignment1;

import org.apache.giraph.graph.BasicComputation;
import org.apache.giraph.edge.Edge;
import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.log4j.Logger;

import java.io.IOException;

public class wccG extends BasicComputation<LongWritable, LongWritable, NullWritable, LongWritable> {

    @Override
    public void compute(Vertex<LongWritable, LongWritable, NullWritable> vertex, Iterable<LongWritable> messages) throws IOException {

        if (getSuperstep() == 0) {
            vertex.setValue(vertex.getId());

            for (Edge<LongWritable, NullWritable> edge : vertex.getEdges()) {
                sendMessage(edge.getTargetVertexId(), vertex.getId());
            }
        }
        else{
            long maxValue = 0L;

            for (LongWritable message : messages) {
                maxValue = Math.max(maxValue, message.get());
            }

            if (maxValue > vertex.getValue().get()) {
                vertex.setValue(new LongWritable(maxValue));

                for (Edge<LongWritable, NullWritable> edge : vertex.getEdges()) {
                    sendMessage(edge.getTargetVertexId(), new LongWritable(maxValue));
                }
            }
        }

        vertex.voteToHalt();
    }
}