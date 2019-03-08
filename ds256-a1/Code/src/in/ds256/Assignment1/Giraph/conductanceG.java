package in.ds256.Assignment1.Giraph;

import org.apache.giraph.edge.Edge;
import org.apache.giraph.edge.EdgeFactory;
import org.apache.giraph.graph.BasicComputation;
import org.apache.giraph.master.MasterCompute;
import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.giraph.aggregators.LongSumAggregator;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Random;

public class conductanceG extends BasicComputation<LongWritable, BooleanWritable, NullWritable, BooleanWritable> {

    @Override
    public void compute(Vertex<LongWritable, BooleanWritable, NullWritable> vertex, Iterable<BooleanWritable> messages) throws IOException {

        if (vertex.getId().get() == -1L) {
            vertex.voteToHalt();
        }
        else if (getSuperstep() == 0) {

            int replicate = Integer.parseInt(getContext().getConfiguration().get("replicate"));

            if (replicate == 1) {
                for (Edge<LongWritable, NullWritable> edge : vertex.getEdges()) {
                    addEdgeRequest(edge.getTargetVertexId(), EdgeFactory.create(vertex.getId()));
                }
            }
        }
        else if(getSuperstep() == 1) {

            Random rand = new Random();
            long degree = 0L;
            for (Edge<LongWritable, NullWritable> edge : vertex.getEdges()) {
                degree++;
            }
            vertex.setValue(new BooleanWritable(rand.nextInt(1000) % 3 == 0));
            if (vertex.getValue().get()) {
                aggregate("InDegree", new LongWritable(degree));
            }
            else {
                aggregate("InDegree", new LongWritable(degree));
            }

        }
        else if(getSuperstep() == 2) {
            for (Edge<LongWritable, NullWritable> edge : vertex.getEdges()) {
                sendMessage(edge.getTargetVertexId(), vertex.getValue());
            }
        }
        else if(getSuperstep() == 3) {
            long count = 0L;
            for (BooleanWritable message : messages) {
                if (message.get() ^ vertex.getValue().get()) {
                    count++;
                }
            }
            aggregate("crossEdges", new LongWritable(count));
        }
        else {
            vertex.voteToHalt();
        }
    }
}
