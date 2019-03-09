package in.ds256.Assignment1.Giraph;

import org.apache.giraph.edge.Edge;
import org.apache.giraph.edge.EdgeFactory;
import org.apache.giraph.graph.BasicComputation;
import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.NullWritable;
import java.io.IOException;
import java.util.Random;

public class conductanceG extends BasicComputation<LongWritable, BooleanWritable, NullWritable, LongWritable> {

    @Override
    public void compute(Vertex<LongWritable, BooleanWritable, NullWritable> vertex, Iterable<LongWritable> messages) throws IOException {

        if (vertex.getId().get() == -1L) {
            vertex.voteToHalt();
        }
        else if (getSuperstep() == 0) {
            for (Edge<LongWritable, NullWritable> edge : vertex.getEdges()) {
                sendMessage(edge.getTargetVertexId(), vertex.getId());
            }
        }
        else if (getSuperstep() == 1) {

            int replicate = Integer.parseInt(getContext().getConfiguration().get("replicate"));

            if(replicate==1) {
                for (LongWritable message : messages) {
                    vertex.addEdge(EdgeFactory.create(new LongWritable(message.get())));
                }
            }
        }
        else if(getSuperstep() == 2) {

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
        else if(getSuperstep() == 3) {
            for (Edge<LongWritable, NullWritable> edge : vertex.getEdges()) {
                sendMessage(edge.getTargetVertexId(), new LongWritable(vertex.getValue().get()?1L:0L));
            }
        }
        else if(getSuperstep() == 4) {
            long count = 0L;
            for (LongWritable message : messages) {
                if ((message.get() == 1L) ^ vertex.getValue().get()) {
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
