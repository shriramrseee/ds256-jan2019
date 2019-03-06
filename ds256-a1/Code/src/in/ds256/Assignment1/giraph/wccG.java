package in.ds256.Assignment1.Giraph;

import org.apache.giraph.graph.BasicComputation;
import org.apache.giraph.edge.Edge;
import org.apache.giraph.edge.EdgeFactory;
import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.log4j.Logger;

import java.io.IOException;

public class wccG extends BasicComputation<IntWritable, IntWritable, NullWritable, IntWritable> {

    @Override
    public void compute(Vertex<IntWritable, IntWritable, NullWritable> vertex, Iterable<IntWritable> messages) throws IOException {

        if (getSuperstep() == 0) {
            vertex.setValue(vertex.getId());

            for (Edge<IntWritable, NullWritable> edge : vertex.getEdges()) {
                addEdgeRequest(edge.getTargetVertexId(), EdgeFactory.create(vertex.getId(), null));
            }
        }
        else if(getSuperstep() == 1) {
            for (Edge<IntWritable, NullWritable> edge : vertex.getEdges()) {
                sendMessage(edge.getTargetVertexId(), vertex.getId());
            }

            vertex.voteToHalt();
        }
        else{
            int maxValue = 0;

            for (IntWritable message : messages) {
                maxValue = Math.max(maxValue, message.get());
            }

            if (maxValue > vertex.getValue().get()) {
                vertex.setValue(new IntWritable(maxValue));

                for (Edge<IntWritable, NullWritable> edge : vertex.getEdges()) {
                    sendMessage(edge.getTargetVertexId(), new IntWritable(maxValue));
                }
            }

            vertex.voteToHalt();
        }

    }
}