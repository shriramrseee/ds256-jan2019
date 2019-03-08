package in.ds256.Assignment1.Giraph;

import org.apache.giraph.edge.EdgeFactory;
import org.apache.giraph.graph.BasicComputation;
import org.apache.giraph.edge.Edge;
import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.ArrayWritable;

import java.io.IOException;

public class prG extends BasicComputation<LongWritable, ArrayWritable, NullWritable, DoubleWritable> {

    @Override
    public void compute(Vertex<LongWritable, ArrayWritable, NullWritable> vertex, Iterable<DoubleWritable> messages) throws IOException {

        if(vertex.getId().get() == -1L) {
            vertex.voteToHalt();
        }
        else if (getSuperstep() == 0) {

            // [OldPR, NewPR]
            DoubleWritable[] pr = {new DoubleWritable(1000.0), new DoubleWritable(1.0 / getTotalNumVertices())};

            vertex.setValue(new ArrayWritable(DoubleWritable.class, pr));

            for (Edge<LongWritable, NullWritable> edge : vertex.getEdges()) {
                sendMessage(edge.getTargetVertexId(), (DoubleWritable) vertex.getValue().get()[1]);
            }

        } else {
            Double oldPR = ((DoubleWritable) vertex.getValue().get()[0]).get();
            Double newPR = ((DoubleWritable) vertex.getValue().get()[1]).get();
            double tolerance = Double.parseDouble(getContext().getConfiguration().get("tolerance"));
            double weight = Double.parseDouble(getContext().getConfiguration().get("weight"));

            if (Math.abs(newPR - oldPR) / oldPR < tolerance)
                vertex.voteToHalt();
            else {

                double sum = 0.0;

                for (DoubleWritable message : messages) {
                    sum += message.get();
                }

                oldPR = newPR;
                newPR = weight * sum + (1-weight) * (1.0 / getTotalNumVertices());

                DoubleWritable[] pr = {new DoubleWritable(oldPR), new DoubleWritable(newPR)};

                vertex.setValue(new ArrayWritable(DoubleWritable.class, pr));

                for (Edge<LongWritable, NullWritable> edge : vertex.getEdges()) {
                    sendMessage(edge.getTargetVertexId(), (DoubleWritable) vertex.getValue().get()[1]);
                }

                if (Math.abs(newPR - oldPR) / oldPR < tolerance)
                    vertex.voteToHalt();

            }

        }

    }

}
