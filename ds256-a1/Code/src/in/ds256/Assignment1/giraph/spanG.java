package in.ds256.Assignment1.Giraph;

import org.apache.giraph.graph.BasicComputation;
import org.apache.giraph.edge.Edge;
import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.log4j.Logger;

import java.io.IOException;

public class spanG extends BasicComputation<LongWritable, ArrayWritable, NullWritable, ArrayWritable> {

    @Override
    public void compute(Vertex<LongWritable, ArrayWritable, NullWritable> vertex, Iterable<ArrayWritable> messages) throws IOException {

        if (getSuperstep() == 0) {

            // [Distance, Parent]
            LongWritable[] val = {new LongWritable(Long.MAX_VALUE), new LongWritable(-1)};

            vertex.setValue(new ArrayWritable(LongWritable.class, val));

            if (val[0].get() < Long.MAX_VALUE) {
                for (Edge<LongWritable, NullWritable> edge : vertex.getEdges()) {
                    sendMessage(edge.getTargetVertexId(), new ArrayWritable(LongWritable.class, val));
                }
            }

        } else {

            Long dis = ((LongWritable) vertex.getValue().get()[0]).get();
            Long parent = ((LongWritable) vertex.getValue().get()[1]).get();
            Boolean isChanged = false;

            for (ArrayWritable message : messages) {
                if (((LongWritable) message.get()[0]).get() + 1 < dis) {
                    dis = ((LongWritable) message.get()[0]).get() + 1;
                    parent = ((LongWritable) message.get()[1]).get();
                    isChanged = true;
                }
            }

            if (isChanged) {

                LongWritable[] val = {new LongWritable(dis), new LongWritable(parent)};
                vertex.setValue(new ArrayWritable(LongWritable.class, val));
                val[1] = vertex.getId();

                for (Edge<LongWritable, NullWritable> edge : vertex.getEdges()) {
                    sendMessage(edge.getTargetVertexId(), new ArrayWritable(LongWritable.class, val));
                }

            }

        }

        vertex.voteToHalt();

    }

}
