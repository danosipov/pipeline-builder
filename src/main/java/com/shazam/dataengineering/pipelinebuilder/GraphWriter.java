package com.shazam.dataengineering.pipelinebuilder;

import com.amazonaws.services.datapipeline.model.*;
import org.jgrapht.ext.DOTExporter;
import org.jgrapht.ext.EdgeNameProvider;
import org.jgrapht.ext.IntegerNameProvider;
import org.jgrapht.ext.VertexNameProvider;
import org.jgrapht.graph.ClassBasedEdgeFactory;
import org.jgrapht.graph.DefaultEdge;
import org.jgrapht.graph.DirectedMultigraph;

import java.io.Writer;
import java.util.HashMap;
import java.util.List;
import java.util.Set;

/**
 * Class responsible for writing out DOT representation of the pipeline.
 *
 */
public class GraphWriter {

    @SuppressWarnings("unchecked")
    public void writeDOT(Writer writer, PipelineObject pipeline) {
        DirectedMultigraph graph = getGraph(pipeline);

        DOTExporter dot = new DOTExporter(
                new IntegerNameProvider(),
                new PipelineVertexNameProvider(),
                new PipelineEdgeNameProvider()
        );

        dot.export(writer, graph);
    }

    /**
     * Build DAG of the pipeline for writing to DOT
     * Only generates activity information. Full graphs tend to be very noisy.
     *
     * @return DirectedMultigraph representation of the pipeline
     */
    @SuppressWarnings("unchecked")
    private DirectedMultigraph getGraph(PipelineObject pipeline) {
        DirectedMultigraph graph = new DirectedMultigraph<com.amazonaws.services.datapipeline.model.PipelineObject, RelationshipEdge>(
                new ClassBasedEdgeFactory<com.amazonaws.services.datapipeline.model.PipelineObject, RelationshipEdge>(RelationshipEdge.class));

        // Maintain map for easier edge creation
        HashMap<String, com.amazonaws.services.datapipeline.model.PipelineObject> idToPipeline =
                new HashMap<String, com.amazonaws.services.datapipeline.model.PipelineObject>();

        List<com.amazonaws.services.datapipeline.model.PipelineObject> awsObjects = pipeline.getAWSObjects();
        for (com.amazonaws.services.datapipeline.model.PipelineObject awsObject : awsObjects) {
            // Ignore everything except activities
            if (!awsObject.getId().equals("Default")) {
                for (Field field : awsObject.getFields()) {
                    // Only add activities to the DOT
                    if (field.getKey().equals("type") && field.getStringValue().contains("Activity")) {
                        idToPipeline.put(awsObject.getId(), awsObject);
                        graph.addVertex(awsObject);
                    }
                }
            }
        }

        for (com.amazonaws.services.datapipeline.model.PipelineObject awsObject :
                (Set<com.amazonaws.services.datapipeline.model.PipelineObject>) graph.vertexSet()) {
            List<Field> fields = awsObject.getFields();
            for (Field field : fields) {
                if (field.getRefValue() != null && idToPipeline.containsKey(field.getRefValue())) {
                    graph.addEdge(awsObject, idToPipeline.get(field.getRefValue()),
                            new RelationshipEdge<com.amazonaws.services.datapipeline.model.PipelineObject>(
                                    awsObject,
                                    idToPipeline.get(field.getRefValue()),
                                    field.getKey()
                            ));
                }
            }
        }

        return graph;
    }

    private class PipelineVertexNameProvider
            implements VertexNameProvider<com.amazonaws.services.datapipeline.model.PipelineObject> {
        public String getVertexName(com.amazonaws.services.datapipeline.model.PipelineObject object) {
            if (object == null) {
                return "none";
            } else if (object.getName() != null && !object.getName().isEmpty()) {
                String label = object.getName() + "\nID: " + object.getId();
                for (Field field : object.getFields()) {
                    if (field.getKey().equals("type")) {
                        label += "\nType: " + field.getStringValue();
                    }
                }

                return clean(label);
            } else {
                return clean(object.toString());
            }
        }

        private String clean(String input) {
            return input.replaceAll("\"", "\'");
        }
    }

    private class PipelineEdgeNameProvider implements EdgeNameProvider<RelationshipEdge> {
        public String getEdgeName(RelationshipEdge edge) {
            if (edge == null) {
                return "none";
            } else {
                return edge.toString().replaceAll("\"", "\'");
            }
        }
    }

    public static class RelationshipEdge<V> extends DefaultEdge {
        private V v1;
        private V v2;
        private String label;

        public RelationshipEdge(V v1, V v2, String label) {
            this.v1 = v1;
            this.v2 = v2;
            this.label = label;
        }

        public V getV1() {
            return v1;
        }

        public V getV2() {
            return v2;
        }

        public String toString() {
            return label;
        }
    }
}
