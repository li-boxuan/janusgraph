package org.janusgraph.graphdb.tinkerpop.optimize.strategy;

import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.EdgeOtherVertexStep;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.AbstractTraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalHelper;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.util.ElementHelper;
import org.janusgraph.graphdb.vertices.AbstractVertex;

import java.util.Collections;
import java.util.List;
import java.util.Set;

/**
 * This strategy is to ensure traversals work properly even if it encounters proxy nodes
 */
public class JanusGraphProxyTraversalStrategy extends AbstractTraversalStrategy<TraversalStrategy.ProviderOptimizationStrategy> implements TraversalStrategy.ProviderOptimizationStrategy {

    private static final JanusGraphProxyTraversalStrategy INSTANCE = new JanusGraphProxyTraversalStrategy();

    private JanusGraphProxyTraversalStrategy() {
    }

    @Override
    public void apply(final Traversal.Admin<?, ?> traversal) {
        TraversalHelper.getStepsOfClass(EdgeOtherVertexStep.class, traversal).forEach(originalStep -> {
            final JanusGraphEdgeOtherVertexStep step = new JanusGraphEdgeOtherVertexStep(traversal);
            TraversalHelper.replaceStep(originalStep, step, traversal);
        });
    }

    public static JanusGraphProxyTraversalStrategy instance() {
        return INSTANCE;
    }

    private static final Set<Class<? extends ProviderOptimizationStrategy>> PRIORS = Collections.singleton(JanusGraphLocalQueryOptimizerStrategy.class);


    @Override
    public Set<Class<? extends ProviderOptimizationStrategy>> applyPrior() {
        return PRIORS;
    }


    public static class JanusGraphEdgeOtherVertexStep extends EdgeOtherVertexStep {

        public JanusGraphEdgeOtherVertexStep(final Traversal.Admin<?, ?> traversal) {
            super(traversal);
        }

        @Override
        protected Vertex map(final Traverser.Admin<Edge> traverser) {
            final List<Object> objects = traverser.path().objects();
            for (int i = objects.size() - 2; i >= 0; i--) {
                if (objects.get(i) instanceof Vertex) {
                    final Edge edge = traverser.get();
                    final Vertex outVertex = edge.outVertex();
                    final Vertex inVertex = edge.inVertex();
                    final Vertex v = (Vertex) objects.get(i);
                    AbstractVertex otherV;
                    if (ElementHelper.areEqual(v, edge.outVertex())) {
                        otherV = (AbstractVertex) inVertex;
                    } else if (ElementHelper.areEqual(v, edge.inVertex())) {
                        otherV = (AbstractVertex) outVertex;
                    } else {
                        // at least one endpoint of this edge is a proxy node
                        if (outVertex.label().equals("proxy") && outVertex.vertices(Direction.IN, "is-proxy").next().id().equals(v.id())) {
                            otherV = (AbstractVertex) inVertex;
                        } else {
                            otherV = (AbstractVertex) outVertex;
                        }
                    }
                    if (otherV.label().equals("proxy")) {
                        return otherV.vertices(Direction.IN, "is-proxy").next();
                    } else {
                        return otherV;
                    }
                }
            }
            throw new IllegalStateException("The path history of the traverser does not contain a previous vertex: " + traverser.path());
        }

    }
}
