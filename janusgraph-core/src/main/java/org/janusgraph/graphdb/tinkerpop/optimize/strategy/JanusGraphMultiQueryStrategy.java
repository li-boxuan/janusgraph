// Copyright 2021 JanusGraph Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package org.janusgraph.graphdb.tinkerpop.optimize.strategy;

import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal.Admin;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.DropStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.NoOpBarrierStep;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.AbstractTraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalHelper;
import org.janusgraph.graphdb.database.StandardJanusGraph;
import org.janusgraph.graphdb.tinkerpop.optimize.JanusGraphTraversalUtil;
import org.janusgraph.graphdb.tinkerpop.optimize.step.JanusGraphMultiQueryStep;
import org.janusgraph.graphdb.tinkerpop.optimize.step.MultiQueriable;

import java.util.*;

/**
 * @author Marko A. Rodriguez (https://markorodriguez.com)
 * @author Matthias Broecheler (http://matthiasb.com)
 */
public class JanusGraphMultiQueryStrategy extends AbstractTraversalStrategy<TraversalStrategy.ProviderOptimizationStrategy> implements TraversalStrategy.ProviderOptimizationStrategy {

    private static final Set<Class<? extends ProviderOptimizationStrategy>> PRIORS = Collections.singleton(JanusGraphLocalQueryOptimizerStrategy.class);
    private static final JanusGraphMultiQueryStrategy INSTANCE = new JanusGraphMultiQueryStrategy();

    private JanusGraphMultiQueryStrategy() {
    }

    @Override
    public void apply(final Admin<?, ?> traversal) {
        if (!traversal.getGraph().isPresent()
            || TraversalHelper.onGraphComputer(traversal)
            // The LazyBarrierStrategy is not allowed to run on traversals which use drop(). As a precaution,
            // this strategy should not run on those traversals either, because it can also insert barrier().
            || !TraversalHelper.getStepsOfAssignableClassRecursively(DropStep.class, traversal).isEmpty()) {
            return;
        }

        final StandardJanusGraph janusGraph = JanusGraphTraversalUtil.getJanusGraph(traversal);
        if (janusGraph == null || !janusGraph.getConfiguration().useMultiQuery()) {
            return;
        }

        insertMultiQuerySteps(traversal, janusGraph.getConfiguration().limitBatchSize());
        configureMultiQueriables(traversal);
    }

    /**
     * Insert JanusGraphMultiQuerySteps everywhere in the current traversal where MultiQueriable steps could benefit
     * @param traversal The local traversal layer.
     */
    private void insertMultiQuerySteps(final Admin<?,?> traversal, boolean limitBatchSize) {
        JanusGraphTraversalUtil.getSteps(JanusGraphTraversalUtil::isMultiQueryCompatibleStep, traversal).forEach(step -> {
            Optional<Step> multiQueryPosition = JanusGraphTraversalUtil.getLocalMultiQueryPositionForStep(step);
            if (multiQueryPosition.isPresent() && JanusGraphTraversalUtil.isLegalMultiQueryPosition(multiQueryPosition.get())) {
                Step pos = multiQueryPosition.get();
                if (limitBatchSize && !(multiQueryPosition.get() instanceof NoOpBarrierStep)) {
                    NoOpBarrierStep barrier = new NoOpBarrierStep(traversal);
                    TraversalHelper.insertBeforeStep(barrier, pos, traversal);
                    pos = barrier;
                }
                JanusGraphMultiQueryStep multiQueryStep = new JanusGraphMultiQueryStep(traversal, limitBatchSize);
                TraversalHelper.insertBeforeStep(multiQueryStep, pos, traversal);
            }
        });
    }

    /**
     * Looks for MultiQueriables in within the traversal and registers them as clients of their respective
     * JanusGraphMultiQuerySteps
     * @param traversal The local traversal layer.
     */
    private void configureMultiQueriables(final Admin<?,?> traversal) {
        TraversalHelper.getStepsOfAssignableClass(MultiQueriable.class, traversal).forEach(multiQueriable -> {
            final List<Step> mqPositions = JanusGraphTraversalUtil.getAllMultiQueryPositionsForMultiQueriable(multiQueriable);
            final List<JanusGraphMultiQueryStep> multiQuerySteps = new ArrayList<>(mqPositions.size());
            boolean multiQueryAllowed = true;
            for (Step mqPos : mqPositions) {
                // If one position is not legal, this means that the entire step can not use the multiQuery feature.
                multiQueryAllowed &= JanusGraphTraversalUtil.isLegalMultiQueryPosition(mqPos);
                final Optional<JanusGraphMultiQueryStep> multiQueryStep =
                    JanusGraphTraversalUtil.getPreviousStepOfClass(JanusGraphMultiQueryStep.class, mqPos);
                multiQueryStep.ifPresent(multiQuerySteps::add);
            }
            if (multiQueryAllowed) {
                multiQueriable.setUseMultiQuery(true);
                multiQuerySteps.forEach(mqs -> mqs.attachClient(multiQueriable));
            }
        });
    }

    @Override
    public Set<Class<? extends ProviderOptimizationStrategy>> applyPrior() {
        return PRIORS;
    }

    public static JanusGraphMultiQueryStrategy instance() {
        return INSTANCE;
    }
}
