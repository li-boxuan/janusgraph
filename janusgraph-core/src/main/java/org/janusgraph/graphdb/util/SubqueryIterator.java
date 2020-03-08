/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.janusgraph.graphdb.util;

import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.google.common.base.Preconditions;
import org.janusgraph.core.JanusGraphElement;
import org.janusgraph.core.JanusGraphException;
import org.janusgraph.diskstorage.BackendTransaction;
import org.janusgraph.graphdb.database.IndexSerializer;
import org.janusgraph.graphdb.query.Query;
import org.janusgraph.graphdb.query.graph.JointIndexQuery;
import org.janusgraph.graphdb.query.profile.QueryProfiler;

import com.google.common.cache.Cache;

/**
 * @author davidclement90@laposte.net
 */
public class SubqueryIterator implements Iterator<JanusGraphElement>, AutoCloseable {

    private final JointIndexQuery.Subquery subQuery;

    private final Cache<JointIndexQuery.Subquery, List<Object>> indexCache;

    private Iterator<? extends JanusGraphElement> elementIterator;

    private List<Object> currentIds;

    private QueryProfiler profiler;

    private boolean isTimerRunning;

    public SubqueryIterator(List<JointIndexQuery.Subquery> queries, IndexSerializer indexSerializer,
                            BackendTransaction tx, Cache<JointIndexQuery.Subquery, List<Object>> indexCache,
                            int limit, Function<Object, ? extends JanusGraphElement> conversionFunction) {
        Preconditions.checkArgument(!queries.isEmpty());
        Preconditions.checkArgument(limit >= 0, "Invalid limit: %s", limit);
        this.indexCache = indexCache;
        final Stream<?> stream;
        if (queries.size() == 1) {
            // For a single query, we are safe to execute it with the limit constraint
            JointIndexQuery.Subquery subquery = queries.get(0).updateLimit(limit);
            final List<Object> cacheResponse = indexCache.getIfPresent(subquery);
            if (cacheResponse != null) {
                stream = cacheResponse.stream();
            } else {
                profiler = QueryProfiler.startProfile(subquery.getProfiler(), subquery);
                isTimerRunning = true;
                stream = indexSerializer.query(subquery, tx); // TODO: add to currentIds for cache purpose
            }
            elementIterator = stream.limit(limit).map(conversionFunction).map(r -> (JanusGraphElement) r).iterator();
        } else {
            // For multiple queries, we progressively fetch results and take intersections
            final int multiplier = Math.min(16, (int) Math.pow(2, queries.size() - 1));
            int baseSubLimit = Math.min(limit * multiplier, Query.NO_LIMIT);
            // A mapping of result to a number list of queries that contain this result
            Map<Object, List<Integer>> subResultToQueryMap = new HashMap<>();
            double[] scores = new double[queries.size()];
            int[] offsets = new int[queries.size()];
            boolean[] resultsExhausted = new boolean[queries.size()];
            int resultsExhaustedCount = 0;
            List<Object> results = new ArrayList<>();
            do {
                // Retrieve the lowest score
                double lowestScore = Double.MAX_VALUE;
                for (double score : scores) {
                    lowestScore = Math.min(lowestScore, score);
                }

                // Pick up suitable queries to execute
                for (int i = 0; i < queries.size(); i++) {
                    if (resultsExhausted[i]) continue;
                    if (Math.abs(scores[i] - lowestScore) > 1e-2) continue;
                    JointIndexQuery.Subquery subQuery = queries.get(i);
                    // TODO: utilise and save into cache
                    int subLimit = (int) Math.min(Query.NO_LIMIT, Math.max(baseSubLimit,
                        Math.max(Math.pow(offsets[i], 1.5), (offsets[i] + 1) * 2)));
                    // TODO: profile the query
                    // TODO: leverage the scrolling capability of external indexing backends rather than throw away old results
                    indexSerializer.query(subQuery.updateLimit(subLimit), tx).skip(offsets[i]).forEach(result -> {
                        offsets[i]++;
                        List<Integer> queryNumbers = subResultToQueryMap.get(result);
                        if (queryNumbers == null) queryNumbers = new ArrayList<>();
                        queryNumbers.add(i);
                        subResultToQueryMap.put(result, queryNumbers);
                    });
                    if (offsets[i] >= subLimit) {
                        resultsExhausted[i] = true;
                        resultsExhaustedCount++;
                    }
                }

                // Process results and do intersection
                for (Map.Entry<Object, List<Integer>> entry : subResultToQueryMap.entrySet()) {
                    if (entry.getValue().size() == queries.size()) {
                        // this particular result satisfies every index query
                        if (results.size() < limit) results.add(entry.getKey());
                        subResultToQueryMap.remove(entry.getKey());
                    }
                }

                // Calculate score for each query. Lower score means the query is more selective and more likely to
                // be the bottleneck. Unless more factors are taken into consideration, at the moment it does not make
                // sense to compare queries if we only have two.
                // TODO: more factors to be considered: latency, total available size,
                if (resultsExhaustedCount < queries.size() && results.size() < limit && queries.size() > 2) {
                    for (int i = 0; i < scores.length; i++) scores[i] = 0;
                    /*
                     * A query whose results have many intersections with other queries is less likely to be selective.
                     * in the extreme case, an 'everything query' has intersections with all other queries
                     */
                    for (List<Integer> queryNoList : subResultToQueryMap.values()) {
                        for (int idx : queryNoList) {
                            scores[idx] += Math.log(queryNoList.size());
                        }
                    }
                    // TODO: need to take offset into account; we don't want to focus too much on one particular query
                }

            } while (resultsExhaustedCount < queries.size() && results.size() < limit);
            elementIterator = results.stream().map(conversionFunction).map(r -> (JanusGraphElement) r).iterator();
        }
    }

    public SubqueryIterator(JointIndexQuery.Subquery subQuery, IndexSerializer indexSerializer, BackendTransaction tx,
            Cache<JointIndexQuery.Subquery, List<Object>> indexCache, int limit,
            Function<Object, ? extends JanusGraphElement> function, List<Object> otherResults) {
        this.subQuery = subQuery;
        this.indexCache = indexCache;
        final List<Object> cacheResponse = indexCache.getIfPresent(subQuery);
        final Stream<?> stream;
        if (cacheResponse != null) {
            stream = cacheResponse.stream();
        } else {
            try {
                currentIds = new ArrayList<>();
                profiler = QueryProfiler.startProfile(subQuery.getProfiler(), subQuery);
                isTimerRunning = true;
                stream = indexSerializer.query(subQuery, tx).peek(r -> currentIds.add(r));
            } catch (final Exception e) {
                throw new JanusGraphException("Could not call index", e.getCause());
            }
        }
        elementIterator = stream.filter(e -> otherResults == null || otherResults.contains(e)).limit(limit).map(function).map(r -> (JanusGraphElement) r).iterator();
    }

    @Override
    public boolean hasNext() {
        if (!elementIterator.hasNext() && currentIds != null) {
            indexCache.put(subQuery, currentIds);
            profiler.stopTimer();
            isTimerRunning = false;
            profiler.setResultSize(currentIds.size());
        }
        return elementIterator.hasNext();
    }

    @Override
    public JanusGraphElement next() {
        return this.elementIterator.next();
    }

    @Override
    public void close() throws Exception {
        if (isTimerRunning) {
            profiler.stopTimer();
        }
    }

}
