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

import com.google.common.base.Preconditions;
import com.google.common.cache.Cache;
import org.janusgraph.core.JanusGraphElement;
import org.janusgraph.core.JanusGraphException;
import org.janusgraph.diskstorage.BackendTransaction;
import org.janusgraph.graphdb.database.IndexSerializer;
import org.janusgraph.graphdb.query.Query;
import org.janusgraph.graphdb.query.graph.JointIndexQuery;
import org.janusgraph.graphdb.query.profile.QueryProfiler;

import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * @author davidclement90@laposte.net
 */
public class SubqueryIterator implements Iterator<JanusGraphElement>, AutoCloseable {

    private final Cache<JointIndexQuery.Subquery, List<Object>> indexCache;

    private Iterator<? extends JanusGraphElement> elementIterator;

    private List<Object> currentIds;

    private QueryProfiler streamedQueryProfiler;

    private JointIndexQuery.Subquery streamedQuery;

    private boolean isTimerRunning;

    public SubqueryIterator(List<JointIndexQuery.Subquery> queries, IndexSerializer indexSerializer,
                            BackendTransaction tx, Cache<JointIndexQuery.Subquery, List<Object>> indexCache,
                            int limit, Function<Object, ? extends JanusGraphElement> conversionFunction) {
        Preconditions.checkArgument(!queries.isEmpty());
        Preconditions.checkArgument(limit >= 0, "Invalid limit: %s", limit);
        this.indexCache = indexCache;
        final Stream<?> stream;
        if (limit == Query.NO_LIMIT || queries.size() == 1) {
            // If there is no limit, we lazily stream the first query and eagerly execute the rest of queries all at once
            // Otherwise if there is only one query, we just lazily stream it
            JointIndexQuery.Subquery firstQuery = queries.get(0).updateLimit(limit);
            final List<Object> cacheResponse = indexCache.getIfPresent(firstQuery);
            if (cacheResponse != null) {
                stream = cacheResponse.stream();
            } else {
                currentIds = new ArrayList<>();
                streamedQuery = firstQuery;
                streamedQueryProfiler = QueryProfiler.startProfile(firstQuery.getProfiler(), firstQuery);
                isTimerRunning = true;
                stream = indexSerializer.query(firstQuery, tx).peek(r -> currentIds.add(r));
            }

            // retrieve results from the rest queries and do intersection
            Set<Object> otherResults = null;
            for (int i = 1; i < queries.size(); i++) {
                JointIndexQuery.Subquery subQuery = queries.get(i);
                try {
                    Set<Object> subResults = new HashSet<>(indexCache.get(subQuery, () -> {
                        QueryProfiler profiler = subQuery.getProfiler();
                        QueryProfiler.startProfile(profiler, subQuery);
                        List<Object> queryResults = indexSerializer.query(subQuery, tx).collect(Collectors.toList());
                        profiler.stopTimer();
                        profiler.setResultSize(queryResults.size());
                        return queryResults;
                    }));
                    if (i == 1) {
                        otherResults = subResults;
                    } else {
                        assert otherResults != null;
                        otherResults.retainAll(subResults);
                    }
                } catch (Exception e) {
                    throw new JanusGraphException("Could not execute index query", e.getCause());
                }
            }

            Set<Object> others = otherResults;
            // combine the results of lazily streamed first query and the results of rest queries
            elementIterator = stream.filter(e -> others == null || others.contains(e))
                .map(conversionFunction).map(r -> (JanusGraphElement) r).iterator();
        } else {
            // For multiple queries, we progressively fetch results and take intersections
            final int multiplier = Math.min(16, (int) Math.pow(2, queries.size() - 1));
            int baseSubLimit = Math.min(limit * multiplier, Query.NO_LIMIT);
            // A mapping of result to a number list of queries that contain this result
            LinkedHashMap<Object, List<Integer>> subResultToQueryMap = new LinkedHashMap<>();
            double[] scores = new double[queries.size()];
            int[] offsets = new int[queries.size()];
            boolean[] resultsExhausted = new boolean[queries.size()];
            int resultsExhaustedCount = 0;
            List<Object> results = new ArrayList<>();
            do {
                double scoreSum = 0;
                for (double score : scores) scoreSum += score;
                // Pick up suitable queries to execute
                for (int i = 0; i < queries.size(); i++) {
                    final int idx = i;
                    if (resultsExhausted[i]) continue;
                    if (scores[i] > scoreSum / queries.size()) continue;
                    // TODO: subLimit should be based on offset (offset small means we should increase subLimit more)
                    int subLimit = (int) Math.min(Query.NO_LIMIT, Math.max(baseSubLimit,
                        Math.max(Math.pow(offsets[i], 1.5), (offsets[i] + 1) * 2)));
                    JointIndexQuery.Subquery subQuery = queries.get(i).updateLimit(subLimit);
                    final List<Object> subQueryCache = indexCache.getIfPresent(subQuery);
                    if (subQueryCache != null) {
                        assert subQueryCache.size() >= offsets[idx];
                        for (int j = offsets[idx]; j < subQueryCache.size(); j++) {
                            subResultToQueryMap.computeIfAbsent(subQueryCache.get(j), k -> new ArrayList<>()).add(idx);
                        }
                        offsets[idx] = subQueryCache.size();
                    } else {
                        QueryProfiler profiler = subQuery.getProfiler();
                        QueryProfiler.startProfile(profiler, subQuery);
                        // TODO: leverage the scrolling capability of external indexing backends rather than throw away old results
                        indexSerializer.query(subQuery, tx).skip(offsets[idx]).forEachOrdered(result -> {
                            offsets[idx]++;
                            subResultToQueryMap.computeIfAbsent(result, k -> new ArrayList<>()).add(idx);
                        });
                        profiler.stopTimer();
                        profiler.setResultSize(offsets[idx]);
                        // TODO: should we put it into cache?
                    }
                    if (offsets[i] < subLimit) {
                        resultsExhausted[i] = true;
                        resultsExhaustedCount++;
                    }
                }

                // Process results and do intersection
                for (Iterator<Map.Entry<Object, List<Integer>>> it = subResultToQueryMap.entrySet().iterator(); it.hasNext(); ) {
                    Map.Entry<Object, List<Integer>> entry = it.next();
                    if (entry.getValue().size() == queries.size()) {
                        // this particular result satisfies every index query
                        if (results.size() < 10) System.out.println("results[" + results.size() + "] = " + entry.getKey());
                        results.add(entry.getKey());
                        it.remove();
                    }
                }

                // Calculate score for each query. Lower score means the query is more selective and more likely to
                // be the bottleneck. Unless more factors are taken into consideration, at the moment it does not make
                // sense to compare queries if we only have two.
                if (resultsExhaustedCount < queries.size() && results.size() < limit && queries.size() > 2) {
                    for (int i = 0; i < scores.length; i++) scores[i] = 0;
                    // A query whose results have many intersections with other queries is less likely to be selective.
                    for (List<Integer> queryNoList : subResultToQueryMap.values()) {
                        for (int idx : queryNoList) {
                            scores[idx] += Math.log(queryNoList.size());
                        }
                    }
                }

            } while (resultsExhaustedCount < queries.size() && results.size() < limit);
            List<JanusGraphElement> jgResults = results.stream().map(conversionFunction).map(r -> (JanusGraphElement) r).collect(Collectors.toList());
            // we must ensure results are in a certain order, otherwise calling
            Collections.sort(jgResults, (a, b) -> a.longId() > b.longId() ? 1 : -1);
            elementIterator = jgResults.stream().limit(limit).iterator();
        }
    }

//    public SubqueryIterator(JointIndexQuery.Subquery subQuery, IndexSerializer indexSerializer, BackendTransaction tx,
//            Cache<JointIndexQuery.Subquery, List<Object>> indexCache, int limit,
//            Function<Object, ? extends JanusGraphElement> function, List<Object> otherResults) {
//        this.subQuery = subQuery;
//        this.indexCache = indexCache;
//        final List<Object> cacheResponse = indexCache.getIfPresent(subQuery);
//        final Stream<?> stream;
//        if (cacheResponse != null) {
//            stream = cacheResponse.stream();
//        } else {
//            try {
//                currentIds = new ArrayList<>();
//                profiler = QueryProfiler.startProfile(subQuery.getProfiler(), subQuery);
//                isTimerRunning = true;
//                stream = indexSerializer.query(subQuery, tx).peek(r -> currentIds.add(r));
//            } catch (final Exception e) {
//                throw new JanusGraphException("Could not call index", e.getCause());
//            }
//        }
//        elementIterator = stream.filter(e -> otherResults == null || otherResults.contains(e)).limit(limit).map(function).map(r -> (JanusGraphElement) r).iterator();
//    }

    @Override
    public boolean hasNext() {
        if (!elementIterator.hasNext() && currentIds != null) {
            indexCache.put(streamedQuery, currentIds);
            streamedQueryProfiler.stopTimer();
            isTimerRunning = false;
            streamedQueryProfiler.setResultSize(currentIds.size());
        }
        return elementIterator.hasNext();
    }

    @Override
    public JanusGraphElement next() {
        return elementIterator.next();
    }

    @Override
    public void close() throws Exception {
        if (isTimerRunning) {
            streamedQueryProfiler.stopTimer();
        }
    }

}
