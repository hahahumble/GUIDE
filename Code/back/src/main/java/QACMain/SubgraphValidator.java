package QACMain;

import java.util.*;
import model.Edge;
import model.Graph;
import model.Vertex;

public class SubgraphValidator {

    public static boolean isSubgraph(Graph queryGraph, Graph targetGraph) {
        if (queryGraph == null || targetGraph == null) {
            return false;
        }

        if (queryGraph.size() > targetGraph.size()) {
            return false;
        }

        return vf2SubgraphMatch(queryGraph, targetGraph);
    }

    private static boolean vf2SubgraphMatch(
        Graph queryGraph,
        Graph targetGraph
    ) {
        if (queryGraph.size() == 0) {
            return true;
        }

        Map<Integer, Integer> queryToTarget = new HashMap<>();
        Map<Integer, Integer> targetToQuery = new HashMap<>();

        for (int startNode = 0; startNode < targetGraph.size(); startNode++) {
            queryToTarget.clear();
            targetToQuery.clear();

            if (
                recursiveMatch(
                    queryGraph,
                    targetGraph,
                    0,
                    startNode,
                    queryToTarget,
                    targetToQuery
                )
            ) {
                return true;
            }
        }

        return false;
    }

    private static boolean recursiveMatch(
        Graph queryGraph,
        Graph targetGraph,
        int queryNode,
        int targetNode,
        Map<Integer, Integer> queryToTarget,
        Map<Integer, Integer> targetToQuery
    ) {
        if (
            queryGraph.get(queryNode).label != targetGraph.get(targetNode).label
        ) {
            return false;
        }

        if (queryToTarget.containsKey(queryNode)) {
            return queryToTarget.get(queryNode) == targetNode;
        }
        if (targetToQuery.containsKey(targetNode)) {
            return targetToQuery.get(targetNode) == queryNode;
        }

        queryToTarget.put(queryNode, targetNode);
        targetToQuery.put(targetNode, queryNode);

        boolean edgesMatch = checkEdgeCompatibility(
            queryGraph,
            targetGraph,
            queryNode,
            targetNode,
            queryToTarget,
            targetToQuery
        );

        if (!edgesMatch) {
            queryToTarget.remove(queryNode);
            targetToQuery.remove(targetNode);
            return false;
        }

        if (queryToTarget.size() == queryGraph.size()) {
            return true;
        }

        int nextQueryNode = -1;
        for (int i = 0; i < queryGraph.size(); i++) {
            if (!queryToTarget.containsKey(i)) {
                nextQueryNode = i;
                break;
            }
        }

        if (nextQueryNode == -1) {
            return true;
        }

        for (int i = 0; i < targetGraph.size(); i++) {
            if (!targetToQuery.containsKey(i)) {
                if (
                    recursiveMatch(
                        queryGraph,
                        targetGraph,
                        nextQueryNode,
                        i,
                        queryToTarget,
                        targetToQuery
                    )
                ) {
                    return true;
                }
            }
        }

        queryToTarget.remove(queryNode);
        targetToQuery.remove(targetNode);
        return false;
    }

    private static boolean checkEdgeCompatibility(
        Graph queryGraph,
        Graph targetGraph,
        int queryNode,
        int targetNode,
        Map<Integer, Integer> queryToTarget,
        Map<Integer, Integer> targetToQuery
    ) {
        Vertex queryVertex = queryGraph.get(queryNode);
        Vertex targetVertex = targetGraph.get(targetNode);

        for (Edge queryEdge : queryVertex.edge) {
            int queryNeighbor = queryEdge.to;

            if (queryToTarget.containsKey(queryNeighbor)) {
                int targetNeighbor = queryToTarget.get(queryNeighbor);

                boolean edgeFound = false;
                for (Edge targetEdge : targetVertex.edge) {
                    if (
                        targetEdge.to == targetNeighbor &&
                        targetEdge.eLabel == queryEdge.eLabel
                    ) {
                        edgeFound = true;
                        break;
                    }
                }

                if (!edgeFound) {
                    return false;
                }
            }
        }

        for (int i = 0; i < queryGraph.size(); i++) {
            if (queryToTarget.containsKey(i)) {
                int mappedTargetNode = queryToTarget.get(i);

                boolean queryEdgeExists = false;
                int queryEdgeLabel = -1;
                for (Edge queryEdge : queryGraph.get(i).edge) {
                    if (queryEdge.to == queryNode) {
                        queryEdgeExists = true;
                        queryEdgeLabel = queryEdge.eLabel;
                        break;
                    }
                }

                if (queryEdgeExists) {
                    boolean targetEdgeExists = false;
                    for (Edge targetEdge : targetGraph.get(
                        mappedTargetNode
                    ).edge) {
                        if (
                            targetEdge.to == targetNode &&
                            targetEdge.eLabel == queryEdgeLabel
                        ) {
                            targetEdgeExists = true;
                            break;
                        }
                    }

                    if (!targetEdgeExists) {
                        return false;
                    }
                }
            }
        }

        return true;
    }
}
