package QACMain;

import java.io.*;
import java.util.*;
import model.*;

public class GraphExtensionManager {

    private final ArrayList<Graph> database;
    private final Arguments arguments;

    public GraphExtensionManager(
        ArrayList<Graph> database,
        Arguments arguments
    ) {
        this.database = database;
        this.arguments = arguments;
    }

    public List<DFSCode> expandGraphToTargetSize(
        DFSCode currentDfsCode,
        int targetSize
    ) {
        List<DFSCode> allSupergraphs = new ArrayList<>();

        Graph queryGraph = dfsCodeToGraph(currentDfsCode);

        if (queryGraph == null) {
            System.out.println(
                "Query graph conversion failed, cannot perform extension"
            );
            return allSupergraphs;
        }

        System.out.println(
            "Starting exhaustive VF2 extension: " +
            queryGraph.size() +
            " nodes -> " +
            targetSize +
            " nodes"
        );
        System.out.println("Database total graphs: " + database.size());

        int processedGraphs = 0;
        int totalMatches = 0;

        for (Graph dbGraph : database) {
            processedGraphs++;

            if (
                dbGraph.size() >= queryGraph.size() &&
                dbGraph.size() <= targetSize
            ) {
                List<Map<Integer, Integer>> allMatches = findAllVF2Matches(
                    queryGraph,
                    dbGraph
                );

                if (!allMatches.isEmpty()) {
                    System.out.println(
                        "Graph " +
                        (processedGraphs) +
                        " (size:" +
                        dbGraph.size() +
                        ") found " +
                        allMatches.size() +
                        " VF2 matches"
                    );
                    totalMatches += allMatches.size();

                    for (Map<Integer, Integer> match : allMatches) {
                        Graph supergraph = generateSupergraphFromMatch(
                            queryGraph,
                            dbGraph,
                            match
                        );

                        if (
                            supergraph != null &&
                            supergraph.size() >= queryGraph.size()
                        ) {
                            DFSCode supergraphDfsCode = graphToDfsCode(
                                supergraph
                            );
                            if (supergraphDfsCode != null) {
                                allSupergraphs.add(supergraphDfsCode);

                                System.out.println(
                                    "Generated supergraph: " +
                                    queryGraph.size() +
                                    "->" +
                                    supergraph.size() +
                                    " nodes, " +
                                    supergraph.getEdgeSize() +
                                    " edges"
                                );
                            }
                        }
                    }
                }
            }

            if (processedGraphs % 1000 == 0) {
                System.out.println(
                    "Processed " +
                    processedGraphs +
                    "/" +
                    database.size() +
                    " database graphs, generated " +
                    allSupergraphs.size() +
                    " supergraphs"
                );
            }
        }

        System.out.println("Exhaustive enumeration completed:");
        System.out.println(
            "  Processed database graphs: " +
            processedGraphs +
            "/" +
            database.size()
        );
        System.out.println("  Total VF2 matches: " + totalMatches);
        System.out.println("  Generated supergraphs: " + allSupergraphs.size());

        return allSupergraphs;
    }

    private List<Map<Integer, Integer>> findAllVF2Matches(
        Graph queryGraph,
        Graph targetGraph
    ) {
        List<Map<Integer, Integer>> allMatches = new ArrayList<>();

        Map<Integer, Integer> currentMatch = new HashMap<>();
        Set<Integer> usedTargetVertices = new HashSet<>();

        findAllMatchesRecursive(
            queryGraph,
            targetGraph,
            0,
            currentMatch,
            usedTargetVertices,
            allMatches
        );

        return allMatches;
    }

    private void findAllMatchesRecursive(
        Graph queryGraph,
        Graph targetGraph,
        int queryVertex,
        Map<Integer, Integer> currentMatch,
        Set<Integer> usedTargetVertices,
        List<Map<Integer, Integer>> allMatches
    ) {
        if (queryVertex >= queryGraph.size()) {
            allMatches.add(new HashMap<>(currentMatch));
            return;
        }

        for (
            int targetVertex = 0;
            targetVertex < targetGraph.size();
            targetVertex++
        ) {
            if (usedTargetVertices.contains(targetVertex)) {
                continue;
            }

            if (
                isVertexCompatible(
                    queryGraph,
                    targetGraph,
                    queryVertex,
                    targetVertex
                )
            ) {
                if (
                    isEdgeConstraintsSatisfied(
                        queryGraph,
                        targetGraph,
                        queryVertex,
                        targetVertex,
                        currentMatch
                    )
                ) {
                    currentMatch.put(queryVertex, targetVertex);
                    usedTargetVertices.add(targetVertex);

                    findAllMatchesRecursive(
                        queryGraph,
                        targetGraph,
                        queryVertex + 1,
                        currentMatch,
                        usedTargetVertices,
                        allMatches
                    );

                    currentMatch.remove(queryVertex);
                    usedTargetVertices.remove(targetVertex);
                }
            }
        }
    }

    private Graph generateSupergraphFromMatch(
        Graph queryGraph,
        Graph dbGraph,
        Map<Integer, Integer> match
    ) {
        try {
            Graph supergraph = new Graph();
            Map<Integer, Integer> dbToSuperMapping = new HashMap<>();

            Set<Integer> mappedDbVertices = new HashSet<>(match.values());

            Set<Integer> superVertices = new HashSet<>(mappedDbVertices);

            for (Integer dbVertex : mappedDbVertices) {
                for (Edge edge : dbGraph.get(dbVertex).edge) {
                    if (
                        !superVertices.contains(edge.to) &&
                        superVertices.size() < arguments.maxNodeNum
                    ) {
                        superVertices.add(edge.to);

                        if (superVertices.size() >= queryGraph.size() + 6) {
                            break;
                        }
                    }
                }
            }

            List<Integer> sortedSuperVertices = new ArrayList<>(superVertices);
            sortedSuperVertices.sort(Integer::compareTo);

            for (int i = 0; i < sortedSuperVertices.size(); i++) {
                Integer dbVertexId = sortedSuperVertices.get(i);
                dbToSuperMapping.put(dbVertexId, i);

                Vertex superVertex = new Vertex();
                superVertex.label = dbGraph.get(dbVertexId).label;
                supergraph.add(superVertex);
            }

            Set<String> addedEdges = new HashSet<>();
            for (Integer dbVertex : sortedSuperVertices) {
                for (Edge dbEdge : dbGraph.get(dbVertex).edge) {
                    if (superVertices.contains(dbEdge.to)) {
                        int superFrom = dbToSuperMapping.get(dbEdge.from);
                        int superTo = dbToSuperMapping.get(dbEdge.to);

                        String edgeKey =
                            Math.min(superFrom, superTo) +
                            "-" +
                            Math.max(superFrom, superTo) +
                            "-" +
                            dbEdge.eLabel;
                        if (!addedEdges.contains(edgeKey)) {
                            addedEdges.add(edgeKey);

                            supergraph
                                .get(superFrom)
                                .push(superFrom, superTo, dbEdge.eLabel);
                            if (superFrom != superTo) {
                                supergraph
                                    .get(superTo)
                                    .push(superTo, superFrom, dbEdge.eLabel);
                            }
                        }
                    }
                }
            }

            return supergraph;
        } catch (Exception e) {
            System.err.println(
                "Error generating supergraph: " + e.getMessage()
            );
            return null;
        }
    }

    private boolean isVertexCompatible(
        Graph queryGraph,
        Graph targetGraph,
        int queryVertex,
        int targetVertex
    ) {
        return (
            queryGraph.get(queryVertex).label ==
            targetGraph.get(targetVertex).label
        );
    }

    private boolean isEdgeConstraintsSatisfied(
        Graph queryGraph,
        Graph targetGraph,
        int queryVertex,
        int targetVertex,
        Map<Integer, Integer> currentMatch
    ) {
        for (Map.Entry<Integer, Integer> entry : currentMatch.entrySet()) {
            int mappedQueryVertex = entry.getKey();
            int mappedTargetVertex = entry.getValue();

            Edge queryEdge = findEdge(
                queryGraph,
                queryVertex,
                mappedQueryVertex
            );
            if (queryEdge != null) {
                Edge targetEdge = findEdge(
                    targetGraph,
                    targetVertex,
                    mappedTargetVertex
                );
                if (
                    targetEdge == null || targetEdge.eLabel != queryEdge.eLabel
                ) {
                    return false;
                }
            }
        }

        return true;
    }

    private Edge findEdge(Graph graph, int from, int to) {
        for (Edge edge : graph.get(from).edge) {
            if (edge.to == to) {
                return edge;
            }
        }
        return null;
    }

    private Graph dfsCodeToGraph(DFSCode dfsCode) {
        Graph graph = new Graph();
        Map<Integer, Integer> vertexIdMap = new HashMap<>();
        int nextVertexId = 0;

        try {
            for (DFS dfs : dfsCode) {
                if (!vertexIdMap.containsKey(dfs.from)) {
                    vertexIdMap.put(dfs.from, nextVertexId);
                    Vertex vertex = new Vertex();
                    vertex.label = dfs.fromLabel;
                    graph.add(vertex);
                    nextVertexId++;
                }
                if (!vertexIdMap.containsKey(dfs.to)) {
                    vertexIdMap.put(dfs.to, nextVertexId);
                    Vertex vertex = new Vertex();
                    vertex.label = dfs.toLabel;
                    graph.add(vertex);
                    nextVertexId++;
                }
            }

            for (DFS dfs : dfsCode) {
                int fromId = vertexIdMap.get(dfs.from);
                int toId = vertexIdMap.get(dfs.to);

                graph.get(fromId).push(fromId, toId, dfs.eLabel);
                graph.get(toId).push(toId, fromId, dfs.eLabel);
            }
        } catch (Exception e) {
            System.err.println("❌ DFS代码转换图失败: " + e.getMessage());
            return null;
        }

        return graph;
    }

    private DFSCode graphToDfsCode(Graph graph) {
        DFSCode dfsCode = new DFSCode();
        Set<String> addedEdges = new HashSet<>();

        try {
            for (int i = 0; i < graph.size(); i++) {
                for (Edge edge : graph.get(i).edge) {
                    String edgeKey =
                        Math.min(edge.from, edge.to) +
                        "-" +
                        Math.max(edge.from, edge.to) +
                        "-" +
                        edge.eLabel;
                    if (!addedEdges.contains(edgeKey)) {
                        addedEdges.add(edgeKey);

                        int fromLabel = graph.get(edge.from).label;
                        int toLabel = graph.get(edge.to).label;

                        DFS dfs = new DFS();
                        dfs.from = edge.from;
                        dfs.to = edge.to;
                        dfs.fromLabel = fromLabel;
                        dfs.eLabel = edge.eLabel;
                        dfs.toLabel = toLabel;
                        dfsCode.add(dfs);
                    }
                }
            }
        } catch (Exception e) {
            System.err.println("❌ 图转换DFS代码失败: " + e.getMessage());
            return null;
        }

        return dfsCode;
    }
}
