package QACMain;

import java.util.*;
import model.DFSCode;
import model.Edge;
import model.Graph;
import model.Vertex;

public class IsomorphismRemover {

    private static class GraphCanonicalForm {

        final int nodeCount;
        final int edgeCount;
        final String degreeSequence;
        final String labelSequence;
        final String adjacencySignature;
        final String canonicalEdgeList;

        GraphCanonicalForm(Graph graph) {
            this.nodeCount = graph.size();
            this.edgeCount = graph.getEdgeSize();
            this.degreeSequence = computeDegreeSequence(graph);
            this.labelSequence = computeLabelSequence(graph);
            this.adjacencySignature = computeAdjacencySignature(graph);
            this.canonicalEdgeList = computeCanonicalEdgeList(graph);
        }

        private String computeDegreeSequence(Graph graph) {
            Map<Integer, Integer> degreeCount = new HashMap<>();

            int[] degrees = new int[graph.size()];
            for (int i = 0; i < graph.size(); i++) {
                degrees[i] = graph.get(i).edge.size();
                degreeCount.put(
                    degrees[i],
                    degreeCount.getOrDefault(degrees[i], 0) + 1
                );
            }

            List<Integer> sortedDegrees = new ArrayList<>();
            for (int degree : degrees) {
                sortedDegrees.add(degree);
            }
            Collections.sort(sortedDegrees);

            return sortedDegrees.toString();
        }

        private String computeLabelSequence(Graph graph) {
            List<Integer> nodeLabels = new ArrayList<>();
            Map<Integer, Integer> edgeLabels = new HashMap<>();

            for (int i = 0; i < graph.size(); i++) {
                nodeLabels.add(graph.get(i).label);
                for (Edge edge : graph.get(i).edge) {
                    edgeLabels.put(
                        edge.eLabel,
                        edgeLabels.getOrDefault(edge.eLabel, 0) + 1
                    );
                }
            }

            Collections.sort(nodeLabels);
            List<Integer> sortedEdgeLabels = new ArrayList<>(
                edgeLabels.keySet()
            );
            Collections.sort(sortedEdgeLabels);

            return (
                "N:" +
                nodeLabels.toString() +
                ",E:" +
                sortedEdgeLabels.toString()
            );
        }

        private String computeAdjacencySignature(Graph graph) {
            StringBuilder signature = new StringBuilder();

            Map<Integer, List<Integer>> labelGroups = new HashMap<>();
            for (int i = 0; i < graph.size(); i++) {
                int label = graph.get(i).label;
                labelGroups
                    .computeIfAbsent(label, k -> new ArrayList<>())
                    .add(i);
            }

            List<Integer> sortedLabels = new ArrayList<>(labelGroups.keySet());
            Collections.sort(sortedLabels);

            List<String> labelGroupSignatures = new ArrayList<>();

            for (int label : sortedLabels) {
                List<Integer> nodes = labelGroups.get(label);
                List<String> nodeSignatures = new ArrayList<>();

                for (int node : nodes) {
                    List<String> connections = new ArrayList<>();
                    for (Edge edge : graph.get(node).edge) {
                        int targetLabel = graph.get(edge.to).label;
                        connections.add(targetLabel + ":" + edge.eLabel);
                    }
                    Collections.sort(connections);
                    nodeSignatures.add(connections.toString());
                }

                Collections.sort(nodeSignatures);
                labelGroupSignatures.add(
                    label + "->" + nodeSignatures.toString()
                );
            }

            return String.join(";", labelGroupSignatures);
        }

        private String computeCanonicalEdgeList(Graph graph) {
            List<String> edges = new ArrayList<>();
            Set<String> processedEdges = new HashSet<>();

            for (int from = 0; from < graph.size(); from++) {
                int fromLabel = graph.get(from).label;
                for (Edge edge : graph.get(from).edge) {
                    int toLabel = graph.get(edge.to).label;

                    String edgeStr;
                    if (fromLabel <= toLabel) {
                        edgeStr = fromLabel + "-" + edge.eLabel + "-" + toLabel;
                    } else {
                        edgeStr = toLabel + "-" + edge.eLabel + "-" + fromLabel;
                    }

                    processedEdges.add(edgeStr);
                }
            }

            List<String> sortedEdges = new ArrayList<>(processedEdges);
            Collections.sort(sortedEdges);
            return String.join(";", sortedEdges);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof GraphCanonicalForm)) return false;
            GraphCanonicalForm that = (GraphCanonicalForm) o;
            return (
                nodeCount == that.nodeCount &&
                edgeCount == that.edgeCount &&
                Objects.equals(degreeSequence, that.degreeSequence) &&
                Objects.equals(labelSequence, that.labelSequence) &&
                Objects.equals(adjacencySignature, that.adjacencySignature)
            );
        }

        @Override
        public int hashCode() {
            return Objects.hash(
                nodeCount,
                edgeCount,
                degreeSequence,
                labelSequence,
                adjacencySignature
            );
        }

        @Override
        public String toString() {
            return String.format(
                "Graph[nodes=%d, edges=%d, deg=%s, labels=%s]",
                nodeCount,
                edgeCount,
                degreeSequence,
                labelSequence
            );
        }
    }

    public static List<Graph> removeIsomorphicGraphs(List<Graph> graphs) {
        if (graphs == null || graphs.size() <= 1) {
            return new ArrayList<>(graphs);
        }

        List<Graph> uniqueGraphs = new ArrayList<>();
        Set<GraphCanonicalForm> seenForms = new HashSet<>();
        Map<GraphCanonicalForm, Graph> representativeGraphs = new HashMap<>();

        for (int i = 0; i < graphs.size(); i++) {
            Graph graph = graphs.get(i);
            if (graph == null) continue;

            GraphCanonicalForm canonicalForm = new GraphCanonicalForm(graph);

            if (!seenForms.contains(canonicalForm)) {
                seenForms.add(canonicalForm);
                representativeGraphs.put(canonicalForm, graph);
                uniqueGraphs.add(graph);
            } else {
                Graph representative = representativeGraphs.get(canonicalForm);
                if (!isStrictlyIsomorphic(graph, representative)) {
                    uniqueGraphs.add(graph);
                }
            }
        }

        int originalSize = graphs.size();
        int removedCount = originalSize - uniqueGraphs.size();

        return uniqueGraphs;
    }

    private static boolean isStrictlyIsomorphic(Graph g1, Graph g2) {
        if (g1.size() != g2.size() || g1.getEdgeSize() != g2.getEdgeSize()) {
            return false;
        }

        Map<Integer, Integer> labels1 = new HashMap<>();
        Map<Integer, Integer> labels2 = new HashMap<>();

        for (int i = 0; i < g1.size(); i++) {
            int label = g1.get(i).label;
            labels1.put(label, labels1.getOrDefault(label, 0) + 1);
        }

        for (int i = 0; i < g2.size(); i++) {
            int label = g2.get(i).label;
            labels2.put(label, labels2.getOrDefault(label, 0) + 1);
        }

        if (!labels1.equals(labels2)) {
            return false;
        }

        Map<Integer, Integer> edgeLabels1 = new HashMap<>();
        Map<Integer, Integer> edgeLabels2 = new HashMap<>();

        for (int i = 0; i < g1.size(); i++) {
            for (Edge edge : g1.get(i).edge) {
                edgeLabels1.put(
                    edge.eLabel,
                    edgeLabels1.getOrDefault(edge.eLabel, 0) + 1
                );
            }
        }

        for (int i = 0; i < g2.size(); i++) {
            for (Edge edge : g2.get(i).edge) {
                edgeLabels2.put(
                    edge.eLabel,
                    edgeLabels2.getOrDefault(edge.eLabel, 0) + 1
                );
            }
        }

        return edgeLabels1.equals(edgeLabels2);
    }

    public static String getGraphSignature(Graph graph) {
        GraphCanonicalForm form = new GraphCanonicalForm(graph);
        return form.toString();
    }

    public static List<Graph> removeIsomorphicGraphsPreservingOrder(
        List<Graph> graphs,
        boolean verbose
    ) {
        List<Graph> result = removeIsomorphicGraphs(graphs);
        return result;
    }
}
