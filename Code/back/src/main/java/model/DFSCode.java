package model;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Set;
import java.util.Map;
import java.util.HashMap;

public class DFSCode extends ArrayList<DFS> {
    private static final long serialVersionUID = 1L;
    private ArrayList<Integer> rmPath;
    
    // Add query graph node label protection
    private static Map<Integer, Integer> queryNodeLabels = new HashMap<>();
    
    public DFSCode() {
        rmPath = new ArrayList<>();
    }
    
    /**
     * Set query graph node labels to protect original labels from being overwritten
     */
    public static void setQueryNodeLabels(Map<Integer, Integer> labels) {
        queryNodeLabels.clear();
        queryNodeLabels.putAll(labels);
    }
    
    /**
     * Clear query graph node label protection
     */
    public static void clearQueryNodeLabels() {
        queryNodeLabels.clear();
    }

    public void push(int from, int to, int fromLabel, int eLabel, int toLabel) {
        // Check for duplicate edges before adding DFS
        for (DFS existing : this) {
            if ((existing.from == from && existing.to == to) ||
                (existing.from == to && existing.to == from)) {
                // Connection already exists, skip adding
                return;
            }
        }
        
        DFS d = new DFS();
        d.from = from;
        d.to = to;
        d.fromLabel = fromLabel;
        d.eLabel = eLabel;
        d.toLabel = toLabel;
        this.add(d);
    }

    public void pop() {
        this.remove(this.size() - 1);
    }

    public void toGraph(Graph g) {
        g.clear();

        // First pass: determine the required size and initialize all nodes
        int maxNodeId = -1;
        for (DFS it : this) {
            maxNodeId = Math.max(maxNodeId, Math.max(it.from, it.to));
        }
        
        if (maxNodeId >= 0) {
            g.resize(maxNodeId + 1);
        }

        // Second pass: set node labels (avoid overwriting with -1)
        for (DFS it : this) {
            if (it.fromLabel != -1) {
                g.get(it.from).label = it.fromLabel;
            }
            if (it.toLabel != -1) {
                g.get(it.to).label = it.toLabel;
            }
        }
        
        // Third pass: smart label repair - prioritize query graph labels, avoid incorrect overwrites
        for (int i = 0; i < g.size(); i++) {
            if (g.get(i).label == -1) {
                // If this is a query graph node, use original label
                if (queryNodeLabels.containsKey(i)) {
                    g.get(i).label = queryNodeLabels.get(i);
                } else {
                    // For newly extended nodes, use default label 0
                    g.get(i).label = 0;
                }
            }
        }

        // Fourth pass: add edges (check for duplicates)
        Set<String> addedEdges = new HashSet<>();
        for (DFS it : this) {
            String edgeKey = it.from + "_" + it.to + "_" + it.eLabel;
            if (!addedEdges.contains(edgeKey)) {
                g.get(it.from).push(it.from, it.to, it.eLabel);
                addedEdges.add(edgeKey);
                
                if (!g.directed) {
                    String reverseEdgeKey = it.to + "_" + it.from + "_" + it.eLabel;
                    if (!addedEdges.contains(reverseEdgeKey)) {
                        g.get(it.to).push(it.to, it.from, it.eLabel);
                        addedEdges.add(reverseEdgeKey);
                    }
                }
            }
        }

        g.buildEdge();
    }

    public ArrayList<Integer> buildRMPath() {
        rmPath.clear();

        int old_from = -1;

        for (int i = size() - 1; i >= 0; --i) {
            if (this.get(i).from < this.get(i).to && // forward
                    (rmPath.isEmpty() || old_from == this.get(i).to)) {
                rmPath.add(i);
                old_from = this.get(i).from;
            }
        }

        return rmPath;
    }

    /**
     * Return number of nodes in the graph.
     * @return number of nodes in the graph
     */
    public int countNode() {
        int nodeCount = 0;

        for (DFS it : this)
            nodeCount = Math.max(nodeCount, Math.max(it.from, it.to) + 1);

        return nodeCount;
    }
}
