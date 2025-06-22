package QACMain;

import java.util.ArrayList;

import model.Edge;
import model.Graph;
import model.History;
import model.Vertex;

public class Misc {
    public static boolean getForwardRoot(Graph g, Vertex v, ArrayList<Edge> result) {
        result.clear();
        for (Edge it : v.edge) {
            assert (it.to >= 0 && it.to < g.size());
            int toLabel = g.get(it.to).label;
            if (toLabel == 0)
                continue;
            result.add(it);
        }

        return !result.isEmpty();
    }

    public static Edge getBackward(Graph graph, Edge e1, Edge e2, History history) {
        if (e1 == e2)
            return null;

        assert (e1.from >= 0 && e1.from < graph.size());
        assert (e1.to >= 0 && e1.to < graph.size());
        assert (e2.to >= 0 && e2.to < graph.size());

        for (Edge it : graph.get(e2.to).edge) {
            if (history.hasEdge(it.id))
                continue;

            if ((it.to == e1.from) && ((e1.eLabel == it.eLabel))) {
                return it;
            }
        }

        return null;
    }
    
    /**
     * Enhanced cycle detection method for long chain structures
     * Relaxes edge label matching requirements to allow more cycle possibilities
     */
    public static Edge getBackwardRelaxed(Graph graph, Edge e1, Edge e2, History history) {
        if (e1 == e2)
            return null;

        assert (e1.from >= 0 && e1.from < graph.size());
        assert (e1.to >= 0 && e1.to < graph.size());
        assert (e2.to >= 0 && e2.to < graph.size());

        for (Edge it : graph.get(e2.to).edge) {
            if (history.hasEdge(it.id))
                continue;

            // Relaxed cycle condition: only require connectivity, not exact edge label match
            if (it.to == e1.from) {
                return it;
            }
            
            // Additional check: also try connecting to e1.to (forming different types of cycles)
            if (it.to == e1.to) {
                return it;
            }
        }

        return null;
    }
    
    /**
     * Specialized long chain cycle detection - checks connection between any two nodes
     */
    public static Edge getChainCycleConnection(Graph graph, int fromNode, int toNode, History history) {
        if (fromNode == toNode || fromNode < 0 || toNode < 0 || 
            fromNode >= graph.size() || toNode >= graph.size()) {
            return null;
        }

        // Check direct connection from fromNode to toNode
        for (Edge edge : graph.get(fromNode).edge) {
            if (edge.to == toNode && !history.hasEdge(edge.id)) {
                return edge;
            }
        }
        
        return null;
    }

    public static boolean getForwardPure(Graph graph, Edge e, int minLabel, History history, ArrayList<Edge> result) {
        result.clear();

        assert (e.to >= 0 && e.to < graph.size());

        // Walk all edges leaving from vertex e.to.
        for (Edge it : graph.get(e.to).edge) {
            // -e. [e.to] -it. [it.to]
            assert (it.to >= 0 && it.to < graph.size());
            int toLabel = graph.get(it.to).label;
            if (history.hasVertex(it.to) || toLabel == 0)
                continue;

            result.add(it);
        }

        return !result.isEmpty();
    }

    public static boolean getForwardRmPath(Graph graph, Edge e, int minLabel, History history, ArrayList<Edge> result) {
        result.clear();
        assert (e.to >= 0 && e.to < graph.size());
        assert (e.from >= 0 && e.from < graph.size());
        int toLabel = graph.get(e.to).label;

        for (Edge it : graph.get(e.from).edge) {
            int toLabel2 = graph.get(it.to).label;
            if (e.to == it.to || history.hasVertex(it.to) || toLabel2 == 0)
                continue;

            result.add(it);
        }

        return !result.isEmpty();
    }
}
