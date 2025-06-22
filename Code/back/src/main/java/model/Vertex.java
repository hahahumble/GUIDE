package model;

import java.util.ArrayList;

public class Vertex {
    public int label;
    public ArrayList<Edge> edge;

    public Vertex() {
        label = -1;  // Use -1 as default to indicate uninitialized label
        edge = new ArrayList<>();
    }

    public void push(int from, int to, int eLabel) {
        // Check if edge already exists to avoid duplicates
        for (Edge existingEdge : edge) {
            if (existingEdge.from == from && existingEdge.to == to && existingEdge.eLabel == eLabel) {
                // Edge already exists, skip adding
                return;
            }
        }
        
        Edge e = new Edge();
        e.from = from;
        e.to = to;
        e.eLabel = eLabel;
        edge.add(e);
    }
}
