package model;

public class PDFS {
    // ID of the original input graph
    public int id = 0;
    public Edge edge = null;
    public PDFS prev = null;

    // Constructor to initialize PDFS object
    public PDFS(int id, Edge edge, PDFS prev) {
        this.id = id;
        this.edge = edge;
        this.prev = prev;
    }
}
