package model;

import java.util.ArrayList;

public class Projected extends ArrayList<PDFS> {
    private static final long serialVersionUID = 1L;

    public void push(int id, Edge edge, PDFS prev) {
        PDFS d = new PDFS(id, edge, prev);
        this.add(d);
    }
}
