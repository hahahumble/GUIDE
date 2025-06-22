package model;

import java.io.BufferedReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.NavigableMap;
import java.util.NavigableSet;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.Map;
import java.util.HashMap;

public class Graph extends ArrayList<Vertex> {
    private static final long serialVersionUID = 1L;
    public int edge_size = 0;
    public boolean directed = false;

    public Graph() {
    }

    public Graph(boolean directed) {
        this.directed = directed;
    }

    public class ReadResult {
        public BufferedReader reader;
        public boolean valid;

        public ReadResult(BufferedReader is, boolean valid) {
            this.reader = is;
            this.valid = valid;
        }
    }

    public int getEdgeSize() {
    	return edge_size;
    }
    
    void buildEdge() {
        String buf;
        NavigableMap<String, Integer> tmp = new TreeMap<>();

        int id = 0;
        for (int from = 0; from < size(); ++from) {
            for (Edge it : this.get(from).edge) {
                if (directed || from <= it.to)
                    buf = from + " " + it.to + " " + it.eLabel;
                else
                    buf = it.to + " " + from + " " + it.eLabel;

                // Assign unique id's for the edges.
                if (tmp.get(buf) == null) {
                    it.id = id;
                    tmp.put(buf, id);
                    ++id;
                } else {
                    it.id = tmp.get(buf);
                }
            }
        }

        edge_size = id;
    }

    public BufferedReader read(BufferedReader is) throws IOException {
        ArrayList<String> result = new ArrayList<>();
        String line;

        clear();

        while ((line = is.readLine()) != null) {
            result.clear();
            String[] splitRead = line.split(" ");
            Collections.addAll(result, splitRead);

            if (!result.isEmpty()) {
                if (result.get(0).equals("t")) {
                    if (!this.isEmpty()) { // use as delimiter
                        break;
                    }
                } else if (result.get(0).equals("v") && result.size() >= 3) {
                    // int id = Integer.parseInt(result.get(1));
                    Vertex vex = new Vertex();
                    vex.label = Integer.parseInt(result.get(2));
                    this.add(vex);
                } else if (result.get(0).equals("e") && result.size() >= 4) {
                    int from = Integer.parseInt(result.get(1));
                    int to = Integer.parseInt(result.get(2));
                    int eLabel = Integer.parseInt(result.get(3));

                    if (this.size() <= from || this.size() <= to) {
                        System.out.println("Format Error:  define vertex lists before edges");
                        return null;
                    }

                    this.get(from).push(from, to, eLabel);

                    if (!directed) {
                        this.get(to).push(to, from, eLabel);
                    }
                }
            }
        }

        buildEdge();

        return is;
    }
    public BufferedReader read_spcificnum(BufferedReader is, List<Integer> numSet) throws IOException {
        ArrayList<String> result = new ArrayList<>();
        String line;
        boolean readGraph = false;
    
        clear();
    
        while ((line = is.readLine()) != null) {
            result.clear();
            String[] splitRead = line.split(" ");
            Collections.addAll(result, splitRead);
    
            if (!result.isEmpty()) {
                if (result.get(0).equals("t")) {
                    if (!this.isEmpty() && readGraph) { // use as delimiter
                        break;
                    }
                    readGraph = false;
                    if (result.size() >= 3) {
                        int graphId = Integer.parseInt(result.get(2));
                        if (numSet.contains(graphId)) {
                            readGraph = true;
                        }
                    }
                } else if (readGraph && result.get(0).equals("v") && result.size() >= 3) {
                    Vertex vex = new Vertex();
                    vex.label = Integer.parseInt(result.get(2));
                    this.add(vex);
                } else if (readGraph && result.get(0).equals("e") && result.size() >= 4) {
                    int from = Integer.parseInt(result.get(1));
                    int to = Integer.parseInt(result.get(2));
                    int eLabel = Integer.parseInt(result.get(3));
    
                    if (this.size() <= from || this.size() <= to) {
                        System.out.println("Format Error: define vertex lists before edges");
                        return null;
                    }
    
                    this.get(from).push(from, to, eLabel);
    
                    if (!directed) {
                        this.get(to).push(to, from, eLabel);
                    }
                }
            }
        }
    
        if (readGraph) {
            buildEdge();
        }
    
        return is;
    }

    public ReadResult readspnum(BufferedReader is, long[] gl) throws IOException {
        ArrayList<String> result = new ArrayList<>();
        String line;
        boolean valid = false;

        clear();

        while ((line = is.readLine()) != null) {
            result.clear();
            String[] splitRead = line.split(" ");
            Collections.addAll(result, splitRead);

            if (line.trim().isEmpty()) { // If empty line is read, stop reading
                break;
            }

            if (!result.isEmpty()) {
                if (result.get(0).equals("t")) {

                    if (!this.isEmpty()) { // use as delimiter
                        break;
                    }
                    else {
                        if (result.size() >= 3) {
                            long thirdChar = Long.parseLong(result.get(2));
                            for (long num : gl) {
                                if (num == thirdChar) {
                                    valid = true;
                                    break;
                                }
                            }
                        }
                    }
                } else if (result.get(0).equals("v") && result.size() >= 3) {
                    // int id = Integer.parseInt(result.get(1));
                    Vertex vex = new Vertex();
                    vex.label = Integer.parseInt(result.get(2));
                    this.add(vex);
                } else if (result.get(0).equals("e") && result.size() >= 4) {
                    int from = Integer.parseInt(result.get(1));
                    int to = Integer.parseInt(result.get(2));
                    int eLabel = Integer.parseInt(result.get(3));

                    if (this.size() <= from || this.size() <= to) {
                        System.out.println("Format Error:  define vertex lists before edges");
                        return null;
                    }

                    this.get(from).push(from, to, eLabel);

                    if (!directed) {
                        this.get(to).push(to, from, eLabel);
                    }
                }
            }
        }

        buildEdge();

        return new ReadResult(is, valid);
    }

    public void write(FileWriter os) throws IOException {
        String buf;
        
        // Add multi-edge filtering: keep at most one edge between any two nodes
        Map<String, String> edgeMap = new HashMap<>(); // key: "from-to", value: "from to eLabel"
        
        for (int from = 0; from < size(); ++from) {
            for (Edge it : this.get(from).edge) {
                String nodeKey;
                if (directed || from <= it.to) {
                    buf = from + " " + it.to + " " + it.eLabel;
                    nodeKey = from + "-" + it.to;
                } else {
                    buf = it.to + " " + from + " " + it.eLabel;
                    nodeKey = it.to + "-" + from;
                }
                
                // Multi-edge check: if edge exists for this node pair, keep the one with smaller edge label
                if (edgeMap.containsKey(nodeKey)) {
                    String existingEdge = edgeMap.get(nodeKey);
                    String[] existingParts = existingEdge.split(" ");
                    int existingLabel = Integer.parseInt(existingParts[2]);
                    int currentLabel = it.eLabel;
                    
                    // Keep edge with smaller label
                    if (currentLabel < existingLabel) {
                        edgeMap.put(nodeKey, buf);
                    }
                } else {
                    edgeMap.put(nodeKey, buf);
                }
            }
        }
        
        // Use TreeSet to sort final edges
        NavigableSet<String> tmp = new TreeSet<>((o1, o2) -> {
            String[] split1 = o1.split(" ");
            String[] split2 = o2.split(" ");
            if (Integer.parseInt(split1[0]) == Integer.parseInt(split2[0])) {
                if (Integer.parseInt(split1[1]) == Integer.parseInt(split2[1])) {
                    return Integer.parseInt(split1[2]) - Integer.parseInt(split2[2]);
                } else {
                    return Integer.parseInt(split1[1]) - Integer.parseInt(split2[1]);
                }
            } else {
                return Integer.parseInt(split1[0]) - Integer.parseInt(split2[0]);
            }
        });
        
        // Write vertex information
        for (int from = 0; from < size(); ++from) {
            os.write("v " + from + " " + this.get(from).label + System.getProperty("line.separator"));
        }
        
        // Add filtered edges to sorted set
        tmp.addAll(edgeMap.values());

        // Write edge information
        for (String it : tmp) {
            os.write("e " + it + System.getProperty("line.separator"));
        }

        os.flush();
    }

    public void check() {
        /*
         * Check all indices
         */
        for (int from = 0; from < size(); ++from) {
            System.out.println(
                    "check vertex " + from + ", label " + this.get(from).label + System.getProperty("line.separator"));

            for (Edge it : this.get(from).edge) {
                System.out.println("   check edge from " + it.from + " to " + it.to + ", label " + it.eLabel
                        + System.getProperty("line.separator"));
                assert (it.from >= 0 && it.from < size());
                assert (it.to >= 0 && it.to < size());
            }
        }
    }

    public void resize(int size) {
        while (this.size() < size) {
            this.add(new Vertex());
        }
    }
}
