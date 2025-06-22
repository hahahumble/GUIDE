package QACMain;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import model.DFS;
import model.DFSCode;
import model.Edge;
import model.Graph;
import model.PDFS;
import model.Projected;
import model.Vertex;

public class GraphIndexManager {

    private final ArrayList<Graph> TRANS;
    private Arguments arg;

    private Map<String, Set<Integer>> vertexLabelIndex = new HashMap<>();
    private Map<String, Set<Integer>> edgeLabelIndex = new HashMap<>();
    private Map<String, Set<Integer>> patternSignatureIndex = new HashMap<>();

    private Map<Integer, String> dfsCodeSignatureCache = new HashMap<>();
    private Map<Integer, Boolean> minimalCheckCache = new HashMap<>();
    private Set<String> generatedGraphSignatures = new HashSet<>();
    private Set<String> quickFeatureVectors = new HashSet<>();
    private Set<String> processedGraphHashSet = new HashSet<>();

    private Set<String> canonicalDFSCodes = new HashSet<>();
    private Map<String, String> graphStructureHashes = new HashMap<>();
    private Map<String, Set<String>> isomorphismGroups = new HashMap<>();
    private Set<String> vertexEdgeLabelSignatures = new HashSet<>();

    private GraphDuplicateDetector duplicateDetector;

    private static final int MAX_SIGNATURE_CACHE_SIZE = 1000000;
    private LinkedHashMap<String, Boolean> frequentSignatureCache =
        new LinkedHashMap<String, Boolean>(10000, 0.75f, true) {
            @Override
            protected boolean removeEldestEntry(
                Map.Entry<String, Boolean> eldest
            ) {
                return size() > MAX_SIGNATURE_CACHE_SIZE;
            }
        };

    private int processedGraphsCount = 0;
    private static final int CACHE_CLEANUP_THRESHOLD = 10000;

    private ExecutorService executor;
    private final int THREAD_COUNT = Runtime.getRuntime().availableProcessors();

    private HashMap<Integer, Set<Integer>> CoveredEdges_OriginalGraphs;
    private Set<Integer> allCoveredEdges;
    private ArrayList<Set<Integer>> CoveredEdges_EachPattern;
    private ArrayList<Integer> Priv_pattern;
    private HashMap<Integer, Set<Integer>> Rcov_edge;
    private Integer numberofcovered;
    private Integer minimumpattern_score;
    private Integer minimumpattern_id;
    private HashMap<Integer, Set<Integer>> Rpriv_i;

    private HashMap<Long, Set<Integer>> CoveredEdges_patterns;
    private HashMap<Integer, Set<Integer>> PatternsID_edges;

    public GraphIndexManager(ArrayList<Graph> TRANS, Arguments arg) {
        this.TRANS = TRANS;
        this.arg = arg;
        this.executor = Executors.newFixedThreadPool(THREAD_COUNT);

        this.duplicateDetector = new GraphDuplicateDetector();

        this.allCoveredEdges = new HashSet<Integer>();
        this.CoveredEdges_OriginalGraphs = new HashMap<Integer, Set<Integer>>();
        this.Priv_pattern = new ArrayList<Integer>();
        this.Rcov_edge = new HashMap<Integer, Set<Integer>>();
        this.numberofcovered = 0;
        this.minimumpattern_score = -1;
        this.Rpriv_i = new HashMap<Integer, Set<Integer>>();
        this.CoveredEdges_EachPattern = new ArrayList<Set<Integer>>();
        this.CoveredEdges_patterns = new HashMap<Long, Set<Integer>>();
        this.PatternsID_edges = new HashMap<Integer, Set<Integer>>();
    }

    public Map<String, Set<Integer>> buildVertexLabelIndex(List<Graph> graphs) {
        Map<String, Set<Integer>> index = new HashMap<>();

        for (int i = 0; i < graphs.size(); i++) {
            Graph g = graphs.get(i);
            for (Vertex v : g) {
                String label = String.valueOf(v.label);
                index.computeIfAbsent(label, k -> new HashSet<>()).add(i);
            }
        }

        return index;
    }

    public Map<String, Set<Integer>> buildEdgeLabelIndex(List<Graph> graphs) {
        Map<String, Set<Integer>> index = new HashMap<>();

        for (int i = 0; i < graphs.size(); i++) {
            Graph g = graphs.get(i);
            for (int from = 0; from < g.size(); from++) {
                for (Edge e : g.get(from).edge) {
                    String label = String.valueOf(e.eLabel);
                    index.computeIfAbsent(label, k -> new HashSet<>()).add(i);
                }
            }
        }

        return index;
    }

    public int DynamicSupportSetting() {
        int loss_score_min = this.minimumpattern_score;
        List<Integer> list = new ArrayList<Integer>();

        for (int id = 0; id < TRANS.size(); ++id) {
            int count = 0;
            for (int nid = 0; nid < TRANS.get(id).size(); ++nid) {
                for (Edge e : TRANS.get(id).get(nid).edge) {
                    Integer edgeid = id * 1000 + e.id;
                    if (
                        this.Rcov_edge.get(edgeid) == null ||
                        this.Rcov_edge.get(edgeid).size() == 0
                    ) continue;
                    count++;
                }
            }
            list.add(count);
        }

        Collections.sort(list);
        int sum = 0;
        int index = list.size() - 1;
        while (sum < 2 * loss_score_min) {
            sum += list.get(index);
            index--;
        }

        int ans = 1;
        if ((list.size() - 1 - index) > ans) ans = list.size() - 1 - index;
        if (ans <= (int) arg.minSup) {
            ans = (int) arg.minSup;
        }

        return ans;
    }

    public int DynamicSupportSetting2() {
        int loss_score_min = this.minimumpattern_score;
        List<Integer> list = new ArrayList<Integer>();

        for (int id = 0; id < TRANS.size(); ++id) {
            int count = 0;
            for (int nid = 0; nid < TRANS.get(id).size(); ++nid) {
                for (Edge e : TRANS.get(id).get(nid).edge) {
                    Integer edgeid = id * 1000 + e.id;
                    if (this.allCoveredEdges.contains(edgeid)) continue;
                    count++;
                }
            }
            list.add(count);
        }

        Collections.sort(list);
        int sum = 0;
        int index = list.size() - 1;
        while (sum < 2 * loss_score_min) {
            sum += list.get(index);
            index--;
        }

        int ans = 1;
        if ((list.size() - 1 - index) > ans) ans = list.size() - 1 - index;
        if (ans <= (int) arg.minSup) {
            ans = (int) arg.minSup;
        }

        return ans;
    }

    public Boolean BranchAndBound(
        Projected projected_g,
        Projected projected_g2,
        Boolean hasupdated
    ) {
        if (hasupdated) {
            int maximum_benefit = 0;
            Set<Integer> temp = new HashSet<Integer>();
            for (PDFS aProjected : projected_g2) {
                int id = aProjected.id;
                if (temp.contains(id) == false) {
                    int size = this.TRANS.get(id).getEdgeSize();
                    int count = 0;
                    for (int i = 0; i < size; i++) {
                        Integer edgeid = 1000 * id + i;
                        if (
                            CoveredEdges_OriginalGraphs.get(id) != null &&
                            CoveredEdges_OriginalGraphs.get(id).contains(edgeid)
                        ) count++;
                    }
                    maximum_benefit = maximum_benefit + size - count;

                    if (arg.swapcondition.equals("swap1")) {
                        if (maximum_benefit > 2 * minimumpattern_score) {
                            return false;
                        }
                    } else if (arg.swapcondition.equals("swap2")) {
                        if (
                            maximum_benefit >
                            minimumpattern_score +
                            (this.allCoveredEdges.size() * 1.0) /
                            arg.numberofpatterns
                        ) {
                            return false;
                        }
                    } else {
                        if (
                            maximum_benefit >
                            (1 + arg.swapAlpha) * minimumpattern_score +
                            (1 - arg.swapAlpha) *
                            ((this.allCoveredEdges.size() * 1.0) /
                                arg.numberofpatterns)
                        ) {
                            return false;
                        }
                    }
                    temp.add(id);
                }
            }
            return true;
        } else {
            return performComplexBranchAndBound(projected_g, projected_g2);
        }
    }

    private boolean performComplexBranchAndBound(
        Projected projected_g,
        Projected projected_g2
    ) {
        if (true) {
            int maximum_benefit = 0;
            Set<Integer> temp = new HashSet<Integer>();
            for (PDFS aProjected : projected_g2) {
                int id = aProjected.id;
                if (temp.contains(id) == false) {
                    int size = this.TRANS.get(id).getEdgeSize();
                    int count = 0;
                    for (int i = 0; i < size; i++) {
                        Integer edgeid = 1000 * id + i;
                        if (this.allCoveredEdges.contains(edgeid)) count++;
                    }
                    maximum_benefit = maximum_benefit + size - count;
                    temp.add(id);
                }
            }

            if (arg.swapcondition.equals("swap1")) {
                if (maximum_benefit <= 2 * minimumpattern_score) {
                    return true;
                }
            } else if (arg.swapcondition.equals("swap2")) {
                if (
                    maximum_benefit <=
                    minimumpattern_score +
                    (this.allCoveredEdges.size() * 1.0) / arg.numberofpatterns
                ) {
                    return true;
                }
            } else {
                if (
                    maximum_benefit <=
                    (1 + arg.swapAlpha) * minimumpattern_score +
                    (1 - arg.swapAlpha) *
                    ((this.allCoveredEdges.size() * 1.0) / arg.numberofpatterns)
                ) {
                    return true;
                }
            }
        }

        return performDetailedBranchAndBound(projected_g, projected_g2);
    }

    private boolean performDetailedBranchAndBound(
        Projected projected_g,
        Projected projected_g2
    ) {
        int maximum_benefit = 0;
        int totaledges = 0;
        Set<Integer> graphIDs = new HashSet<Integer>();
        Set<Integer> Cov_g = new HashSet<Integer>();
        Set<Integer> Cov_g2 = new HashSet<Integer>();
        Set<Integer> Cov_i = new HashSet<Integer>();

        for (PDFS aProjected : projected_g2) {
            int id = aProjected.id;
            for (PDFS p = aProjected; p != null; p = p.prev) {
                Integer temp = 1000 * id + p.edge.id;
                Cov_g2.add(temp);
            }
            graphIDs.add(id);
        }

        for (PDFS aProjected : projected_g) {
            int id = aProjected.id;
            if (graphIDs.contains(id) == false) continue;
            for (PDFS p = aProjected; p != null; p = p.prev) {
                Integer temp = 1000 * id + p.edge.id;
                Cov_g.add(temp);
            }
        }

        for (int id : graphIDs) {
            totaledges += this.TRANS.get(id).getEdgeSize();
            for (int e : allCoveredEdges) if (
                e >= 1000 * id && e < 1000 * (id + 1)
            ) Cov_i.add(e);
        }

        Set<Integer> Cov_diff = new HashSet<Integer>();
        for (Integer e : Cov_g) {
            if (Cov_g2.contains(e) == false && Cov_i.contains(e) == false) {
                Cov_diff.add(e);
            }
        }

        Set<Integer> Cov_union = Cov_i;
        Cov_union.addAll(Cov_diff);

        maximum_benefit = totaledges - Cov_union.size();

        if (arg.swapcondition.equals("swap1")) {
            if (maximum_benefit > 2 * minimumpattern_score) {
                return false;
            }
        } else if (arg.swapcondition.equals("swap2")) {
            if (
                maximum_benefit >
                minimumpattern_score +
                (this.allCoveredEdges.size() * 1.0) / arg.numberofpatterns
            ) {
                return false;
            }
        } else {
            if (
                maximum_benefit >
                (1 + arg.swapAlpha) * minimumpattern_score +
                (1 - arg.swapAlpha) *
                ((this.allCoveredEdges.size() * 1.0) / arg.numberofpatterns)
            ) {
                return false;
            }
        }

        return true;
    }

    public void processBatches(List<Graph> allGraphs) throws IOException {
        int batchSize = 1000;
        int totalGraphs = allGraphs.size();
        int batchCount = (totalGraphs + batchSize - 1) / batchSize;

        ExecutorService batchExecutor = Executors.newFixedThreadPool(
            Math.min(batchCount, Runtime.getRuntime().availableProcessors())
        );

        List<Future<?>> futures = new ArrayList<>();
        for (int i = 0; i < batchCount; i++) {
            final int batchIndex = i;
            futures.add(
                batchExecutor.submit(() -> {
                    int startIdx = batchIndex * batchSize;
                    int endIdx = Math.min(startIdx + batchSize, totalGraphs);

                    try {
                        processBatch(allGraphs, startIdx, endIdx);
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                })
            );
        }

        for (Future<?> future : futures) {
            try {
                future.get();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        batchExecutor.shutdown();
    }

    private void processBatch(List<Graph> allGraphs, int startIdx, int endIdx)
        throws IOException {
        List<Graph> batchGraphs = new ArrayList<>(
            allGraphs.subList(startIdx, endIdx)
        );

        Map<String, Set<Integer>> batchVertexLabelIndex = buildVertexLabelIndex(
            batchGraphs
        );
        Map<String, Set<Integer>> batchEdgeLabelIndex = buildEdgeLabelIndex(
            batchGraphs
        );

        for (int minSize = 3; minSize <= 20; minSize++) {
            minePatterns(
                batchGraphs,
                batchVertexLabelIndex,
                batchEdgeLabelIndex,
                minSize
            );
        }
    }

    private void minePatterns(
        List<Graph> graphs,
        Map<String, Set<Integer>> vertexIndex,
        Map<String, Set<Integer>> edgeIndex,
        int minSize
    ) throws IOException {
        System.out.println(
            "Mining patterns of size " +
            minSize +
            " from " +
            graphs.size() +
            " graphs"
        );
    }

    public String generateGraphHashFromDFS(DFSCode dfsCode) {
        StringBuilder sb = new StringBuilder();

        for (int i = 0; i < dfsCode.size(); i++) {
            DFS dfs = dfsCode.get(i);
            sb
                .append(dfs.from)
                .append(",")
                .append(dfs.to)
                .append(",")
                .append(dfs.fromLabel)
                .append(",")
                .append(dfs.eLabel)
                .append(",")
                .append(dfs.toLabel)
                .append(";");
        }

        return sb.toString();
    }

    public String generateEnhancedGraphHash(DFSCode dfsCode) {
        String basicHash = generateGraphHashFromDFS(dfsCode);

        String structureHash = generateStructureHash(dfsCode);

        String labelHash = generateLabelSequenceHash(dfsCode);

        return basicHash + "|" + structureHash + "|" + labelHash;
    }

    private String generateStructureHash(DFSCode dfsCode) {
        StringBuilder sb = new StringBuilder();

        Map<Integer, Set<Integer>> adjacency = new HashMap<>();
        for (int i = 0; i < dfsCode.size(); i++) {
            DFS dfs = dfsCode.get(i);
            adjacency
                .computeIfAbsent(dfs.from, k -> new TreeSet<>())
                .add(dfs.to);
            adjacency
                .computeIfAbsent(dfs.to, k -> new TreeSet<>())
                .add(dfs.from);
        }

        List<Map.Entry<Integer, Set<Integer>>> sortedVertices = new ArrayList<>(
            adjacency.entrySet()
        );
        sortedVertices.sort((a, b) -> {
            int degreeCompare = Integer.compare(
                b.getValue().size(),
                a.getValue().size()
            );
            if (degreeCompare != 0) return degreeCompare;
            return Integer.compare(a.getKey(), b.getKey());
        });

        for (Map.Entry<Integer, Set<Integer>> entry : sortedVertices) {
            sb
                .append(entry.getKey())
                .append(":")
                .append(entry.getValue().size())
                .append(";");
        }

        return sb.toString();
    }

    private String generateLabelSequenceHash(DFSCode dfsCode) {
        List<String> vertexLabels = new ArrayList<>();
        List<String> edgeLabels = new ArrayList<>();

        for (int i = 0; i < dfsCode.size(); i++) {
            DFS dfs = dfsCode.get(i);
            if (dfs.fromLabel != -1) {
                vertexLabels.add(String.valueOf(dfs.fromLabel));
            }
            if (dfs.toLabel != -1) {
                vertexLabels.add(String.valueOf(dfs.toLabel));
            }
            edgeLabels.add(String.valueOf(dfs.eLabel));
        }

        Collections.sort(vertexLabels);
        Collections.sort(edgeLabels);

        return (
            String.join(",", vertexLabels) + "|" + String.join(",", edgeLabels)
        );
    }

    public boolean isGraphUniqueEnhanced(DFSCode dfsCode) {
        return duplicateDetector.isGraphUnique(dfsCode);
    }

    public boolean isGraphUniqueRelaxed(DFSCode dfsCode) {
        String basicHash = generateGraphHashFromDFS(dfsCode);
        return isGraphUnique(basicHash);
    }

    public boolean isGraphUniqueAdaptive(DFSCode dfsCode) {
        int nodeCount = dfsCode.countNode();
        int edgeCount = dfsCode.size();

        if (nodeCount <= 5 && edgeCount <= 10) {
            return isGraphUniqueModerate(dfsCode);
        } else {
            return isGraphUniqueRelaxed(dfsCode);
        }
    }

    public boolean isGraphUniqueModerate(DFSCode dfsCode) {
        String basicHash = generateGraphHashFromDFS(dfsCode);
        if (processedGraphHashSet.contains(basicHash)) {
            return false;
        }

        String structureSignature = generateSimpleStructureSignature(dfsCode);
        if (quickFeatureVectors.contains(structureSignature)) {
            if (isSimilarGraph(dfsCode, structureSignature)) {
                return false;
            }
        }

        processedGraphHashSet.add(basicHash);
        quickFeatureVectors.add(structureSignature);

        if (processedGraphHashSet.size() > MAX_SIGNATURE_CACHE_SIZE) {
            cleanUpHashCache();
        }

        return true;
    }

    private String generateSimpleStructureSignature(DFSCode dfsCode) {
        int nodeCount = dfsCode.countNode();
        int edgeCount = dfsCode.size();

        Map<Integer, Set<Integer>> adjacency = new HashMap<>();
        for (int i = 0; i < dfsCode.size(); i++) {
            DFS dfs = dfsCode.get(i);
            adjacency
                .computeIfAbsent(dfs.from, k -> new HashSet<>())
                .add(dfs.to);
            adjacency
                .computeIfAbsent(dfs.to, k -> new HashSet<>())
                .add(dfs.from);
        }

        List<Integer> degrees = new ArrayList<>();
        for (Set<Integer> neighbors : adjacency.values()) {
            degrees.add(neighbors.size());
        }
        Collections.sort(degrees);

        return nodeCount + "-" + edgeCount + "-" + degrees.toString();
    }

    private boolean isSimilarGraph(DFSCode dfsCode, String signature) {
        Map<Integer, Integer> vertexLabels = new HashMap<>();
        Map<Integer, Integer> edgeLabels = new HashMap<>();

        for (int i = 0; i < dfsCode.size(); i++) {
            DFS dfs = dfsCode.get(i);
            if (dfs.fromLabel != -1) {
                vertexLabels.put(
                    dfs.fromLabel,
                    vertexLabels.getOrDefault(dfs.fromLabel, 0) + 1
                );
            }
            if (dfs.toLabel != -1) {
                vertexLabels.put(
                    dfs.toLabel,
                    vertexLabels.getOrDefault(dfs.toLabel, 0) + 1
                );
            }
            edgeLabels.put(
                dfs.eLabel,
                edgeLabels.getOrDefault(dfs.eLabel, 0) + 1
            );
        }

        String labelSignature =
            vertexLabels.toString() + "|" + edgeLabels.toString();

        return (
            vertexEdgeLabelSignatures.contains(labelSignature) &&
            !vertexEdgeLabelSignatures.add(labelSignature)
        );
    }

    public void cleanUpHashCache() {
        int targetSize = processedGraphHashSet.size() / 2;
        Set<String> newSet = new HashSet<>(targetSize);

        int count = 0;
        for (String hash : processedGraphHashSet) {
            if (count >= targetSize) break;
            newSet.add(hash);
            count++;
        }

        processedGraphHashSet = newSet;

        if (quickFeatureVectors.size() > targetSize) {
            Set<String> newFeatureSet = new HashSet<>();
            int featureCount = 0;
            for (String feature : quickFeatureVectors) {
                if (featureCount >= targetSize) break;
                newFeatureSet.add(feature);
                featureCount++;
            }
            quickFeatureVectors = newFeatureSet;
        }
    }

    public boolean isGraphUnique(String graphHash) {
        if (processedGraphHashSet.contains(graphHash)) {
            return false;
        }

        processedGraphHashSet.add(graphHash);

        if (processedGraphHashSet.size() > MAX_SIGNATURE_CACHE_SIZE) {
            cleanUpHashCache();
        }

        return true;
    }

    public void printDuplicationStatistics() {
        Map<String, Long> stats = duplicateDetector.getStatistics();
        Map<String, Integer> cacheInfo = duplicateDetector.getCacheInfo();

        System.out.println("\n======= DUPLICATE DETECTION STATISTICS =======");
        System.out.printf("Total Graph Checks: %d\n", stats.get("totalChecks"));
        System.out.printf(
            "Duplicates Found: %d\n",
            stats.get("duplicatesFound")
        );
        System.out.printf("Unique Graphs: %d\n", stats.get("uniqueGraphs"));

        if (stats.containsKey("duplicateRate")) {
            System.out.printf(
                "Duplicate Rate: %d%%\n",
                stats.get("duplicateRate")
            );
        }
        if (stats.containsKey("cacheHitRate")) {
            System.out.printf(
                "Cache Hit Rate: %d%%\n",
                stats.get("cacheHitRate")
            );
        }

        System.out.println("\n--- Cache Usage ---");
        System.out.printf(
            "Basic Hash Cache: %d entries\n",
            cacheInfo.get("basicHashCacheSize")
        );
        System.out.printf(
            "Structure Group Cache: %d entries\n",
            cacheInfo.get("structureGroupCacheSize")
        );
        System.out.printf(
            "Signature Cache: %d entries\n",
            cacheInfo.get("signatureCacheSize")
        );
        System.out.printf(
            "Canonical Form Cache: %d entries\n",
            cacheInfo.get("canonicalFormCacheSize")
        );
        System.out.println("===============================================");
    }

    public void clearDuplicationCaches() {
        duplicateDetector.clearCaches();
    }

    public HashMap<Integer, Set<Integer>> getCoveredEdges_OriginalGraphs() {
        return CoveredEdges_OriginalGraphs;
    }

    public Set<Integer> getAllCoveredEdges() {
        return allCoveredEdges;
    }

    public ArrayList<Set<Integer>> getCoveredEdges_EachPattern() {
        return CoveredEdges_EachPattern;
    }

    public ArrayList<Integer> getPriv_pattern() {
        return Priv_pattern;
    }

    public HashMap<Integer, Set<Integer>> getRcov_edge() {
        return Rcov_edge;
    }

    public Integer getNumberofcovered() {
        return numberofcovered;
    }

    public void setNumberofcovered(Integer numberofcovered) {
        this.numberofcovered = numberofcovered;
    }

    public Integer getMinimumpattern_score() {
        return minimumpattern_score;
    }

    public void setMinimumpattern_score(Integer minimumpattern_score) {
        this.minimumpattern_score = minimumpattern_score;
    }

    public Integer getMinimumpattern_id() {
        return minimumpattern_id;
    }

    public void setMinimumpattern_id(Integer minimumpattern_id) {
        this.minimumpattern_id = minimumpattern_id;
    }

    public HashMap<Integer, Set<Integer>> getRpriv_i() {
        return Rpriv_i;
    }

    public HashMap<Long, Set<Integer>> getCoveredEdges_patterns() {
        return CoveredEdges_patterns;
    }

    public HashMap<Integer, Set<Integer>> getPatternsID_edges() {
        return PatternsID_edges;
    }
}
