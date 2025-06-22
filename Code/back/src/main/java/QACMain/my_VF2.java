package QACMain;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class my_VF2 {

    public static void main(String[] args) {
        if (args.length < 3) {
            System.out.println("args[0]: Graph file");
            System.out.println("args[1]: subGraph file");
            System.out.println("args[2]: output file");
            System.out.println(
                "args[3]: (optional) optimization mode: fast/safe/original (default: fast)"
            );
            System.exit(1);
        }

        long start = System.currentTimeMillis();

        Vf vf2 = new Vf();

        String mode = args.length > 3 ? args[3].toLowerCase() : "fast";

        switch (mode) {
            case "fast":
                System.out.println(
                    "Using fast mode (parallel processing + all optimizations)"
                );
                vf2.vf2runFast(args[0], args[1], args[2], false, null);
                break;
            case "safe":
                System.out.println(
                    "Using safe mode (sequential processing + filtering optimizations)"
                );
                vf2.vf2runSafe(args[0], args[1], args[2], false, null);
                break;
            case "original":
                System.out.println("Using original mode (no optimizations)");
                vf2.setUseParallelProcessing(false);
                vf2.setEnableProgressReporting(false);
                vf2.vf2run(args[0], args[1], args[2], false);
                break;
            default:
                System.out.println(
                    "Unknown mode '" + mode + "', using fast mode"
                );
                vf2.vf2runFast(args[0], args[1], args[2], false, null);
                break;
        }

        long end = System.currentTimeMillis();
        double timeInMinutes = (end - start) / 60000.0;
        double timeInSeconds = (end - start) / 1000.0;
        System.out.println(
            "Total execution time: " +
            String.format("%.2f", timeInSeconds) +
            " seconds (" +
            String.format("%.2f", timeInMinutes) +
            " minutes)"
        );
    }

    static class GraphSet {

        private List<Object[]> graphSet = new ArrayList<>();
        private List<Map<Integer, String>> vertexSet = new ArrayList<>();
        private List<Map<String, String>> edgeSet = new ArrayList<>();
        private Map<Integer, Map<Integer, List<Integer>>> neighborCache =
            new HashMap<>();

        public GraphSet(String inputFile) {
            try (
                BufferedReader br = new BufferedReader(
                    new FileReader(inputFile)
                )
            ) {
                int lineNum = -1;
                Map<Integer, String> curVertexSet = new HashMap<>();
                Map<String, String> curEdgeSet = new HashMap<>();

                String line;
                while ((line = br.readLine()) != null) {
                    String[] lineList = line.trim().split(" ");
                    if (lineList.length == 0) {
                        System.out.println(
                            "Class GraphSet __init__() line split error!"
                        );
                        System.exit(1);
                    }

                    if (lineList[0].equals("t")) {
                        if (lineNum > -1) {
                            Object[] currentGraph = {
                                lineNum,
                                curVertexSet,
                                curEdgeSet,
                            };
                            graphSet.add(currentGraph);
                            vertexSet.add(curVertexSet);
                            edgeSet.add(curEdgeSet);
                        }
                        lineNum += 1;
                        curVertexSet = new HashMap<>();
                        curEdgeSet = new HashMap<>();
                    } else if (lineList[0].equals("v")) {
                        if (lineList.length != 3) {
                            System.out.println(
                                "Class GraphSet __init__() line vertex error!"
                            );
                            System.exit(1);
                        }
                        curVertexSet.put(
                            Integer.parseInt(lineList[1]),
                            lineList[2]
                        );
                    } else if (lineList[0].equals("e")) {
                        if (lineList.length != 4) {
                            System.out.println(
                                "Class GraphSet __init__() line edge error!"
                            );
                            System.exit(1);
                        }

                        int v1 = Integer.parseInt(lineList[1]);
                        int v2 = Integer.parseInt(lineList[2]);
                        String edgeKey =
                            Math.min(v1, v2) + ":" + Math.max(v1, v2);
                        curEdgeSet.put(edgeKey, lineList[3]);
                    }
                }
            } catch (IOException e) {
                System.out.println(
                    "Class GraphSet __init__() Cannot open Graph file: " + e
                );
                System.exit(1);
            }
        }

        public List<Object[]> graphSet() {
            return graphSet;
        }

        public Map<Integer, String> curVSet(int offset) {
            if (offset >= vertexSet.size()) {
                System.out.println(
                    "Class GraphSet curVSet() offset out of index!"
                );
                System.exit(1);
            }
            return vertexSet.get(offset);
        }

        public Map<String, String> curESet(int offset) {
            if (offset >= edgeSet.size()) {
                System.out.println(
                    "Class GraphSet curESet() offset out of index!"
                );
                System.exit(1);
            }
            return edgeSet.get(offset);
        }

        public List<List<String>> curVESet(int offset) {
            if (offset >= vertexSet.size()) {
                System.out.println(
                    "Class GraphSet curVESet() offset out of index!"
                );
                System.exit(1);
            }

            int vertexNum = vertexSet.get(offset).size();
            List<List<String>> result = new ArrayList<>(vertexNum);
            for (int i = 0; i < vertexNum; i++) {
                result.add(new ArrayList<>());
            }

            if (offset < edgeSet.size()) {
                for (String key : edgeSet.get(offset).keySet()) {
                    String[] parts = key.split(":");
                    int v1 = Integer.parseInt(parts[0]);
                    int v2 = Integer.parseInt(parts[1]);

                    if (v1 < vertexNum && v2 < vertexNum) {
                        result.get(v1).add(key);
                        result.get(v2).add(key);
                    } else {
                        System.out.println(
                            "Warning: Vertex index out of bounds for edge: " +
                            key
                        );
                    }
                }
            } else {
                System.out.println(
                    "Warning: edgeSet is not valid for the current offset: " +
                    offset
                );
            }

            return result;
        }

        public List<Integer> neighbor(int offset, int vertexIndex) {
            if (offset >= vertexSet.size()) {
                System.out.println(
                    "Class GraphSet neighbor() offset out of index!"
                );
                System.exit(1);
            }

            if (!neighborCache.containsKey(offset)) {
                neighborCache.put(offset, new HashMap<>());
            }

            Map<Integer, List<Integer>> offsetCache = neighborCache.get(offset);
            if (offsetCache.containsKey(vertexIndex)) {
                return offsetCache.get(vertexIndex);
            }

            List<List<String>> VESet = curVESet(offset);
            List<String> aList = VESet.get(vertexIndex);
            List<Integer> neighborSet = new ArrayList<>();

            for (String edge : aList) {
                String[] parts = edge.split(":");
                int v1 = Integer.parseInt(parts[0]);
                int v2 = Integer.parseInt(parts[1]);

                if (v1 != vertexIndex) {
                    neighborSet.add(v1);
                } else if (v2 != vertexIndex) {
                    neighborSet.add(v2);
                } else {
                    System.exit(1);
                }
            }

            offsetCache.put(vertexIndex, neighborSet);
            return neighborSet;
        }
    }

    static class Mapping {

        private List<Integer> subMap = new ArrayList<>();
        private List<Integer> gMap = new ArrayList<>();

        public Mapping(java.util.Map<Integer, Integer> result) {
            if (result != null) {
                for (Integer key : result.keySet()) {
                    subMap.add(key);
                    gMap.add(result.get(key));
                }
            }
        }

        public List<Integer> subMap() {
            return subMap;
        }

        public List<Integer> gMap() {
            return gMap;
        }

        public boolean isCovered(java.util.Map<Integer, String> vertexSet) {
            return subMap.size() == vertexSet.size();
        }

        public List<Integer> neighbor(
            int offset,
            GraphSet graph,
            int type,
            boolean isInMap
        ) {
            List<List<String>> VESet = graph.curVESet(offset);
            List<Integer> neighbor = new ArrayList<>();
            List<Integer> curMap = (type == 1) ? gMap : subMap;

            for (int index : curMap) {
                List<String> aList = VESet.get(index);
                for (String edge : aList) {
                    String[] parts = edge.split(":");
                    int v1 = Integer.parseInt(parts[0]);
                    int v2 = Integer.parseInt(parts[1]);

                    int v;
                    if (v1 != index) {
                        v = v1;
                    } else if (v2 != index) {
                        v = v2;
                    } else {
                        System.out.println(
                            "Class Mapping subNeighbor() VESet error!"
                        );
                        System.exit(1);
                        return null;
                    }

                    if (isInMap && !neighbor.contains(v)) {
                        neighbor.add(v);
                    } else if (
                        !isInMap && !neighbor.contains(v) && !curMap.contains(v)
                    ) {
                        neighbor.add(v);
                    }
                }
            }

            if (neighbor.isEmpty()) {
                for (Integer index : graph.curVSet(offset).keySet()) {
                    if (!curMap.contains(index)) {
                        neighbor.add(index);
                    }
                }
            }

            return neighbor;
        }
    }

    static class Vf {

        private GraphSet origin = null;
        private GraphSet sub = null;
        private Map<String, String> edgeLabelCache = new HashMap<>();

        private boolean useParallelProcessing = true;
        private boolean enableProgressReporting = true;

        public List<String> candidate(
            List<Integer> subMNeighbor,
            List<Integer> gMNeighbor,
            int i,
            int j
        ) {
            if (
                subMNeighbor == null ||
                subMNeighbor.isEmpty() ||
                gMNeighbor == null ||
                gMNeighbor.isEmpty()
            ) {
                System.out.println(
                    "Class Vf candidate() arguments value error! subMNeighbor or gMNeighbor is empty!"
                );
                System.exit(1);
            }

            List<String> pairs = new ArrayList<>();

            Map<Integer, String> subNodeLabels = new HashMap<>();
            for (int v1 : subMNeighbor) {
                subNodeLabels.put(v1, sub.curVSet(i).get(v1));
            }

            for (int v1 : subMNeighbor) {
                String label1 = subNodeLabels.get(v1);
                for (int v2 : gMNeighbor) {
                    if (label1.equals(origin.curVSet(j).get(v2))) {
                        pairs.add(v1 + ":" + v2);
                    }
                }
            }
            return pairs;
        }

        public List<Integer> preSucc(
            List<Integer> vertexNeighbor,
            List<Integer> map,
            int type
        ) {
            if (type != 0 && type != 1) {
                System.out.println(
                    "Class Vf preSucc() arguments value error! type expected 0 or 1!"
                );
            }

            List<Integer> result = new ArrayList<>();

            if (type == 1) {
                for (int vertex : vertexNeighbor) {
                    if (!map.contains(vertex)) {
                        result.add(vertex);
                    }
                }
            } else {
                for (int vertex : vertexNeighbor) {
                    if (map.contains(vertex)) {
                        result.add(vertex);
                    }
                }
            }
            return result;
        }

        public String edgeLabel(int offset, int index1, int index2, int type) {
            String key;
            if (index1 < index2) {
                key = index1 + ":" + index2;
            } else {
                key = index2 + ":" + index1;
            }

            String cacheKey = offset + ":" + key + ":" + type;
            if (edgeLabelCache.containsKey(cacheKey)) {
                return edgeLabelCache.get(cacheKey);
            }

            java.util.Map<String, String> ESet;
            if (type == 1) {
                ESet = origin.curESet(offset);
            } else {
                ESet = sub.curESet(offset);
            }

            String result = null;
            if (ESet.containsKey(key)) {
                result = ESet.get(key);
            }

            edgeLabelCache.put(cacheKey, result);
            return result;
        }

        public boolean isMatchInV2Succ(
            int j,
            String vertex,
            String edge,
            int v2,
            List<Integer> v2Succ
        ) {
            for (int succ : v2Succ) {
                String vLabel = origin.curVSet(j).get(succ);
                String eLabel = edgeLabel(j, v2, succ, 1);
                if (
                    vLabel.equals(vertex) &&
                    eLabel != null &&
                    eLabel.equals(edge)
                ) {
                    return true;
                }
            }
            return false;
        }

        public boolean isMeetRules(
            int v1,
            int v2,
            int i,
            int j,
            java.util.Map<Integer, Integer> result,
            List<Integer> subMap,
            List<Integer> gMap,
            List<Integer> subMNeighbor,
            List<Integer> gMNeighbor
        ) {
            if (!sub.curVSet(i).get(v1).equals(origin.curVSet(j).get(v2))) {
                return false;
            }

            for (Map.Entry<Integer, Integer> entry : result.entrySet()) {
                int i_a = entry.getKey();
                int j_a = entry.getValue();

                String sKey1 = Math.min(v1, i_a) + ":" + Math.max(v1, i_a);
                String gKey1 = Math.min(v2, j_a) + ":" + Math.max(v2, j_a);

                if (sub.curESet(i).containsKey(sKey1)) {
                    if (!origin.curESet(j).containsKey(gKey1)) {
                        return false;
                    }
                    if (
                        !sub
                            .curESet(i)
                            .get(sKey1)
                            .equals(origin.curESet(j).get(gKey1))
                    ) {
                        return false;
                    }
                }
            }

            return true;
        }

        public java.util.Map<Integer, Integer> dfsMatch(
            int i,
            int j,
            java.util.Map<Integer, Integer> result
        ) {
            if (sub.curVSet(i).size() > origin.curVSet(j).size()) {
                return result;
            }

            Mapping curMap = new Mapping(result);

            if (curMap.isCovered(sub.curVSet(i))) {
                return result;
            }

            List<Integer> subMNeighbor = curMap.neighbor(i, sub, 0, true);
            List<Integer> gMNeighbor = curMap.neighbor(j, origin, 1, true);

            List<Integer> subNMNeighbor = curMap.neighbor(i, sub, 0, false);
            List<Integer> gNMNeighbor = curMap.neighbor(j, origin, 1, false);

            if (result.isEmpty()) {
                subNMNeighbor.clear();
                gNMNeighbor.clear();
                subNMNeighbor.add(0);

                for (Integer vertex : origin.curVSet(j).keySet()) {
                    gNMNeighbor.add(vertex);
                }
            }

            while (subNMNeighbor.size() > 1) {
                subNMNeighbor.remove(subNMNeighbor.size() - 1);
            }

            List<String> pairs = candidate(subNMNeighbor, gNMNeighbor, i, j);
            if (pairs.isEmpty()) {
                return result;
            }

            for (String pair : pairs) {
                String[] parts = pair.split(":");
                int v1 = Integer.parseInt(parts[0]);
                int v2 = Integer.parseInt(parts[1]);

                if (
                    isMeetRules(
                        v1,
                        v2,
                        i,
                        j,
                        result,
                        curMap.subMap(),
                        curMap.gMap(),
                        subMNeighbor,
                        gMNeighbor
                    )
                ) {
                    result.put(v1, v2);
                    dfsMatch(i, j, result);

                    if (result.size() == sub.curVSet(i).size()) {
                        return result;
                    }
                    result.remove(v1);
                }
            }
            return result;
        }

        private Map<String, Integer> extractNodeLabelFrequency(
            GraphSet graphSet,
            int graphId
        ) {
            Map<String, Integer> frequency = new HashMap<>();
            Map<Integer, String> vertices = graphSet.curVSet(graphId);

            for (String label : vertices.values()) {
                frequency.put(label, frequency.getOrDefault(label, 0) + 1);
            }

            return frequency;
        }

        private Map<String, Integer> extractEdgeLabelFrequency(
            GraphSet graphSet,
            int graphId
        ) {
            Map<String, Integer> frequency = new HashMap<>();
            Map<String, String> edges = graphSet.curESet(graphId);

            for (String label : edges.values()) {
                frequency.put(label, frequency.getOrDefault(label, 0) + 1);
            }

            return frequency;
        }

        private Set<Integer> filterCandidatesByLabels(
            int queryId,
            int totalGraphs
        ) {
            Set<Integer> candidates = new HashSet<>();

            Map<String, Integer> queryNodeLabels = extractNodeLabelFrequency(
                sub,
                queryId
            );
            Map<String, Integer> queryEdgeLabels = extractEdgeLabelFrequency(
                sub,
                queryId
            );

            for (int graphId = 0; graphId < totalGraphs; graphId++) {
                boolean isCandidate = true;

                if (
                    sub.curVSet(queryId).size() > origin.curVSet(graphId).size()
                ) {
                    continue;
                }

                Map<String, Integer> graphNodeLabels =
                    extractNodeLabelFrequency(origin, graphId);
                for (Map.Entry<
                    String,
                    Integer
                > entry : queryNodeLabels.entrySet()) {
                    String label = entry.getKey();
                    int requiredCount = entry.getValue();
                    if (
                        graphNodeLabels.getOrDefault(label, 0) < requiredCount
                    ) {
                        isCandidate = false;
                        break;
                    }
                }

                if (!isCandidate) continue;

                Map<String, Integer> graphEdgeLabels =
                    extractEdgeLabelFrequency(origin, graphId);
                for (Map.Entry<
                    String,
                    Integer
                > entry : queryEdgeLabels.entrySet()) {
                    String label = entry.getKey();
                    int requiredCount = entry.getValue();
                    if (
                        graphEdgeLabels.getOrDefault(label, 0) < requiredCount
                    ) {
                        isCandidate = false;
                        break;
                    }
                }

                if (isCandidate) {
                    candidates.add(graphId);
                }
            }

            return candidates;
        }

        private void processInParallel(
            int queryId,
            Set<Integer> candidates,
            List<Integer> matches,
            BufferedWriter writer
        ) throws IOException {
            int processorCount = Runtime.getRuntime().availableProcessors();
            ExecutorService executor = Executors.newFixedThreadPool(
                processorCount
            );
            Map<Integer, Map<Integer, Integer>> allResults =
                new ConcurrentHashMap<>();

            final int queryVertexCount = sub.curVSet(queryId).size();

            AtomicInteger processedCount = new AtomicInteger(0);
            AtomicInteger matchedCount = new AtomicInteger(0);
            final int totalCandidates = candidates.size();

            List<Integer> candidatesList = new ArrayList<>(candidates);
            int batchSize = Math.max(1, candidatesList.size() / processorCount);
            List<Future<?>> futures = new ArrayList<>();

            Thread progressThread = null;
            if (enableProgressReporting && totalCandidates > 100) {
                progressThread = new Thread(() -> {
                    while (!Thread.currentThread().isInterrupted()) {
                        try {
                            Thread.sleep(2000);
                            int processed = processedCount.get();
                            int matched = matchedCount.get();
                            if (processed > 0) {
                                System.out.printf(
                                    "Progress: %d/%d (%.1f%%) candidates processed, %d matches found%n",
                                    processed,
                                    totalCandidates,
                                    (processed * 100.0) / totalCandidates,
                                    matched
                                );
                            }
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                            break;
                        }
                    }
                });
                progressThread.setDaemon(true);
                progressThread.start();
            }

            for (
                int batchStart = 0;
                batchStart < candidatesList.size();
                batchStart += batchSize
            ) {
                int batchEnd = Math.min(
                    batchStart + batchSize,
                    candidatesList.size()
                );
                List<Integer> batch = candidatesList.subList(
                    batchStart,
                    batchEnd
                );

                futures.add(
                    executor.submit(() -> {
                        for (int graphId : batch) {
                            try {
                                Map<Integer, Integer> result = new HashMap<>();
                                result = dfsMatch(queryId, graphId, result);

                                if (
                                    result != null &&
                                    result.size() == queryVertexCount
                                ) {
                                    synchronized (matches) {
                                        matches.add(graphId);
                                    }
                                    allResults.put(graphId, result);
                                    matchedCount.incrementAndGet();
                                }

                                processedCount.incrementAndGet();
                            } catch (Exception e) {
                                System.err.println(
                                    "Error processing graph " +
                                    graphId +
                                    ": " +
                                    e.getMessage()
                                );
                                processedCount.incrementAndGet();
                            }
                        }
                    })
                );
            }

            for (Future<?> future : futures) {
                try {
                    future.get();
                } catch (Exception e) {
                    System.err.println(
                        "Error in parallel execution: " + e.getMessage()
                    );
                }
            }

            executor.shutdown();
            try {
                executor.awaitTermination(1, TimeUnit.HOURS);
            } catch (InterruptedException e) {
                System.err.println("Executor interrupted: " + e.getMessage());
            }

            if (progressThread != null) {
                progressThread.interrupt();
            }

            Collections.sort(matches);

            for (int graphId : matches) {
                Map<Integer, Integer> result = allResults.get(graphId);
                if (result != null) {
                    writeMatchDetails(
                        writer,
                        graphId,
                        result,
                        queryVertexCount
                    );
                }
            }
        }

        private void processSequentially(
            int queryId,
            Set<Integer> candidates,
            List<Integer> matches,
            BufferedWriter writer
        ) throws IOException {
            int queryVertexCount = sub.curVSet(queryId).size();
            int processedCount = 0;
            int totalCandidates = candidates.size();
            Map<Integer, Map<Integer, Integer>> allResults = new HashMap<>();

            for (int graphId : candidates) {
                try {
                    processedCount++;
                    if (
                        enableProgressReporting &&
                        totalCandidates > 100 &&
                        (processedCount % 100 == 0 ||
                            processedCount == totalCandidates)
                    ) {
                        System.out.printf(
                            "Progress: %d/%d (%.1f%%) candidates processed, %d matches found%n",
                            processedCount,
                            totalCandidates,
                            (processedCount * 100.0) / totalCandidates,
                            matches.size()
                        );
                    }

                    Map<Integer, Integer> result = new HashMap<>();
                    result = dfsMatch(queryId, graphId, result);

                    if (result != null && result.size() == queryVertexCount) {
                        matches.add(graphId);
                        allResults.put(graphId, result);
                    }
                } catch (Exception e) {
                    System.err.println(
                        "Error processing graph " +
                        graphId +
                        ": " +
                        e.getMessage()
                    );
                }
            }

            Collections.sort(matches);

            for (int graphId : matches) {
                Map<Integer, Integer> result = allResults.get(graphId);
                if (result != null) {
                    writeMatchDetails(
                        writer,
                        graphId,
                        result,
                        queryVertexCount
                    );
                }
            }
        }

        private void writeMatchDetails(
            BufferedWriter writer,
            int graphId,
            Map<Integer, Integer> result,
            int vertexCount
        ) throws IOException {
            writer.write("In: Graph " + graphId + " [");
            int[] mappingArray = new int[vertexCount];
            for (Map.Entry<Integer, Integer> entry : result.entrySet()) {
                if (entry.getKey() < vertexCount) {
                    mappingArray[entry.getKey()] = entry.getValue();
                }
            }
            for (int k = 0; k < mappingArray.length; k++) {
                writer.write(String.valueOf(mappingArray[k]));
                if (k < mappingArray.length - 1) {
                    writer.write(", ");
                }
            }
            writer.write("]\n");
        }

        public void setUseParallelProcessing(boolean useParallel) {
            this.useParallelProcessing = useParallel;
        }

        public void setEnableProgressReporting(boolean enableProgress) {
            this.enableProgressReporting = enableProgress;
        }

        public void mainWithSpecificGraphs(
            String f1,
            String f2,
            String outputFile,
            boolean rerun,
            Arguments arguments
        ) {
            System.out.println("Loading graph data...");
            long loadStart = System.currentTimeMillis();
            origin = new GraphSet(f1);
            sub = new GraphSet(f2);
            long loadEnd = System.currentTimeMillis();
            System.out.println(
                "Graph data loaded in " + (loadEnd - loadStart) + " ms"
            );

            int subLen = sub.graphSet().size();
            int gLen = origin.graphSet().size();

            try (
                BufferedWriter writer = new BufferedWriter(
                    new FileWriter(outputFile)
                )
            ) {
                List<Integer> allMatches = new ArrayList<>();

                for (int i = 0; i < subLen; i++) {
                    List<Integer> matches = new ArrayList<>();
                    writer.write("Maps for: Query " + i + "\n");

                    Set<Integer> candidates;

                    if (
                        rerun &&
                        arguments != null &&
                        arguments.IDs != null &&
                        !arguments.IDs.isEmpty()
                    ) {
                        candidates = new HashSet<>(arguments.IDs);
                        System.out.println(
                            "VF2 reusing " +
                            arguments.IDs.size() +
                            " previously matched graph IDs"
                        );
                    } else {
                        long filterStart = System.currentTimeMillis();
                        candidates = filterCandidatesByLabels(i, gLen);
                        long filterEnd = System.currentTimeMillis();
                        System.out.println(
                            "Label-based filtering took " +
                            (filterEnd - filterStart) +
                            " ms"
                        );
                        System.out.println(
                            "Reduced search space from " +
                            gLen +
                            " to " +
                            candidates.size() +
                            " candidates"
                        );

                        if (candidates.isEmpty()) {
                            System.out.println(
                                "No candidates found for query " + i
                            );
                            writer.write("[]\n");
                            continue;
                        }
                    }

                    long matchStart = System.currentTimeMillis();
                    if (useParallelProcessing && candidates.size() > 50) {
                        System.out.println(
                            "Processing " +
                            candidates.size() +
                            " candidates in parallel"
                        );
                        processInParallel(i, candidates, matches, writer);
                    } else {
                        System.out.println(
                            "Processing " +
                            candidates.size() +
                            " candidates sequentially"
                        );
                        processSequentially(i, candidates, matches, writer);
                    }
                    long matchEnd = System.currentTimeMillis();
                    System.out.println(
                        "Matching completed in " +
                        (matchEnd - matchStart) +
                        " ms, found " +
                        matches.size() +
                        " matches"
                    );

                    Collections.sort(matches);
                    writer.write(matches.toString() + "\n");

                    allMatches.addAll(matches);
                }

                System.out.println("Total matches found: " + allMatches.size());
                if (!allMatches.isEmpty()) {
                    System.out.println(
                        "Match distribution: " +
                        String.format(
                            "%.2f",
                            (allMatches.size() / (double) subLen)
                        ) +
                        " matches per query graph"
                    );
                }
            } catch (IOException e) {
                System.out.println(
                    "Error writing to output file: " + e.getMessage()
                );
            }
        }

        public void vf2run(
            String f1,
            String f2,
            String outputFile,
            boolean rerun
        ) {
            mainWithSpecificGraphs(f1, f2, outputFile, rerun, null);
        }

        public void vf2run(
            String f1,
            String f2,
            String outputFile,
            boolean rerun,
            Arguments arguments
        ) {
            if (!rerun && arguments != null) {
                arguments.IDs = null;
            }

            if (
                arguments != null &&
                arguments.IDs != null &&
                !arguments.IDs.isEmpty()
            ) {
                System.out.println(
                    "VF2: Will only search through previously matched graphs"
                );
            } else if (rerun) {
                System.out.println(
                    "VF2: Rerun mode is enabled but no matching graph IDs were received"
                );
                System.out.println("VF2: Will search the entire database");
            }

            mainWithSpecificGraphs(f1, f2, outputFile, rerun, arguments);
        }

        public void vf2runOptimized(
            String f1,
            String f2,
            String outputFile,
            boolean rerun,
            Arguments arguments,
            boolean useParallel,
            boolean enableProgress
        ) {
            setUseParallelProcessing(useParallel);
            setEnableProgressReporting(enableProgress);
            mainWithSpecificGraphs(f1, f2, outputFile, rerun, arguments);
        }

        public void vf2runFast(
            String f1,
            String f2,
            String outputFile,
            boolean rerun,
            Arguments arguments
        ) {
            vf2runOptimized(f1, f2, outputFile, rerun, arguments, true, true);
        }

        public void vf2runSafe(
            String f1,
            String f2,
            String outputFile,
            boolean rerun,
            Arguments arguments
        ) {
            vf2runOptimized(f1, f2, outputFile, rerun, arguments, false, true);
        }
    }

    public static List<Integer> extractGraphIDs(String filePath) {
        List<Integer> graphIDs = new ArrayList<>();

        try (
            BufferedReader reader = new BufferedReader(
                new FileReader(filePath),
                16384
            )
        ) {
            String line;

            while ((line = reader.readLine()) != null) {
                if (line.startsWith("[") && line.endsWith("]")) {
                    String content = line.substring(1, line.length() - 1);

                    if (!content.contains(",") && !content.trim().isEmpty()) {
                        try {
                            int singleId = Integer.parseInt(content.trim());
                            graphIDs.add(singleId);
                        } catch (NumberFormatException e) {
                            System.err.println(
                                "VF2: Error parsing single ID: " + content
                            );
                        }
                    } else if (content.contains(",")) {
                        String[] idStrings = content.split(", ");
                        for (String idString : idStrings) {
                            try {
                                graphIDs.add(Integer.parseInt(idString.trim()));
                            } catch (NumberFormatException e) {
                                System.err.println(
                                    "VF2: Error parsing ID in array: " +
                                    idString
                                );
                            }
                        }
                    }
                    break;
                }
            }
        } catch (IOException e) {
            System.err.println(
                "VF2: Error reading VF2 results file: " + e.getMessage()
            );
        }
        return graphIDs;
    }

    static class GraphIndex {

        private Map<String, Set<Integer>> nodeLabelIndex = new HashMap<>();
        private Map<String, Set<Integer>> edgeLabelIndex = new HashMap<>();
        private Map<Integer, Map<String, Integer>> graphNodeCounts =
            new HashMap<>();
        private Map<Integer, Map<String, Integer>> graphEdgeCounts =
            new HashMap<>();

        public void addGraph(
            int graphId,
            Map<Integer, String> vertices,
            Map<String, String> edges
        ) {
            Map<String, Integer> nodeCounts = new HashMap<>();
            for (String label : vertices.values()) {
                nodeLabelIndex
                    .computeIfAbsent(label, k -> new HashSet<>())
                    .add(graphId);
                nodeCounts.put(label, nodeCounts.getOrDefault(label, 0) + 1);
            }
            graphNodeCounts.put(graphId, nodeCounts);

            Map<String, Integer> edgeCounts = new HashMap<>();
            for (String label : edges.values()) {
                edgeLabelIndex
                    .computeIfAbsent(label, k -> new HashSet<>())
                    .add(graphId);
                edgeCounts.put(label, edgeCounts.getOrDefault(label, 0) + 1);
            }
            graphEdgeCounts.put(graphId, edgeCounts);
        }

        public Set<Integer> getCandidateGraphs(
            Map<String, Integer> queryNodeLabels,
            Map<String, Integer> queryEdgeLabels
        ) {
            Set<Integer> candidates = null;

            for (Map.Entry<
                String,
                Integer
            > entry : queryNodeLabels.entrySet()) {
                String label = entry.getKey();
                int requiredCount = entry.getValue();

                Set<Integer> graphsWithLabel = nodeLabelIndex.get(label);
                if (graphsWithLabel == null) {
                    return new HashSet<>();
                }

                Set<Integer> validGraphs = new HashSet<>();
                for (Integer graphId : graphsWithLabel) {
                    Map<String, Integer> graphCounts = graphNodeCounts.get(
                        graphId
                    );
                    if (
                        graphCounts != null &&
                        graphCounts.getOrDefault(label, 0) >= requiredCount
                    ) {
                        validGraphs.add(graphId);
                    }
                }

                if (candidates == null) {
                    candidates = validGraphs;
                } else {
                    candidates.retainAll(validGraphs);
                }

                if (candidates.isEmpty()) {
                    return candidates;
                }
            }

            if (candidates == null) {
                candidates = new HashSet<>();

                candidates.addAll(graphNodeCounts.keySet());
            }

            for (Map.Entry<
                String,
                Integer
            > entry : queryEdgeLabels.entrySet()) {
                String label = entry.getKey();
                int requiredCount = entry.getValue();

                Set<Integer> graphsWithLabel = edgeLabelIndex.get(label);
                if (graphsWithLabel == null) {
                    return new HashSet<>();
                }

                Set<Integer> validGraphs = new HashSet<>();
                for (Integer graphId : graphsWithLabel) {
                    Map<String, Integer> graphCounts = graphEdgeCounts.get(
                        graphId
                    );
                    if (
                        graphCounts != null &&
                        graphCounts.getOrDefault(label, 0) >= requiredCount
                    ) {
                        validGraphs.add(graphId);
                    }
                }

                candidates.retainAll(validGraphs);

                if (candidates.isEmpty()) {
                    return candidates;
                }
            }

            return candidates;
        }
    }

    static class GraphIndexer {

        public static void buildIndex(String graphFile, String indexFile) {
            System.out.println("Building graph index...");
            GraphSet graphSet = new GraphSet(graphFile);
            GraphIndex index = new GraphIndex();

            int graphCount = graphSet.graphSet().size();
            for (int i = 0; i < graphCount; i++) {
                Map<Integer, String> vertices = graphSet.curVSet(i);
                Map<String, String> edges = graphSet.curESet(i);
                index.addGraph(i, vertices, edges);
            }

            System.out.println(
                "Graph index built with " + graphCount + " graphs"
            );
        }

        public static GraphIndex loadIndex(String indexFile) {
            return null;
        }
    }
}
