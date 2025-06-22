package QACMain;

import java.util.*;
import model.DFS;
import model.DFSCode;

public class GraphDuplicateDetector {

    private final Set<String> basicHashCache = new HashSet<>();
    private final Map<String, List<String>> structureGroupCache =
        new HashMap<>();
    private final Map<String, GraphSignature> signatureCache = new HashMap<>();
    private final Set<String> canonicalFormCache = new HashSet<>();

    private long totalChecks = 0;
    private long duplicatesFound = 0;
    private long cacheHits = 0;

    private static class GraphSignature {

        final int nodeCount;
        final int edgeCount;
        final String degreeSequence;
        final String labelDistribution;
        final String structureHash;

        GraphSignature(DFSCode dfsCode) {
            this.nodeCount = dfsCode.countNode();
            this.edgeCount = dfsCode.size();
            this.degreeSequence = computeDegreeSequence(dfsCode);
            this.labelDistribution = computeLabelDistribution(dfsCode);
            this.structureHash = computeStructureHash(dfsCode);
        }

        private String computeDegreeSequence(DFSCode dfsCode) {
            Map<Integer, Integer> degreeCount = new HashMap<>();
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

            for (Set<Integer> neighbors : adjacency.values()) {
                int degree = neighbors.size();
                degreeCount.put(
                    degree,
                    degreeCount.getOrDefault(degree, 0) + 1
                );
            }

            List<Integer> degrees = new ArrayList<>(degreeCount.keySet());
            Collections.sort(degrees);

            StringBuilder sb = new StringBuilder();
            for (int degree : degrees) {
                sb
                    .append(degree)
                    .append(":")
                    .append(degreeCount.get(degree))
                    .append(",");
            }
            return sb.toString();
        }

        private String computeLabelDistribution(DFSCode dfsCode) {
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

            return (
                "V:" + vertexLabels.toString() + ",E:" + edgeLabels.toString()
            );
        }

        private String computeStructureHash(DFSCode dfsCode) {
            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < dfsCode.size(); i++) {
                DFS dfs = dfsCode.get(i);
                sb.append(dfs.from).append("-").append(dfs.to).append(";");
            }
            return sb.toString();
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof GraphSignature)) return false;
            GraphSignature that = (GraphSignature) o;
            return (
                nodeCount == that.nodeCount &&
                edgeCount == that.edgeCount &&
                Objects.equals(degreeSequence, that.degreeSequence) &&
                Objects.equals(labelDistribution, that.labelDistribution) &&
                Objects.equals(structureHash, that.structureHash)
            );
        }

        @Override
        public int hashCode() {
            return Objects.hash(
                nodeCount,
                edgeCount,
                degreeSequence,
                labelDistribution,
                structureHash
            );
        }
    }

    public boolean isGraphUnique(DFSCode dfsCode) {
        totalChecks++;

        String basicHash = generateBasicHash(dfsCode);
        if (basicHashCache.contains(basicHash)) {
            duplicatesFound++;
            cacheHits++;
            return false;
        }

        String simpleSignature = generateSimpleSignature(dfsCode);
        if (signatureCache.containsKey(simpleSignature)) {
            GraphSignature existing = signatureCache.get(simpleSignature);
            GraphSignature current = new GraphSignature(dfsCode);

            if (
                current.nodeCount == existing.nodeCount &&
                current.edgeCount == existing.edgeCount &&
                current.structureHash.equals(existing.structureHash) &&
                current.labelDistribution.equals(existing.labelDistribution)
            ) {
                duplicatesFound++;
                return false;
            }
        }

        basicHashCache.add(basicHash);
        signatureCache.put(simpleSignature, new GraphSignature(dfsCode));

        maintainCacheSize();

        return true;
    }

    private String generateSimpleSignature(DFSCode dfsCode) {
        return (
            dfsCode.countNode() +
            "-" +
            dfsCode.size() +
            "-" +
            generateBasicStructureHash(dfsCode)
        );
    }

    private String generateBasicStructureHash(DFSCode dfsCode) {
        StringBuilder sb = new StringBuilder();

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

        return degrees.toString();
    }

    private String generateBasicHash(DFSCode dfsCode) {
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

    private String generateCanonicalForm(DFSCode dfsCode) {
        List<String> edges = new ArrayList<>();
        for (int i = 0; i < dfsCode.size(); i++) {
            DFS dfs = dfsCode.get(i);

            int from = Math.min(dfs.from, dfs.to);
            int to = Math.max(dfs.from, dfs.to);
            edges.add(from + "-" + to + "-" + dfs.eLabel);
        }

        Collections.sort(edges);
        return String.join(";", edges);
    }

    private boolean arePotentiallyIsomorphic(String hash1, String hash2) {
        if (hash1.equals(hash2)) {
            return true;
        }

        String[] parts1 = hash1.split(";");
        String[] parts2 = hash2.split(";");

        return parts1.length == parts2.length;
    }

    private void maintainCacheSize() {
        final int MAX_CACHE_SIZE = 100000;

        if (basicHashCache.size() > MAX_CACHE_SIZE) {
            Set<String> newCache = new HashSet<>();
            int count = 0;
            int target = MAX_CACHE_SIZE / 2;

            for (String hash : basicHashCache) {
                if (count >= target) break;
                newCache.add(hash);
                count++;
            }

            basicHashCache.clear();
            basicHashCache.addAll(newCache);
        }

        if (signatureCache.size() > MAX_CACHE_SIZE) {
            Map<String, GraphSignature> newSignatureCache = new HashMap<>();
            int count = 0;
            int target = MAX_CACHE_SIZE / 2;

            for (Map.Entry<
                String,
                GraphSignature
            > entry : signatureCache.entrySet()) {
                if (count >= target) break;
                newSignatureCache.put(entry.getKey(), entry.getValue());
                count++;
            }

            signatureCache.clear();
            signatureCache.putAll(newSignatureCache);
        }
    }

    public Map<String, Long> getStatistics() {
        Map<String, Long> stats = new HashMap<>();
        stats.put("totalChecks", totalChecks);
        stats.put("duplicatesFound", duplicatesFound);
        stats.put("cacheHits", cacheHits);
        stats.put("uniqueGraphs", totalChecks - duplicatesFound);

        if (totalChecks > 0) {
            stats.put("duplicateRate", (duplicatesFound * 100) / totalChecks);
            stats.put("cacheHitRate", (cacheHits * 100) / totalChecks);
        }

        return stats;
    }

    public void clearCaches() {
        basicHashCache.clear();
        structureGroupCache.clear();
        signatureCache.clear();
        canonicalFormCache.clear();

        totalChecks = 0;
        duplicatesFound = 0;
        cacheHits = 0;
    }

    public Map<String, Integer> getCacheInfo() {
        Map<String, Integer> info = new HashMap<>();
        info.put("basicHashCacheSize", basicHashCache.size());
        info.put("structureGroupCacheSize", structureGroupCache.size());
        info.put("signatureCacheSize", signatureCache.size());
        info.put("canonicalFormCacheSize", canonicalFormCache.size());
        return info;
    }
}
