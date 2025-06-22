package QACMain;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NavigableMap;
import java.util.Set;
import java.util.TreeMap;
import java.util.Vector;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import model.DFS;
import model.DFSCode;
import model.Edge;
import model.Graph;
import model.History;
import model.PDFS;
import model.Projected;
import model.Vertex;

public class TEDSProcessor {

    private final ArrayList<Graph> TRANS;
    public DFSCode DFS_CODE;
    private final DFSCode DFS_CODE_IS_MIN;
    private final Graph GRAPH_IS_MIN;

    private long ID;
    private boolean directed;
    private FileWriter os;
    private Arguments arg;

    private NavigableMap<Integer, NavigableMap<Integer, Integer>> singleVertex;
    private NavigableMap<Integer, Integer> singleVertexLabel;

    private static int recursionCount = 0;
    private static long startTime = 0;
    private static final int PROGRESS_INTERVAL = 10000;

    private GraphIndexManager indexManager;
    private PatternReporter reporter;
    private GraphExtensionManager extensionManager;

    private static Graph queryGraphReference = null;

    public static Graph getQueryGraphReference() {
        return queryGraphReference;
    }

    private long totalProjectTime = 0;

    private ExecutorService executorService;
    private final int maxThreads = Runtime.getRuntime().availableProcessors();

    private List<ArrayList<Edge>> edgeListPool = new ArrayList<>();

    private long cacheHits = 0;
    private long cacheMisses = 0;

    public TEDSProcessor() {
        TRANS = new ArrayList<>();
        DFS_CODE = new DFSCode();
        DFS_CODE_IS_MIN = new DFSCode();
        GRAPH_IS_MIN = new Graph();
        singleVertex = new TreeMap<>();
        singleVertexLabel = new TreeMap<>();

        executorService = Executors.newFixedThreadPool(maxThreads);

        for (int i = 0; i < 100; i++) {
            edgeListPool.add(new ArrayList<Edge>());
        }
    }

    void run(
        FileReader reader,
        FileWriter writers,
        Arguments arguments,
        FileReader queryReader
    ) throws IOException {
        this.arg = arguments;

        try {
            indexManager = new GraphIndexManager(TRANS, arguments);
            reporter = new PatternReporter(TRANS, arguments, indexManager);
            reporter.setFileWriter(writers);
            extensionManager = new GraphExtensionManager(TRANS, arguments);

            try (
                FileWriter resultWriter = new FileWriter(
                    arguments.getResultPath(),
                    false
                )
            ) {} catch (IOException e) {
                System.err.println(
                    "Error clearing result file: " + e.getMessage()
                );
            }

            os = writers;
            ID = 0;
            directed = false;
            readMultipleGraphs(reader, arguments.gl);
            if (TRANS.isEmpty()) {
                System.out.println("No valid graph found. Exiting.");
                return;
            }
            read(queryReader);
            Long Time1 = System.currentTimeMillis();
            if (
                arg.hasInitialPatternGenerator && !arg.strategy.equals("greedy")
            ) {
                InitialPatternGenerator();
            }
            Long Time2 = System.currentTimeMillis();

            runIntern();

            Long Time3 = System.currentTimeMillis();
            System.out.println(
                "Swapping Time(s): " + ((Time3 - Time2) * 1.0) / 1000
            );

            if (!arg.strategy.equals("greedy")) {
                List<Graph> sortedGraphs = new ArrayList<>(
                    reporter.getAllGraphs()
                );

                List<Graph> uniqueGraphs =
                    IsomorphismRemover.removeIsomorphicGraphsPreservingOrder(
                        sortedGraphs,
                        false
                    );

                int targetCount = arg.numberofpatterns;

                if (uniqueGraphs.size() < targetCount) {
                    uniqueGraphs = supplementFromTED(uniqueGraphs, targetCount);
                }

                Collections.sort(uniqueGraphs, (g1, g2) -> {
                    int nodeDiff = g1.size() - g2.size();
                    if (nodeDiff != 0) {
                        return nodeDiff;
                    }

                    return g1.getEdgeSize() - g2.getEdgeSize();
                });

                if (uniqueGraphs.size() > targetCount) {
                    uniqueGraphs = uniqueGraphs.subList(0, targetCount);
                }

                int count = 0;
                for (Graph g : uniqueGraphs) {
                    reporter.newreport(g, count++);
                }

                System.out.println(
                    "TopK (after dedup), covered edges: " +
                    indexManager.getAllCoveredEdges().size()
                );
                int totalegdes = 0;
                for (int i = 0; i < TRANS.size(); i++) {
                    totalegdes += TRANS.get(i).getEdgeSize();
                }
                System.out.println("totalegdes : " + totalegdes);
                System.out.println(
                    "Coverage rate : " +
                    (indexManager.getAllCoveredEdges().size() * 1.0) /
                    totalegdes
                );
            } else {
                handleGreedyStrategy();
            }

            printFinalPerformanceAnalysis();
        } finally {
            shutdown();
        }
    }

    private void printFinalPerformanceAnalysis() {
        System.out.printf(
            "Processing completed: %.1fs | Memory: %.0fMB\n",
            totalProjectTime / 1_000_000_000.0,
            (Runtime.getRuntime().totalMemory() -
                Runtime.getRuntime().freeMemory()) /
            1024.0 /
            1024.0
        );
    }

    private void handleGreedyStrategy() throws IOException {
        Set<Integer> coverededges_curall = new HashSet<Integer>();
        Set<Long> selectPatternIndex = new HashSet<Long>();
        int tempcount = 0;

        while (tempcount < arg.numberofpatterns) {
            long maxid = -1;
            int maxgain = -1;
            for (int k = 0; k < reporter.getAllGraphs().size(); k++) {
                long id = (long) k;
                if (selectPatternIndex.contains(id)) continue;
                int gain = 0;
                Set<Integer> converages = indexManager
                    .getCoveredEdges_patterns()
                    .get(id);
                for (Integer a : converages) {
                    if (!coverededges_curall.contains(a)) gain++;
                }
                if (gain > maxgain) {
                    maxgain = gain;
                    maxid = id;
                }
            }

            if (maxgain < 0 || maxid < 0) return;

            selectPatternIndex.add(maxid);
            coverededges_curall.addAll(
                indexManager.getCoveredEdges_patterns().get(maxid)
            );
            tempcount++;
        }

        List<Graph> selectedGraphs = new ArrayList<>();
        for (long i : selectPatternIndex) {
            Graph g = reporter.getAllGraphs().get((int) i);

            if (
                g != null &&
                !(arg.maxNodeNum >= arg.minNodeNum &&
                    g.size() > arg.maxNodeNum) &&
                !(arg.minNodeNum > 0 && g.size() < arg.minNodeNum)
            ) {
                selectedGraphs.add(g);
            }
        }

        Collections.sort(selectedGraphs, (g1, g2) -> {
            int nodeDiff = g1.size() - g2.size();
            if (nodeDiff != 0) {
                return nodeDiff;
            }

            return g1.getEdgeSize() - g2.getEdgeSize();
        });

        int countofpatterns = 0;
        for (Graph g : selectedGraphs) {
            reporter.newreport(g, countofpatterns++);
        }

        int countofcoverededges = 0;
        Set<Integer> temp = new HashSet<Integer>();
        for (long i : selectPatternIndex) {
            temp.addAll(indexManager.getCoveredEdges_patterns().get(i));
        }
        countofcoverededges = temp.size();

        System.out.println("Greedy, covered edges: " + countofcoverededges);
        int totalegdes = 0;
        for (int i = 0; i < TRANS.size(); i++) {
            totalegdes += TRANS.get(i).getEdgeSize();
        }
        System.out.println("Total edges: " + totalegdes);
        System.out.println(
            "Coverage rate: " + (countofcoverededges * 1.0) / totalegdes
        );
    }

    private void read(FileReader is) throws IOException {
        int count = 0;
        BufferedReader read = new BufferedReader(is);
        while (true) {
            Graph g = new Graph(directed);
            read = g.read(read);
            if (g.isEmpty()) break;
            TRANS.add(g);
            count++;

            if (count == arg.numberofgraphs) break;
        }
        read.close();
    }

    private void InitialPatternGenerator() throws IOException {
        ArrayList<Edge> edges = new ArrayList<>();
        NavigableMap<
            Integer,
            NavigableMap<Integer, NavigableMap<Integer, Projected>>
        > root = new TreeMap<>();
        for (int id = 0; id < TRANS.size(); ++id) {
            Graph g = TRANS.get(id);
            for (int from = 0; from < g.size(); ++from) {
                if (Misc.getForwardRoot(g, g.get(from), edges)) {
                    for (Edge it : edges) {
                        int key_1 = g.get(from).label;
                        NavigableMap<
                            Integer,
                            NavigableMap<Integer, Projected>
                        > root_1 = root.computeIfAbsent(key_1, k ->
                            new TreeMap<>()
                        );
                        int key_2 = it.eLabel;
                        NavigableMap<Integer, Projected> root_2 =
                            root_1.computeIfAbsent(key_2, k -> new TreeMap<>());
                        int key_3 = g.get(it.to).label;
                        Projected root_3 = root_2.get(key_3);
                        if (root_3 == null) {
                            root_3 = new Projected();
                            root_2.put(key_3, root_3);
                        }
                        root_3.push(id, it, null);
                    }
                }
            }
        }
        for (Entry<
            Integer,
            NavigableMap<Integer, NavigableMap<Integer, Projected>>
        > fromLabel : root.entrySet()) {
            for (Entry<
                Integer,
                NavigableMap<Integer, Projected>
            > eLabel : fromLabel.getValue().entrySet()) {
                for (Entry<Integer, Projected> toLabel : eLabel
                    .getValue()
                    .entrySet()) {
                    DFS_CODE.push(
                        0,
                        1,
                        fromLabel.getKey(),
                        eLabel.getKey(),
                        toLabel.getKey()
                    );
                    project_Initial(toLabel.getValue());
                    DFS_CODE.pop();
                }
            }
        }
        System.out.println(
            "After initial swapping, covered edges: " +
            indexManager.getAllCoveredEdges().size()
        );
        int totalegdes = 0;
        for (int i = 0; i < TRANS.size(); i++) {
            totalegdes += TRANS.get(i).getEdgeSize();
        }
        System.out.println("Total edges: " + totalegdes);
        System.out.println(
            "Coverage rate: " +
            (indexManager.getAllCoveredEdges().size() * 1.0) / totalegdes
        );
        System.out.println(
            "Number of covered: " + indexManager.getNumberofcovered()
        );
    }

    private void runIntern() throws IOException {
        if (arg.minNodeNum <= 1) {
            /*
             * Do single node handling, as the normal gSpan DFS code based
             * processing cannot find sub-graphs of size |sub-g|==1. Hence, we
             * find frequent node labels explicitly.
             */
            for (int id = 0; id < TRANS.size(); ++id) {
                for (int nid = 0; nid < TRANS.get(id).size(); ++nid) {
                    int key = TRANS.get(id).get(nid).label;

                    singleVertex.computeIfAbsent(id, k -> new TreeMap<>());
                    if (singleVertex.get(id).get(key) == null) {
                        singleVertexLabel.put(
                            key,
                            Common.getValue(singleVertexLabel.get(key)) + 1
                        );
                    }
                    singleVertex
                        .get(id)
                        .put(
                            key,
                            Common.getValue(singleVertex.get(id).get(key)) + 1
                        );
                }
            }
        }
        /*
         * All minimum support node labels are frequent 'sub-graphs'.
         * singleVertexLabel[nodeLabel] gives the number of graphs it appears in.
         */
        for (Entry<Integer, Integer> it : singleVertexLabel.entrySet()) {
            if (it.getValue() < arg.minSup) continue;

            int frequent_label = it.getKey();

            Graph g = new Graph(directed);
            Vertex v = new Vertex();
            v.label = frequent_label;
            g.add(v);

            Vector<Integer> counts = new Vector<>();
            counts.setSize(TRANS.size());
            for (Entry<
                Integer,
                NavigableMap<Integer, Integer>
            > it2 : singleVertex.entrySet()) {
                counts.set(it2.getKey(), it2.getValue().get(frequent_label));
            }

            NavigableMap<Integer, Integer> gyCounts = new TreeMap<>();
            for (int n = 0; n < counts.size(); ++n) gyCounts.put(
                n,
                counts.get(n)
            );

            reporter.reportSingle(g, gyCounts);
        }

        ArrayList<Edge> edges = new ArrayList<>();

        NavigableMap<
            Integer,
            NavigableMap<Integer, NavigableMap<Integer, Projected>>
        > root = new TreeMap<>();

        for (int id = 0; id < TRANS.size(); ++id) {
            Graph g = TRANS.get(id);
            for (int from = 0; from < g.size(); ++from) {
                if (Misc.getForwardRoot(g, g.get(from), edges)) {
                    for (Edge it : edges) {
                        int key_1 = g.get(from).label;
                        NavigableMap<
                            Integer,
                            NavigableMap<Integer, Projected>
                        > root_1 = root.computeIfAbsent(key_1, k ->
                            new TreeMap<>()
                        );
                        int key_2 = it.eLabel;
                        NavigableMap<Integer, Projected> root_2 =
                            root_1.computeIfAbsent(key_2, k -> new TreeMap<>());
                        int key_3 = g.get(it.to).label;
                        Projected root_3 = root_2.get(key_3);
                        if (root_3 == null) {
                            root_3 = new Projected();
                            root_2.put(key_3, root_3);
                        }

                        root_3.push(0, it, null);
                    }
                }
            }
        }

        Graph q = TRANS.get(TRANS.size() - 1);

        queryGraphReference = q;

        Map<Integer, Integer> queryLabels = new HashMap<>();
        for (int i = 0; i < q.size(); i++) {
            queryLabels.put(i, q.get(i).label);
        }
        DFSCode.setQueryNodeLabels(queryLabels);

        DFS_CODE.clear();

        try {
            BufferedReader reader = new BufferedReader(
                new FileReader(arg.getQueryFile())
            );
            String line = reader.readLine();

            if (line != null && line.contains("<") && line.contains(">")) {
                String[] dfsEdges = line.split(">");

                Set<Integer> seenVertices = new HashSet<>();

                for (String dfsEdge : dfsEdges) {
                    if (dfsEdge.isEmpty()) continue;

                    if (dfsEdge.startsWith("<")) {
                        dfsEdge = dfsEdge.substring(1);
                    }

                    String[] parts = dfsEdge.split(",");
                    if (parts.length == 5) {
                        int from = Integer.parseInt(parts[0]);
                        int to = Integer.parseInt(parts[1]);
                        int fromLabel = Integer.parseInt(parts[2]);
                        int edgeLabel = Integer.parseInt(parts[3]);
                        int toLabel = Integer.parseInt(parts[4]);

                        boolean fromSeen = seenVertices.contains(from);
                        boolean toSeen = seenVertices.contains(to);

                        seenVertices.add(from);
                        seenVertices.add(to);

                        DFS_CODE.push(
                            from,
                            to,
                            fromSeen ? -1 : fromLabel,
                            edgeLabel,
                            toSeen ? -1 : toLabel
                        );
                    }
                }
            } else {
                while ((line = reader.readLine()) != null) {
                    if (line.startsWith("e ")) {
                        break;
                    }
                }

                Set<Integer> seenVertices = new HashSet<>();

                while (line != null && line.startsWith("e ")) {
                    String[] parts = line.split(" ");
                    int from = Integer.parseInt(parts[1]);
                    int to = Integer.parseInt(parts[2]);
                    int eLabel = Integer.parseInt(parts[3]);

                    boolean fromSeen = seenVertices.contains(from);
                    boolean toSeen = seenVertices.contains(to);

                    seenVertices.add(from);
                    seenVertices.add(to);

                    DFS_CODE.push(
                        from,
                        to,
                        fromSeen ? -1 : q.get(from).label,
                        eLabel,
                        toSeen ? -1 : q.get(to).label
                    );

                    line = reader.readLine();
                }
            }

            reader.close();
        } catch (IOException e) {
            System.err.println(
                "Error reading DFS code from output.txt: " + e.getMessage()
            );
            throw e;
        }

        int totalMatches = getTotalMatchCount();

        if (totalMatches < 10) {
            performVF2ExtensionFromQuery(q);
        }

        Projected qProjected = collectProjectedForQ(q, root);

        if (!qProjected.isEmpty()) {
            project(qProjected);
        }
    }

    private Projected collectProjectedForQ(
        Graph q,
        NavigableMap<
            Integer,
            NavigableMap<Integer, NavigableMap<Integer, Projected>>
        > root
    ) {
        Projected qProjected = new Projected();

        if (DFS_CODE.isEmpty()) {
            return qProjected;
        }

        for (int graphIndex = 0; graphIndex < TRANS.size() - 1; graphIndex++) {
            try {
                BufferedReader reader = new BufferedReader(
                    new FileReader(arg.graphgPath)
                );
                String line;
                int currentLine = 0;

                reader.readLine();

                while (
                    (line = reader.readLine()) != null &&
                    currentLine <= graphIndex
                ) {
                    if (currentLine == graphIndex) {
                        int startBracket = line.indexOf("[");
                        int endBracket = line.indexOf("]");
                        if (startBracket != -1 && endBracket != -1) {
                            String[] numbers = line
                                .substring(startBracket + 1, endBracket)
                                .split(", ");
                            int[] targetNodes = new int[numbers.length];
                            for (int i = 0; i < numbers.length; i++) {
                                targetNodes[i] = Integer.parseInt(numbers[i]);
                            }

                            Graph G = TRANS.get(graphIndex);

                            PDFS prevMatch = null;
                            for (int i = 0; i < DFS_CODE.size(); i++) {
                                DFS dfsEdge = DFS_CODE.get(i);

                                int fromNode = targetNodes[dfsEdge.from];
                                int toNode = targetNodes[dfsEdge.to];
                                int edgeLabel = dfsEdge.eLabel;

                                Edge g0Edge = null;
                                for (Edge e : G.get(fromNode).edge) {
                                    if (
                                        e.to == toNode && e.eLabel == edgeLabel
                                    ) {
                                        g0Edge = e;
                                        break;
                                    }
                                }

                                if (g0Edge != null) {
                                    Edge edge = new Edge();
                                    edge.from = fromNode;
                                    edge.to = toNode;
                                    edge.eLabel = edgeLabel;
                                    edge.id = g0Edge.id;

                                    PDFS currentMatch = new PDFS(
                                        graphIndex,
                                        edge,
                                        prevMatch
                                    );

                                    if (i == DFS_CODE.size() - 1) {
                                        qProjected.push(
                                            graphIndex,
                                            edge,
                                            prevMatch
                                        );
                                    }
                                    prevMatch = currentMatch;
                                } else {
                                    break;
                                }
                            }
                        }
                        break;
                    }
                    currentLine++;
                }
                reader.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        return qProjected;
    }

    private void project(Projected projected) throws IOException {
        long methodStartTime = System.nanoTime();

        recursionCount++;

        try {
            if (recursionCount % (PROGRESS_INTERVAL * 10) == 0) {
                System.out.printf(
                    "Processing: %d patterns processed\n",
                    recursionCount
                );
            }

            projectCompletelyNonRecursive(projected);
        } finally {
            long methodTime = System.nanoTime() - methodStartTime;
            totalProjectTime += methodTime;
        }
    }

    private static class IndependentTask {

        final DFSCode dfsCode;
        final Projected projected;
        final int depth;
        final Graph localGraphIsMin;
        final DFSCode localDfsCodeIsMin;

        IndependentTask(DFSCode dfsCode, Projected projected, int depth) {
            this.dfsCode = cloneStaticDfsCode(dfsCode);
            this.projected = projected;
            this.depth = depth;
            this.localGraphIsMin = new Graph(false);
            this.localDfsCodeIsMin = new DFSCode();
        }

        private static DFSCode cloneStaticDfsCode(DFSCode original) {
            DFSCode clone = new DFSCode();
            for (int i = 0; i < original.size(); i++) {
                DFS dfs = original.get(i);
                clone.push(
                    dfs.from,
                    dfs.to,
                    dfs.fromLabel,
                    dfs.eLabel,
                    dfs.toLabel
                );
            }
            return clone;
        }
    }

    private void projectCompletelyNonRecursive(Projected initialProjected)
        throws IOException {
        BlockingQueue<IndependentTask> currentTasks =
            new LinkedBlockingQueue<>();
        BlockingQueue<IndependentTask> nextTasks = new LinkedBlockingQueue<>();

        currentTasks.offer(
            new IndependentTask(
                DFS_CODE,
                initialProjected,
                DFS_CODE.countNode()
            )
        );

        int currentDepth = DFS_CODE.countNode();
        int totalProcessed = 0;

        long maxSearchDepth = Math.min(arg.maxNodeNum + 5, 25);

        long startTime = System.currentTimeMillis();
        long maxRunTime = 10 * 60 * 1000;
        long maxTotalTasks = 500000L;

        System.out.print("Search depth: ");

        while (!currentTasks.isEmpty() && currentDepth <= maxSearchDepth) {
            if (System.currentTimeMillis() - startTime > maxRunTime) {
                break;
            }

            if (totalProcessed > maxTotalTasks) {
                break;
            }

            int currentTaskCount = currentTasks.size();

            long maxTasksPerDepth = 1000000L;
            if (currentTaskCount > maxTasksPerDepth) {
                List<IndependentTask> filteredTasks = filterMostPromisingTasks(
                    currentTasks,
                    (int) (maxTasksPerDepth * 0.8)
                );
                currentTasks.clear();
                currentTasks.addAll(filteredTasks);
                currentTaskCount = currentTasks.size();
            }

            System.out.print(currentDepth + " ");

            List<IndependentTask> batchTasks = new ArrayList<>();
            currentTasks.drainTo(batchTasks);

            int batchSize = Math.max(
                1,
                Math.min(batchTasks.size() / (maxThreads * 2), 1000)
            );
            AtomicInteger completedTasks = new AtomicInteger(0);

            List<CompletableFuture<List<IndependentTask>>> futures =
                new ArrayList<>();

            for (int i = 0; i < batchTasks.size(); i += batchSize) {
                int endIndex = Math.min(i + batchSize, batchTasks.size());
                List<IndependentTask> batch = batchTasks.subList(i, endIndex);

                CompletableFuture<List<IndependentTask>> future =
                    CompletableFuture.supplyAsync(
                        () -> {
                            List<IndependentTask> newTasks = new ArrayList<>();
                            for (IndependentTask task : batch) {
                                try {
                                    if (shouldPrune(task.projected, false)) {
                                        continue;
                                    }
                                    newTasks.addAll(
                                        processIndependentTask(task)
                                    );
                                } catch (Exception e) {}
                            }
                            return newTasks;
                        },
                        executorService
                    );

                futures.add(future);
            }

            for (CompletableFuture<List<IndependentTask>> future : futures) {
                try {
                    List<IndependentTask> newTasks = future.get();
                    nextTasks.addAll(newTasks);
                    totalProcessed += newTasks.size();
                } catch (Exception e) {}
            }

            currentTasks = nextTasks;
            nextTasks = new LinkedBlockingQueue<>();
            currentDepth++;
        }

        System.out.println();

        if (currentDepth < arg.maxNodeNum) {
            System.out.println(
                "Search depth " +
                currentDepth +
                " not reached target size " +
                arg.maxNodeNum +
                ", starting VF2 graph extension..."
            );

            if (currentTasks.isEmpty()) {
                System.out.println(
                    "Current task queue empty, getting extension base from reported patterns"
                );
                performGraphExtensionFromReported();
            } else {
                performGraphExtension(currentTasks);
            }
        }
    }

    private void performGraphExtension(
        BlockingQueue<IndependentTask> incompleteTasks
    ) {
        try {
            List<IndependentTask> tasksToExtend = new ArrayList<>();
            incompleteTasks.drainTo(tasksToExtend);

            System.out.println(
                "Starting graph extension, tasks to extend: " +
                tasksToExtend.size()
            );

            if (tasksToExtend.isEmpty()) {
                System.out.println("No tasks to extend");
                return;
            }

            int extensionCount = 0;

            for (IndependentTask task : tasksToExtend) {
                if (extensionCount >= arg.numberofpatterns) {
                    break;
                }

                System.out.println(
                    "Processing task: DFS code size=" + task.dfsCode.size()
                );

                List<DFSCode> expandedGraphs =
                    extensionManager.expandGraphToTargetSize(
                        task.dfsCode,
                        (int) arg.maxNodeNum
                    );

                System.out.println(
                    "Task generated " +
                    expandedGraphs.size() +
                    " extended graphs"
                );

                for (DFSCode expandedDfsCode : expandedGraphs) {
                    if (extensionCount >= arg.numberofpatterns) {
                        break;
                    }

                    try {
                        Projected dummyProjected = new Projected();

                        synchronized (reporter) {
                            reporter.reportwithCoveredEdges(
                                1,
                                dummyProjected,
                                expandedDfsCode
                            );
                            extensionCount++;
                        }
                    } catch (Exception e) {
                        System.err.println(
                            "❌ 报告扩展图时出错: " + e.getMessage()
                        );
                    }
                }
            }

            System.out.println(
                "VF2 graph extension completed, total generated: " +
                extensionCount +
                " extended graphs"
            );
        } catch (Exception e) {
            System.err.println("❌ 执行图扩展时出错: " + e.getMessage());
            e.printStackTrace();
        }
    }

    private int getTotalMatchCount() {
        if (arg.IDs != null) {
            return arg.IDs.size();
        }
        return 0;
    }

    private void performVF2ExtensionFromQuery(Graph queryGraph) {
        try {
            System.out.println(
                "Query graph info: size=" + queryGraph.size() + " nodes"
            );

            DFSCode queryDfsCode = convertGraphToDfsCode(queryGraph);
            if (queryDfsCode == null) {
                System.out.println("Query graph DFS code conversion failed");
                return;
            }

            System.out.println("Query DFS code size: " + queryDfsCode.size());

            List<DFSCode> expandedGraphs =
                extensionManager.expandGraphToTargetSize(
                    queryDfsCode,
                    (int) arg.maxNodeNum
                );

            System.out.println(
                "Generated " +
                expandedGraphs.size() +
                " extended graphs from query graph"
            );

            int extensionCount = 0;
            for (DFSCode expandedDfsCode : expandedGraphs) {
                if (extensionCount >= arg.numberofpatterns) {
                    System.out.println(
                        "Reached maximum recommendation limit: " +
                        arg.numberofpatterns
                    );
                    break;
                }

                try {
                    Projected dummyProjected = new Projected();

                    synchronized (reporter) {
                        System.out.println(
                            "Report query extension graph " +
                            (extensionCount + 1) +
                            ": nodes=" +
                            (expandedDfsCode != null
                                    ? expandedDfsCode.countNode()
                                    : "unknown")
                        );
                        reporter.reportwithCoveredEdges(
                            1,
                            dummyProjected,
                            expandedDfsCode
                        );
                        extensionCount++;
                    }
                } catch (Exception e) {
                    System.err.println(
                        "❌ 报告Query扩展图时出错: " + e.getMessage()
                    );
                }
            }

            System.out.println(
                "Query graph VF2 extension completed, total generated: " +
                extensionCount +
                " extended graphs"
            );
        } catch (Exception e) {
            System.err.println(
                "❌ 执行Query图VF2扩展时出错: " + e.getMessage()
            );
            e.printStackTrace();
        }
    }

    private void performGraphExtensionFromReported() {
        try {
            List<Graph> reportedGraphs = reporter.getAllGraphs();

            if (reportedGraphs.isEmpty()) {
                System.out.println(
                    "No reported patterns available for extension"
                );
                return;
            }

            System.out.println(
                "Starting graph extension from " +
                reportedGraphs.size() +
                " reported patterns"
            );

            int extensionCount = 0;

            int startIndex = Math.max(0, reportedGraphs.size() - 5);

            for (
                int i = startIndex;
                i < reportedGraphs.size() &&
                extensionCount < arg.numberofpatterns;
                i++
            ) {
                Graph baseGraph = reportedGraphs.get(i);

                DFSCode baseDfsCode = convertGraphToDfsCode(baseGraph);
                if (baseDfsCode == null) {
                    continue;
                }

                System.out.println(
                    "Base graph " +
                    (i + 1) +
                    ": size=" +
                    baseGraph.size() +
                    ", DFS size=" +
                    baseDfsCode.size()
                );

                List<DFSCode> expandedGraphs =
                    extensionManager.expandGraphToTargetSize(
                        baseDfsCode,
                        (int) arg.maxNodeNum
                    );

                System.out.println(
                    "Base graph generated " +
                    expandedGraphs.size() +
                    " extended graphs"
                );

                for (DFSCode expandedDfsCode : expandedGraphs) {
                    if (extensionCount >= arg.numberofpatterns) {
                        break;
                    }

                    try {
                        Projected dummyProjected = new Projected();

                        synchronized (reporter) {
                            reporter.reportwithCoveredEdges(
                                1,
                                dummyProjected,
                                expandedDfsCode
                            );
                            extensionCount++;
                        }
                    } catch (Exception e) {
                        System.err.println(
                            "❌ 报告扩展图时出错: " + e.getMessage()
                        );
                    }
                }
            }

            System.out.println(
                "Pattern-based VF2 graph extension completed, total generated: " +
                extensionCount +
                " extended graphs"
            );
        } catch (Exception e) {
            System.err.println(
                "❌ 执行基于已报告模式的图扩展时出错: " + e.getMessage()
            );
            e.printStackTrace();
        }
    }

    private DFSCode convertGraphToDfsCode(Graph graph) {
        DFSCode dfsCode = new DFSCode();
        Set<String> addedEdges = new HashSet<>();

        try {
            for (int i = 0; i < graph.size(); i++) {
                for (Edge edge : graph.get(i).edge) {
                    String edgeKey =
                        Math.min(edge.from, edge.to) +
                        "-" +
                        Math.max(edge.from, edge.to) +
                        "-" +
                        edge.eLabel;
                    if (!addedEdges.contains(edgeKey)) {
                        addedEdges.add(edgeKey);

                        int fromLabel = graph.get(edge.from).label;
                        int toLabel = graph.get(edge.to).label;

                        DFS dfs = new DFS();
                        dfs.from = edge.from;
                        dfs.to = edge.to;
                        dfs.fromLabel = fromLabel;
                        dfs.eLabel = edge.eLabel;
                        dfs.toLabel = toLabel;
                        dfsCode.add(dfs);
                    }
                }
            }
        } catch (Exception e) {
            System.err.println("❌ 图转换DFS代码失败: " + e.getMessage());
            return null;
        }

        return dfsCode;
    }

    private List<IndependentTask> filterMostPromisingTasks(
        BlockingQueue<IndependentTask> tasks,
        int maxTasks
    ) {
        List<IndependentTask> allTasks = new ArrayList<>();
        tasks.drainTo(allTasks);

        allTasks.sort((t1, t2) ->
            Integer.compare(t2.projected.size(), t1.projected.size())
        );

        return allTasks.subList(0, Math.min(maxTasks, allTasks.size()));
    }

    private List<IndependentTask> processIndependentTask(IndependentTask task)
        throws IOException {
        List<IndependentTask> newTasks = new ArrayList<>();

        try {
            int sup = support(task.projected);
            if (sup < arg.minSup) {
                return newTasks;
            }

            if (!isIndependentUnique(task)) {
                return newTasks;
            }

            synchronized (reporter) {
                try {
                    Boolean hasUpdated = reporter.reportwithCoveredEdges(
                        sup,
                        task.projected,
                        task.dfsCode
                    );
                } catch (Exception e) {}
            }

            if (
                arg.maxNodeNum >= arg.minNodeNum &&
                task.dfsCode.countNode() > arg.maxNodeNum
            ) {
                return newTasks;
            }

            newTasks.addAll(generateIndependentExtensions(task));
        } catch (Exception e) {}

        return newTasks;
    }

    private boolean isIndependentUnique(IndependentTask task) {
        try {
            if (task.dfsCode.size() <= 1) {
                return true;
            }

            return indexManager.isGraphUniqueRelaxed(task.dfsCode);
        } catch (Exception e) {
            return isBasicDfsValid(task.dfsCode);
        }
    }

    private boolean isBasicDfsValid(DFSCode dfsCode) {
        try {
            if (dfsCode.isEmpty()) return false;

            if (dfsCode.size() > 0) {
                DFS first = dfsCode.get(0);
                if (first.from != 0 || first.to != 1) {
                    return false;
                }
            }

            Set<Integer> seenVertices = new HashSet<>();
            seenVertices.add(0);
            seenVertices.add(1);

            for (int i = 1; i < dfsCode.size(); i++) {
                DFS dfs = dfsCode.get(i);

                if (
                    !seenVertices.contains(dfs.from) &&
                    !seenVertices.contains(dfs.to)
                ) {
                    return false;
                }

                seenVertices.add(dfs.from);
                seenVertices.add(dfs.to);
            }

            return true;
        } catch (Exception e) {
            return true;
        }
    }

    private List<IndependentTask> generateIndependentExtensions(
        IndependentTask task
    ) {
        List<IndependentTask> extensions = new ArrayList<>();

        try {
            ArrayList<Integer> rmPath = task.dfsCode.buildRMPath();
            if (rmPath.isEmpty()) return extensions;

            int minLabel = task.dfsCode.get(0).fromLabel;
            int maxToc = task.dfsCode.get(rmPath.get(0)).to;

            Map<String, Projected> forwardPureExtensions = new HashMap<>();
            Map<String, Projected> forwardRmPathExtensions = new HashMap<>();
            Map<String, Projected> backwardExtensions = new HashMap<>();

            Map<String, Projected> chainCycleExtensions = new HashMap<>();

            for (PDFS aProjected : task.projected) {
                int id = aProjected.id;
                if (id >= TRANS.size()) continue;

                try {
                    History history = new History(TRANS.get(id), aProjected);
                    ArrayList<Edge> edges = new ArrayList<>();

                    if (
                        Misc.getForwardPure(
                            TRANS.get(id),
                            history.get(rmPath.get(0)),
                            minLabel,
                            history,
                            edges
                        )
                    ) {
                        for (Edge edge : edges) {
                            String key =
                                maxToc +
                                "-" +
                                edge.eLabel +
                                "-" +
                                TRANS.get(id).get(edge.to).label;
                            forwardPureExtensions
                                .computeIfAbsent(key, k -> new Projected())
                                .push(id, edge, aProjected);
                        }
                    }

                    for (int i = history.size() - 1; i >= 0; --i) {
                        edges.clear();
                        if (
                            Misc.getForwardRmPath(
                                TRANS.get(id),
                                history.get(i),
                                minLabel,
                                history,
                                edges
                            )
                        ) {
                            for (Edge edge : edges) {
                                int fromVertex = task.dfsCode.get(i).from;
                                String key =
                                    fromVertex +
                                    "-" +
                                    edge.eLabel +
                                    "-" +
                                    TRANS.get(id).get(edge.to).label;
                                forwardRmPathExtensions
                                    .computeIfAbsent(key, k -> new Projected())
                                    .push(id, edge, aProjected);
                            }
                        }
                    }

                    for (int i = history.size() - 1; i >= 1; --i) {
                        for (int j = i - 1; j >= 0; --j) {
                            Edge e = Misc.getBackward(
                                TRANS.get(id),
                                history.get(i),
                                history.get(j),
                                history
                            );
                            if (e != null) {
                                String key =
                                    task.dfsCode.get(i).from +
                                    "-" +
                                    task.dfsCode.get(j).from +
                                    "-" +
                                    e.eLabel;
                                backwardExtensions
                                    .computeIfAbsent(key, k -> new Projected())
                                    .push(id, e, aProjected);
                            }

                            if (
                                isLongChainStructure(task.dfsCode) &&
                                task.dfsCode.size() >= 4
                            ) {
                                Edge relaxedE = Misc.getBackwardRelaxed(
                                    TRANS.get(id),
                                    history.get(i),
                                    history.get(j),
                                    history
                                );
                                if (
                                    relaxedE != null &&
                                    relaxedE != e &&
                                    !isEdgeAlreadyExists(task.dfsCode, relaxedE)
                                ) {
                                    String relaxedKey =
                                        task.dfsCode.get(i).from +
                                        "-" +
                                        task.dfsCode.get(j).from +
                                        "-" +
                                        relaxedE.eLabel +
                                        "-relaxed";
                                    backwardExtensions
                                        .computeIfAbsent(relaxedKey, k ->
                                            new Projected()
                                        )
                                        .push(id, relaxedE, aProjected);
                                }
                            }
                        }
                    }
                    /*
                    if (isLongChainStructure(task.dfsCode)) {
                        System.out.println("🔗 检测到长链状结构，执行增强成环扩展...");


                        if (task.dfsCode.size() >= 4 && task.dfsCode.size() <= 8) {
                            Edge firstToLastEdge = findDirectConnection(TRANS.get(id),
                                                                      history.get(0),
                                                                      history.get(rmPath.get(0)),
                                                                      history);
                            if (firstToLastEdge != null && isReasonableExtension(task.dfsCode, firstToLastEdge)) {
                                String key = "chain-head-tail-" + firstToLastEdge.eLabel;
                                chainCycleExtensions.computeIfAbsent(key, k -> new Projected()).push(id, firstToLastEdge, aProjected);

                            }
                        }


                        int skipConnections = 0;
                        for (int i = 0; i < history.size() - 2 && skipConnections < 2; i++) {
                            for (int j = i + 3; j < history.size() && skipConnections < 2; j++) {
                                Edge skipEdge = findDirectConnection(TRANS.get(id),
                                                                   history.get(i),
                                                                   history.get(j),
                                                                   history);
                                if (skipEdge != null && isReasonableExtension(task.dfsCode, skipEdge)) {
                                    String key = "chain-skip-" + i + "-" + j + "-" + skipEdge.eLabel;
                                    chainCycleExtensions.computeIfAbsent(key, k -> new Projected()).push(id, skipEdge, aProjected);

                                    skipConnections++;
                                }
                            }
                        }
                    }
                    */

                } catch (Exception e) {
                    continue;
                }
            }

            for (Map.Entry<
                String,
                Projected
            > entry : forwardPureExtensions.entrySet()) {
                String[] parts = entry.getKey().split("-");
                if (parts.length == 3) {
                    DFSCode newDfsCode = cloneDfsCode(task.dfsCode);
                    newDfsCode.push(
                        Integer.parseInt(parts[0]),
                        maxToc + 1,
                        -1,
                        Integer.parseInt(parts[1]),
                        Integer.parseInt(parts[2])
                    );
                    extensions.add(
                        new IndependentTask(
                            newDfsCode,
                            entry.getValue(),
                            task.depth + 1
                        )
                    );
                }
            }

            for (Map.Entry<
                String,
                Projected
            > entry : forwardRmPathExtensions.entrySet()) {
                String[] parts = entry.getKey().split("-");
                if (parts.length == 3) {
                    DFSCode newDfsCode = cloneDfsCode(task.dfsCode);
                    newDfsCode.push(
                        Integer.parseInt(parts[0]),
                        maxToc + 1,
                        -1,
                        Integer.parseInt(parts[1]),
                        Integer.parseInt(parts[2])
                    );
                    extensions.add(
                        new IndependentTask(
                            newDfsCode,
                            entry.getValue(),
                            task.depth + 1
                        )
                    );
                }
            }

            for (Map.Entry<
                String,
                Projected
            > entry : backwardExtensions.entrySet()) {
                String[] parts = entry.getKey().split("-");
                if (parts.length >= 3) {
                    DFSCode newDfsCode = cloneDfsCode(task.dfsCode);
                    if (parts.length >= 4 && parts[3].equals("back")) {
                        newDfsCode.push(
                            Integer.parseInt(parts[0]),
                            Integer.parseInt(parts[1]),
                            -1,
                            Integer.parseInt(parts[2]),
                            -1
                        );
                    } else if (
                        parts.length >= 4 && parts[3].equals("relaxed")
                    ) {
                        newDfsCode.push(
                            Integer.parseInt(parts[0]),
                            Integer.parseInt(parts[1]),
                            -1,
                            Integer.parseInt(parts[2]),
                            -1
                        );
                    } else if (
                        parts.length >= 5 &&
                        parts[3].equals("back") &&
                        parts[4].equals("relaxed")
                    ) {
                        newDfsCode.push(
                            Integer.parseInt(parts[0]),
                            Integer.parseInt(parts[1]),
                            -1,
                            Integer.parseInt(parts[2]),
                            -1
                        );
                    } else {
                        newDfsCode.push(
                            Integer.parseInt(parts[0]),
                            Integer.parseInt(parts[1]),
                            -1,
                            Integer.parseInt(parts[2]),
                            -1
                        );
                    }
                    extensions.add(
                        new IndependentTask(
                            newDfsCode,
                            entry.getValue(),
                            task.depth + 1
                        )
                    );
                }
            }

            for (Map.Entry<
                String,
                Projected
            > entry : chainCycleExtensions.entrySet()) {
                String[] parts = entry.getKey().split("-");
                try {
                    DFSCode newDfsCode = cloneDfsCode(task.dfsCode);

                    if (
                        parts[0].equals("chain") &&
                        parts[1].equals("head") &&
                        parts[2].equals("tail")
                    ) {
                        int edgeLabel = Integer.parseInt(parts[3]);
                        newDfsCode.push(maxToc, 0, -1, edgeLabel, -1);
                        extensions.add(
                            new IndependentTask(
                                newDfsCode,
                                entry.getValue(),
                                task.depth + 1
                            )
                        );
                    } else if (
                        parts[0].equals("chain") && parts[1].equals("skip")
                    ) {
                        int fromIdx = Integer.parseInt(parts[2]);
                        int toIdx = Integer.parseInt(parts[3]);
                        int edgeLabel = Integer.parseInt(parts[4]);

                        int fromVertex = getDfsVertexId(task.dfsCode, fromIdx);
                        int toVertex = getDfsVertexId(task.dfsCode, toIdx);

                        if (fromVertex != -1 && toVertex != -1) {
                            newDfsCode.push(
                                Math.max(fromVertex, toVertex),
                                Math.min(fromVertex, toVertex),
                                -1,
                                edgeLabel,
                                -1
                            );
                            extensions.add(
                                new IndependentTask(
                                    newDfsCode,
                                    entry.getValue(),
                                    task.depth + 1
                                )
                            );
                        }
                    }
                } catch (Exception e) {
                    System.err.println(
                        "  ❌ 成环扩展解析失败: " + entry.getKey()
                    );
                }
            }
        } catch (Exception e) {}

        return extensions;
    }

    private boolean isLongChainStructure(DFSCode dfsCode) {
        if (dfsCode.size() < 3) return false;

        int consecutiveEdges = 0;
        for (int i = 0; i < dfsCode.size() - 1; i++) {
            DFS current = dfsCode.get(i);
            DFS next = dfsCode.get(i + 1);

            if (current.to == next.from || current.from == next.to) {
                consecutiveEdges++;
            }
        }

        double chainRatio = (double) consecutiveEdges / (dfsCode.size() - 1);
        boolean isChain = chainRatio > 0.7;

        return isChain;
    }

    private Edge findDirectConnection(
        Graph graph,
        Edge node1,
        Edge node2,
        History history
    ) {
        if (node1 == null || node2 == null || node1.equals(node2)) return null;

        Edge result = null;

        result = Misc.getChainCycleConnection(
            graph,
            node1.to,
            node2.to,
            history
        );
        if (result != null) return result;

        result = Misc.getChainCycleConnection(
            graph,
            node1.from,
            node2.to,
            history
        );
        if (result != null) return result;

        result = Misc.getChainCycleConnection(
            graph,
            node1.to,
            node2.from,
            history
        );
        if (result != null) return result;

        result = Misc.getChainCycleConnection(
            graph,
            node1.from,
            node2.from,
            history
        );
        if (result != null) return result;

        result = Misc.getChainCycleConnection(
            graph,
            node2.to,
            node1.to,
            history
        );
        if (result != null) return result;

        result = Misc.getChainCycleConnection(
            graph,
            node2.from,
            node1.to,
            history
        );
        if (result != null) return result;

        result = Misc.getChainCycleConnection(
            graph,
            node2.to,
            node1.from,
            history
        );
        if (result != null) return result;

        result = Misc.getChainCycleConnection(
            graph,
            node2.from,
            node1.from,
            history
        );
        if (result != null) return result;

        return null;
    }

    private int getDfsVertexId(DFSCode dfsCode, int historyIndex) {
        if (historyIndex < 0 || historyIndex >= dfsCode.size()) return -1;

        DFS dfs = dfsCode.get(historyIndex);

        return dfs.to;
    }

    private boolean isEdgeAlreadyExists(DFSCode dfsCode, Edge newEdge) {
        if (newEdge == null || dfsCode == null) return false;

        for (int i = 0; i < dfsCode.size(); i++) {
            DFS dfs = dfsCode.get(i);

            if (
                (dfs.from == newEdge.from && dfs.to == newEdge.to) ||
                (dfs.from == newEdge.to && dfs.to == newEdge.from)
            ) {
                return true;
            }
        }

        return false;
    }

    private boolean isReasonableExtension(DFSCode dfsCode, Edge newEdge) {
        if (newEdge == null || dfsCode == null) return false;

        if (isEdgeAlreadyExists(dfsCode, newEdge)) {
            return false;
        }

        Map<Integer, Integer> nodeDegree = new HashMap<>();
        for (int i = 0; i < dfsCode.size(); i++) {
            DFS dfs = dfsCode.get(i);
            nodeDegree.put(dfs.from, nodeDegree.getOrDefault(dfs.from, 0) + 1);
            nodeDegree.put(dfs.to, nodeDegree.getOrDefault(dfs.to, 0) + 1);
        }

        int fromDegree = nodeDegree.getOrDefault(newEdge.from, 0) + 1;
        int toDegree = nodeDegree.getOrDefault(newEdge.to, 0) + 1;

        if (fromDegree > 5 || toDegree > 5) {
            return false;
        }

        return true;
    }

    private int support(Projected projected) {
        int oid = 0xffffffff;
        int size = 0;

        for (PDFS cur : projected) {
            if (oid != cur.id) {
                ++size;
            }
            oid = cur.id;
        }

        return size;
    }

    private boolean isMinProject(Projected projected) {
        ArrayList<Integer> rmPath = DFS_CODE_IS_MIN.buildRMPath();

        int minLabel = DFS_CODE_IS_MIN.get(0).fromLabel;

        int maxToc = DFS_CODE_IS_MIN.get(rmPath.get(0)).to;

        {
            NavigableMap<Integer, Projected> root = new TreeMap<>();
            boolean flg = false;
            int newTo = 0;

            for (int i = rmPath.size() - 1; !flg && i >= 1; --i) {
                for (PDFS cur : projected) {
                    History history = new History(GRAPH_IS_MIN, cur);
                    Edge e = Misc.getBackward(
                        GRAPH_IS_MIN,
                        history.get(rmPath.get(i)),
                        history.get(rmPath.get(0)),
                        history
                    );
                    if (e != null) {
                        int key_1 = e.eLabel;
                        Projected root_1 = root.get(key_1);
                        if (root_1 == null) {
                            root_1 = new Projected();
                            root.put(key_1, root_1);
                        }
                        root_1.push(0, e, cur);
                        newTo = DFS_CODE_IS_MIN.get(rmPath.get(i)).from;
                        flg = true;
                    }
                }
            }

            if (flg) {
                Entry<Integer, Projected> eLabel = root.firstEntry();
                DFS_CODE_IS_MIN.push(maxToc, newTo, -1, eLabel.getKey(), -1);
                return isMinProject(eLabel.getValue());
            }
        }

        {
            boolean flg = false;
            int newFrom = 0;
            NavigableMap<Integer, NavigableMap<Integer, Projected>> root =
                new TreeMap<>();
            ArrayList<Edge> edges = new ArrayList<>();

            for (PDFS cur : projected) {
                History history = new History(GRAPH_IS_MIN, cur);
                if (
                    Misc.getForwardPure(
                        GRAPH_IS_MIN,
                        history.get(rmPath.get(0)),
                        minLabel,
                        history,
                        edges
                    )
                ) {
                    flg = true;
                    newFrom = maxToc;
                    for (Edge it : edges) {
                        int key_1 = it.eLabel;
                        NavigableMap<Integer, Projected> root_1 =
                            root.computeIfAbsent(key_1, k -> new TreeMap<>());
                        int key_2 = GRAPH_IS_MIN.get(it.to).label;
                        Projected root_2 = root_1.get(key_2);
                        if (root_2 == null) {
                            root_2 = new Projected();
                            root_1.put(key_2, root_2);
                        }
                        root_2.push(0, it, cur);
                    }
                }
            }

            for (int i = 0; !flg && i < rmPath.size(); ++i) {
                for (PDFS cur : projected) {
                    History history = new History(GRAPH_IS_MIN, cur);
                    if (
                        Misc.getForwardRmPath(
                            GRAPH_IS_MIN,
                            history.get(rmPath.get(i)),
                            minLabel,
                            history,
                            edges
                        )
                    ) {
                        flg = true;
                        newFrom = DFS_CODE_IS_MIN.get(rmPath.get(i)).from;
                        for (Edge it : edges) {
                            int key_1 = it.eLabel;
                            NavigableMap<Integer, Projected> root_1 =
                                root.computeIfAbsent(key_1, k ->
                                    new TreeMap<>()
                                );
                            int key_2 = GRAPH_IS_MIN.get(it.to).label;
                            Projected root_2 = root_1.get(key_2);
                            if (root_2 == null) {
                                root_2 = new Projected();
                                root_1.put(key_2, root_2);
                            }
                            root_2.push(0, it, cur);
                        }
                    }
                }
            }

            if (flg) {
                Entry<Integer, NavigableMap<Integer, Projected>> eLabel =
                    root.firstEntry();
                Entry<Integer, Projected> toLabel = eLabel
                    .getValue()
                    .firstEntry();
                DFS_CODE_IS_MIN.push(
                    newFrom,
                    maxToc + 1,
                    -1,
                    eLabel.getKey(),
                    toLabel.getKey()
                );
                if (
                    DFS_CODE.get(DFS_CODE_IS_MIN.size() - 1).notEqual(
                        DFS_CODE_IS_MIN.get(DFS_CODE_IS_MIN.size() - 1)
                    )
                ) return false;
                return isMinProject(toLabel.getValue());
            }
        }

        return true;
    }

    private void readMultipleGraphs(FileReader is, long[] gl)
        throws IOException {
        BufferedReader read = new BufferedReader(is);
        int i = 0;
        while (true) {
            i++;
            Graph g = new Graph(directed);
            Graph.ReadResult result = g.readspnum(read, gl);
            read = result.reader;
            if (g.isEmpty()) break;
            if (result.valid) {
                TRANS.add(g);
            }
        }
        read.close();
    }

    private boolean ToisMin() {
        if (DFS_CODE.size() == 1) return (true);

        DFS_CODE.toGraph(GRAPH_IS_MIN);
        DFS_CODE_IS_MIN.clear();

        NavigableMap<
            Integer,
            NavigableMap<Integer, NavigableMap<Integer, Projected>>
        > root = new TreeMap<>();
        ArrayList<Edge> edges = new ArrayList<>();

        for (int from = 0; from < GRAPH_IS_MIN.size(); ++from) if (
            Misc.getForwardRoot(GRAPH_IS_MIN, GRAPH_IS_MIN.get(from), edges)
        ) for (Edge it : edges) {
            int key_1 = GRAPH_IS_MIN.get(from).label;
            NavigableMap<Integer, NavigableMap<Integer, Projected>> root_1 =
                root.computeIfAbsent(key_1, k -> new TreeMap<>());
            int key_2 = it.eLabel;
            NavigableMap<Integer, Projected> root_2 = root_1.computeIfAbsent(
                key_2,
                k -> new TreeMap<>()
            );
            int key_3 = GRAPH_IS_MIN.get(it.to).label;
            Projected root_3 = root_2.get(key_3);
            if (root_3 == null) {
                root_3 = new Projected();
                root_2.put(key_3, root_3);
            }

            root_3.push(0, it, null);
        }

        Entry<
            Integer,
            NavigableMap<Integer, NavigableMap<Integer, Projected>>
        > fromLabel = root.firstEntry();
        Entry<Integer, NavigableMap<Integer, Projected>> eLabel = fromLabel
            .getValue()
            .firstEntry();
        Entry<Integer, Projected> toLabel = eLabel.getValue().firstEntry();

        DFS_CODE_IS_MIN.push(
            0,
            1,
            fromLabel.getKey(),
            eLabel.getKey(),
            toLabel.getKey()
        );

        return isMinGenerate(toLabel.getValue());
    }

    private boolean isMinGenerate(Projected projected) {
        ArrayList<Integer> rmPath = DFS_CODE_IS_MIN.buildRMPath();

        int minLabel = DFS_CODE_IS_MIN.get(0).fromLabel;

        int maxToc = DFS_CODE_IS_MIN.get(rmPath.get(0)).to;

        {
            NavigableMap<Integer, Projected> root = new TreeMap<>();
            boolean flg = false;
            int newTo = 0;

            for (int i = rmPath.size() - 1; !flg && i >= 1; --i) {
                for (PDFS cur : projected) {
                    History history = new History(GRAPH_IS_MIN, cur);
                    Edge e = Misc.getBackward(
                        GRAPH_IS_MIN,
                        history.get(rmPath.get(i)),
                        history.get(rmPath.get(0)),
                        history
                    );
                    if (e != null) {
                        int key_1 = e.eLabel;
                        Projected root_1 = root.get(key_1);
                        if (root_1 == null) {
                            root_1 = new Projected();
                            root.put(key_1, root_1);
                        }
                        root_1.push(0, e, cur);
                        newTo = DFS_CODE_IS_MIN.get(rmPath.get(i)).from;
                        flg = true;
                    }
                }
            }

            if (flg) {
                Entry<Integer, Projected> eLabel = root.firstEntry();
                DFS_CODE_IS_MIN.push(maxToc, newTo, -1, eLabel.getKey(), -1);
                return isMinGenerate(eLabel.getValue());
            }
        }

        {
            boolean flg = false;
            int newFrom = 0;
            NavigableMap<Integer, NavigableMap<Integer, Projected>> root =
                new TreeMap<>();
            ArrayList<Edge> edges = new ArrayList<>();

            for (PDFS cur : projected) {
                History history = new History(GRAPH_IS_MIN, cur);
                if (
                    Misc.getForwardPure(
                        GRAPH_IS_MIN,
                        history.get(rmPath.get(0)),
                        minLabel,
                        history,
                        edges
                    )
                ) {
                    flg = true;
                    newFrom = maxToc;
                    for (Edge it : edges) {
                        int key_1 = it.eLabel;
                        NavigableMap<Integer, Projected> root_1 =
                            root.computeIfAbsent(key_1, k -> new TreeMap<>());
                        int key_2 = GRAPH_IS_MIN.get(it.to).label;
                        Projected root_2 = root_1.get(key_2);
                        if (root_2 == null) {
                            root_2 = new Projected();
                            root_1.put(key_2, root_2);
                        }
                        root_2.push(0, it, cur);
                    }
                }
            }

            for (int i = 0; !flg && i < rmPath.size(); ++i) {
                for (PDFS cur : projected) {
                    History history = new History(GRAPH_IS_MIN, cur);
                    if (
                        Misc.getForwardRmPath(
                            GRAPH_IS_MIN,
                            history.get(rmPath.get(i)),
                            minLabel,
                            history,
                            edges
                        )
                    ) {
                        flg = true;
                        newFrom = DFS_CODE_IS_MIN.get(rmPath.get(i)).from;
                        for (Edge it : edges) {
                            int key_1 = it.eLabel;
                            NavigableMap<Integer, Projected> root_1 =
                                root.computeIfAbsent(key_1, k ->
                                    new TreeMap<>()
                                );
                            int key_2 = GRAPH_IS_MIN.get(it.to).label;
                            Projected root_2 = root_1.get(key_2);
                            if (root_2 == null) {
                                root_2 = new Projected();
                                root_1.put(key_2, root_2);
                            }
                            root_2.push(0, it, cur);
                        }
                    }
                }
            }

            if (flg) {
                Entry<Integer, NavigableMap<Integer, Projected>> eLabel =
                    root.firstEntry();
                Entry<Integer, Projected> toLabel = eLabel
                    .getValue()
                    .firstEntry();
                DFS_CODE_IS_MIN.push(
                    newFrom,
                    maxToc + 1,
                    -1,
                    eLabel.getKey(),
                    toLabel.getKey()
                );
                if (
                    DFS_CODE.get(DFS_CODE_IS_MIN.size() - 1).notEqual(
                        DFS_CODE_IS_MIN.get(DFS_CODE_IS_MIN.size() - 1)
                    )
                ) return false;
                return isMinGenerate(toLabel.getValue());
            }
        }

        return true;
    }

    private boolean isUnique() {
        ToisMin();

        return indexManager.isGraphUniqueAdaptive(DFS_CODE_IS_MIN);
    }

    private ArrayList<Edge> getEdgeList() {
        synchronized (edgeListPool) {
            if (!edgeListPool.isEmpty()) {
                ArrayList<Edge> list = edgeListPool.remove(
                    edgeListPool.size() - 1
                );
                list.clear();
                return list;
            }
        }
        return new ArrayList<>();
    }

    private void recycleEdgeList(ArrayList<Edge> list) {
        if (list != null && edgeListPool.size() < 200) {
            synchronized (edgeListPool) {
                edgeListPool.add(list);
            }
        }
    }

    private boolean shouldPrune(Projected projected, boolean hasupdated) {
        if (
            arg.hasPRM &&
            reporter.getAllGraphs().size() >= arg.numberofpatterns * 2
        ) {
            if (!hasupdated && projected.size() <= 1) {
                return true;
            }
        }

        int currentDepth = DFS_CODE.countNode();
        if (currentDepth > arg.maxNodeNum + 5) {
            return true;
        }

        if (projected.size() < Math.max(1, arg.minSup - 1)) {
            return true;
        }

        if (currentDepth > 5) {
            double coverageRatio = (double) projected.size() / TRANS.size();
            double minCoverageThreshold = Math.max(
                0.02,
                1.0 / (arg.numberofpatterns * 3)
            );
            if (coverageRatio < minCoverageThreshold) {
                return true;
            }
        }

        if (totalProjectTime > 300_000_000_000L) {
            if (projected.size() <= 1) {
                return true;
            }
        }

        Runtime runtime = Runtime.getRuntime();
        long usedMemory = runtime.totalMemory() - runtime.freeMemory();
        long maxMemory = runtime.maxMemory();
        if (usedMemory > maxMemory * 0.9) {
            if (projected.size() <= 1) {
                return true;
            }
        }

        return false;
    }

    public void shutdown() {
        if (executorService != null) {
            executorService.shutdown();
            try {
                if (!executorService.awaitTermination(60, TimeUnit.SECONDS)) {
                    executorService.shutdownNow();
                }
            } catch (InterruptedException e) {
                executorService.shutdownNow();
                Thread.currentThread().interrupt();
            }
        }

        edgeListPool.clear();
    }

    private DFSCode cloneDfsCode(DFSCode original) {
        DFSCode clone = new DFSCode();
        for (int i = 0; i < original.size(); i++) {
            DFS dfs = original.get(i);
            clone.push(
                dfs.from,
                dfs.to,
                dfs.fromLabel,
                dfs.eLabel,
                dfs.toLabel
            );
        }
        return clone;
    }

    private void processProjectedSequentially(
        Projected projected,
        ArrayList<Integer> rmPath,
        int minLabel,
        int maxToc,
        NavigableMap<
            Integer,
            NavigableMap<Integer, NavigableMap<Integer, Projected>>
        > new_fwd_root,
        NavigableMap<Integer, NavigableMap<Integer, Projected>> new_bck_root,
        ArrayList<Edge> edges
    ) {
        for (PDFS aProjected : projected) {
            int id = aProjected.id;
            History history = new History(TRANS.get(id), aProjected);

            if (DFS_CODE.countNode() == arg.maxNodeNum - 1) {
                for (int i = history.size() - 1; i >= 0; --i) {
                    for (int j = history.size() - 1; j >= 0; --j) {
                        Edge e = Misc.getBackward(
                            TRANS.get(id),
                            history.get(i),
                            history.get(j),
                            history
                        );
                        if (e != null) {
                            int key_1 = DFS_CODE.get(i).from;
                            NavigableMap<Integer, Projected> root_1 =
                                new_bck_root.computeIfAbsent(key_1, k ->
                                    new TreeMap<>()
                                );
                            int key_2 = e.eLabel;
                            Projected root_2 = root_1.get(key_2);
                            if (root_2 == null) {
                                root_2 = new Projected();
                                root_1.put(key_2, root_2);
                            }
                            root_2.push(id, e, aProjected);
                        }
                    }
                }
            } else {
                for (int i = history.size() - 1; i >= 0; --i) {
                    Edge e = Misc.getBackward(
                        TRANS.get(id),
                        history.get(i),
                        history.get(rmPath.get(0)),
                        history
                    );
                    if (e != null) {
                        int key_1 = DFS_CODE.get(i).from;
                        NavigableMap<Integer, Projected> root_1 =
                            new_bck_root.computeIfAbsent(key_1, k ->
                                new TreeMap<>()
                            );
                        int key_2 = e.eLabel;
                        Projected root_2 = root_1.get(key_2);
                        if (root_2 == null) {
                            root_2 = new Projected();
                            root_1.put(key_2, root_2);
                        }
                        root_2.push(id, e, aProjected);
                    }
                }
            }

            if (
                Misc.getForwardPure(
                    TRANS.get(id),
                    history.get(rmPath.get(0)),
                    minLabel,
                    history,
                    edges
                )
            ) {
                for (Edge it : edges) {
                    NavigableMap<
                        Integer,
                        NavigableMap<Integer, Projected>
                    > root_1 = new_fwd_root.computeIfAbsent(maxToc, k ->
                        new TreeMap<>()
                    );
                    int key_2 = it.eLabel;
                    NavigableMap<Integer, Projected> root_2 =
                        root_1.computeIfAbsent(key_2, k -> new TreeMap<>());
                    int key_3 = TRANS.get(id).get(it.to).label;
                    Projected root_3 = root_2.get(key_3);
                    if (root_3 == null) {
                        root_3 = new Projected();
                        root_2.put(key_3, root_3);
                    }
                    root_3.push(id, it, aProjected);
                }
            }

            for (int i = history.size() - 1; i >= 0; --i) if (
                Misc.getForwardRmPath(
                    TRANS.get(id),
                    history.get(i),
                    minLabel,
                    history,
                    edges
                )
            ) for (Edge it : edges) {
                int key_1 = DFS_CODE.get(i).from;
                NavigableMap<Integer, NavigableMap<Integer, Projected>> root_1 =
                    new_fwd_root.computeIfAbsent(key_1, k -> new TreeMap<>());
                int key_2 = it.eLabel;
                NavigableMap<Integer, Projected> root_2 =
                    root_1.computeIfAbsent(key_2, k -> new TreeMap<>());
                int key_3 = TRANS.get(id).get(it.to).label;
                Projected root_3 = root_2.get(key_3);
                if (root_3 == null) {
                    root_3 = new Projected();
                    root_2.put(key_3, root_3);
                }
                root_3.push(id, it, aProjected);
            }
        }
    }

    private void project_Initial(Projected projected) throws IOException {
        recursionCount++;

        int sup = support(projected);
        if (sup < arg.minSup) return;

        if (!isUnique()) {
            return;
        } else {
            reporter.reportwithCoveredEdges(sup, projected, DFS_CODE);
        }

        if (
            arg.maxNodeNum >= arg.minNodeNum &&
            DFS_CODE.countNode() > arg.maxNodeNum
        ) return;

        ArrayList<Integer> rmPath = DFS_CODE.buildRMPath();
        int minLabel = DFS_CODE.get(0).fromLabel;
        int maxToc = DFS_CODE.get(rmPath.get(0)).to;

        NavigableMap<
            Integer,
            NavigableMap<Integer, NavigableMap<Integer, Projected>>
        > new_fwd_root = new TreeMap<>();
        NavigableMap<Integer, NavigableMap<Integer, Projected>> new_bck_root =
            new TreeMap<>();

        ArrayList<Edge> edges = getEdgeList();

        try {
            processProjectedSequentially(
                projected,
                rmPath,
                minLabel,
                maxToc,
                new_fwd_root,
                new_bck_root,
                edges
            );

            for (Entry<
                Integer,
                NavigableMap<Integer, Projected>
            > to : new_bck_root.entrySet()) {
                for (Entry<Integer, Projected> eLabel : to
                    .getValue()
                    .entrySet()) {
                    DFS_CODE.push(maxToc, to.getKey(), -1, eLabel.getKey(), -1);
                    project_Initial(eLabel.getValue());
                    DFS_CODE.pop();
                }
            }

            for (Entry<
                Integer,
                NavigableMap<Integer, NavigableMap<Integer, Projected>>
            > from : new_fwd_root.descendingMap().entrySet()) {
                for (Entry<
                    Integer,
                    NavigableMap<Integer, Projected>
                > eLabel : from.getValue().entrySet()) {
                    for (Entry<Integer, Projected> toLabel : eLabel
                        .getValue()
                        .entrySet()) {
                        DFS_CODE.push(
                            from.getKey(),
                            maxToc + 1,
                            -1,
                            eLabel.getKey(),
                            toLabel.getKey()
                        );
                        project_Initial(toLabel.getValue());
                        DFS_CODE.pop();
                    }
                }
            }
        } finally {
            recycleEdgeList(edges);
        }
    }

    private void projectWithHeuristic(Projected initialProjected)
        throws IOException {
        List<IndependentTask> currentTasks = new ArrayList<>();
        currentTasks.add(
            new IndependentTask(
                DFS_CODE,
                initialProjected,
                DFS_CODE.countNode()
            )
        );

        int maxIterations = Math.min(500000, 50000);
        int iteration = 0;

        while (!currentTasks.isEmpty() && iteration < maxIterations) {
            iteration++;

            IndependentTask bestTask = currentTasks
                .stream()
                .max((t1, t2) ->
                    Integer.compare(t1.projected.size(), t2.projected.size())
                )
                .orElse(null);

            if (bestTask == null) break;
            currentTasks.remove(bestTask);

            try {
                int sup = support(bestTask.projected);
                if (sup >= arg.minSup && isIndependentUnique(bestTask)) {
                    synchronized (reporter) {
                        reporter.reportwithCoveredEdges(
                            sup,
                            bestTask.projected,
                            bestTask.dfsCode
                        );
                    }
                }

                List<IndependentTask> extensions =
                    generateIndependentExtensions(bestTask);

                int maxExtensions = Math.max(5, 15 - bestTask.depth);
                extensions = extensions
                    .stream()
                    .sorted((t1, t2) ->
                        Integer.compare(
                            t2.projected.size(),
                            t1.projected.size()
                        )
                    )
                    .limit(maxExtensions)
                    .collect(java.util.stream.Collectors.toList());

                currentTasks.addAll(extensions);

                if (currentTasks.size() > 5000) {
                    currentTasks = currentTasks
                        .stream()
                        .sorted((t1, t2) ->
                            Integer.compare(
                                t2.projected.size(),
                                t1.projected.size()
                            )
                        )
                        .limit(3000)
                        .collect(java.util.stream.Collectors.toList());
                }
            } catch (Exception e) {}
        }
    }

    private List<Graph> supplementFromTED(
        List<Graph> existingGraphs,
        int targetCount
    ) throws IOException {
        List<Graph> allTEDGraphs = new ArrayList<>();

        try (
            BufferedReader reader = new BufferedReader(
                new FileReader(arg.TEDPath)
            )
        ) {
            allTEDGraphs = parseTEDFile(reader);
        }

        if (allTEDGraphs.isEmpty()) {
            return existingGraphs;
        }

        List<Graph> supplementGraphs = new ArrayList<>();

        Set<String> existingSignatures = new HashSet<>();
        for (Graph existingGraph : existingGraphs) {
            existingSignatures.add(
                IsomorphismRemover.getGraphSignature(existingGraph)
            );
        }

        for (int i = allTEDGraphs.size() - 1; i >= 0; i--) {
            Graph candidateGraph = allTEDGraphs.get(i);

            if (
                candidateGraph.size() < arg.minNodeNum ||
                candidateGraph.size() > arg.maxNodeNum
            ) {
                continue;
            }

            String candidateSignature = IsomorphismRemover.getGraphSignature(
                candidateGraph
            );

            if (!existingSignatures.contains(candidateSignature)) {
                boolean alreadyExists = false;
                for (Graph supplementGraph : supplementGraphs) {
                    String supplementSignature =
                        IsomorphismRemover.getGraphSignature(supplementGraph);
                    if (candidateSignature.equals(supplementSignature)) {
                        alreadyExists = true;
                        break;
                    }
                }

                if (!alreadyExists) {
                    supplementGraphs.add(candidateGraph);
                    existingSignatures.add(candidateSignature);

                    if (
                        existingGraphs.size() + supplementGraphs.size() >=
                        targetCount
                    ) {
                        break;
                    }
                }
            }
        }

        List<Graph> result = new ArrayList<>(existingGraphs);
        result.addAll(supplementGraphs);

        return result;
    }

    private List<Graph> parseTEDFile(BufferedReader reader) throws IOException {
        List<Graph> graphs = new ArrayList<>();
        String line;
        Graph currentGraph = null;

        while ((line = reader.readLine()) != null) {
            line = line.trim();
            if (line.isEmpty()) continue;

            String[] parts = line.split(" ");

            if (
                parts.length >= 4 &&
                (parts[0].equals("BeforeSwap") || parts[0].equals("Final")) &&
                parts[1].equals("t") &&
                parts[2].equals("#")
            ) {
                if (currentGraph != null && !currentGraph.isEmpty()) {
                    finalizeParsedGraph(currentGraph);
                    graphs.add(currentGraph);
                }

                currentGraph = new Graph(directed);
            } else if (parts.length >= 3 && parts[0].equals("v")) {
                if (currentGraph != null) {
                    int nodeId = Integer.parseInt(parts[1]);
                    int nodeLabel = Integer.parseInt(parts[2]);

                    while (currentGraph.size() <= nodeId) {
                        currentGraph.add(new Vertex());
                    }
                    currentGraph.get(nodeId).label = nodeLabel;
                }
            } else if (parts.length >= 4 && parts[0].equals("e")) {
                if (currentGraph != null) {
                    int from = Integer.parseInt(parts[1]);
                    int to = Integer.parseInt(parts[2]);
                    int eLabel = Integer.parseInt(parts[3]);

                    int maxNode = Math.max(from, to);
                    while (currentGraph.size() <= maxNode) {
                        currentGraph.add(new Vertex());
                    }

                    currentGraph.get(from).push(from, to, eLabel);
                    if (!directed) {
                        currentGraph.get(to).push(to, from, eLabel);
                    }
                }
            }
        }

        if (currentGraph != null && !currentGraph.isEmpty()) {
            finalizeParsedGraph(currentGraph);
            graphs.add(currentGraph);
        }

        return graphs;
    }

    private void finalizeParsedGraph(Graph graph) {
        try {
            NavigableMap<String, Integer> tmp = new TreeMap<>();
            int id = 0;

            for (int from = 0; from < graph.size(); ++from) {
                for (Edge edge : graph.get(from).edge) {
                    String buf;
                    if (directed || from <= edge.to) {
                        buf = from + " " + edge.to + " " + edge.eLabel;
                    } else {
                        buf = edge.to + " " + from + " " + edge.eLabel;
                    }

                    if (tmp.get(buf) == null) {
                        edge.id = id;
                        tmp.put(buf, id);
                        ++id;
                    } else {
                        edge.id = tmp.get(buf);
                    }
                }
            }

            try {
                java.lang.reflect.Field edgeSizeField = graph
                    .getClass()
                    .getDeclaredField("edge_size");
                edgeSizeField.setAccessible(true);
                edgeSizeField.setInt(graph, id);
            } catch (Exception e) {
                System.err.println(
                    "Warning: Could not set edge_size for parsed graph"
                );
            }
        } catch (Exception e) {
            System.err.println(
                "Error finalizing parsed graph: " + e.getMessage()
            );
        }
    }
}
