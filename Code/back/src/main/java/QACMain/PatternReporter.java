package QACMain;

import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;
import java.util.TreeMap;
import java.util.Vector;
import model.DFSCode;
import model.Edge;
import model.Graph;
import model.PDFS;
import model.Projected;

public class PatternReporter {

    private final ArrayList<Graph> TRANS;
    private Arguments arg;
    private GraphIndexManager indexManager;
    private FileWriter os;
    private long ID;
    private boolean directed;
    private final ArrayList<Graph> allGraphs;

    public PatternReporter(
        ArrayList<Graph> TRANS,
        Arguments arg,
        GraphIndexManager indexManager
    ) {
        this.TRANS = TRANS;
        this.arg = arg;
        this.indexManager = indexManager;
        this.allGraphs = new ArrayList<Graph>();
        this.directed = false;
        this.ID = 0;
    }

    public void setFileWriter(FileWriter os) {
        this.os = os;
    }

    public void setID(long ID) {
        this.ID = ID;
    }

    public ArrayList<Graph> getAllGraphs() {
        return allGraphs;
    }

    public void reportSingle(Graph g, NavigableMap<Integer, Integer> nCount)
        throws IOException {
        int sup = 0;

        for (Map.Entry<Integer, Integer> it : nCount.entrySet()) {
            sup += Common.getValue(it.getValue());
        }

        if (
            arg.maxNodeNum >= arg.minNodeNum && g.size() > arg.maxNodeNum
        ) return;
        if (arg.minNodeNum > 0 && g.size() < arg.minNodeNum) return;

        os.write(
            "t # " + ID + " * " + sup + System.getProperty("line.separator")
        );
        g.write(os);
        ID++;
    }

    public void newreport(Graph g, int id) throws IOException {
        Graph queryGraph = TEDSProcessor.getQueryGraphReference();
        if (
            queryGraph != null && !SubgraphValidator.isSubgraph(queryGraph, g)
        ) {
            System.out.println(
                "Skip Final graph " +
                id +
                ": query is not subgraph (target: " +
                g.size() +
                " vertices, " +
                g.getEdgeSize() +
                " edges)"
            );
            return;
        }

        if (
            g != null &&
            !(arg.maxNodeNum >= arg.minNodeNum && g.size() > arg.maxNodeNum) &&
            !(arg.minNodeNum > 0 && g.size() < arg.minNodeNum)
        ) {
            int sup = 0;
            int ID = id;

            String graphOutput =
                "Final t # " +
                ID +
                " * " +
                sup +
                System.getProperty("line.separator");

            os.write(graphOutput);
            g.write(os);

            try (
                FileWriter resultWriter = new FileWriter(
                    arg.getResultPath(),
                    id != 0
                )
            ) {
                resultWriter.write(graphOutput);
                g.write(resultWriter);
            } catch (IOException e) {
                System.err.println(
                    "Error writing to result file: " + e.getMessage()
                );
            }

            ++ID;
        } else {
            System.out.println(
                "Skip graph with invalid node count (nodes: " +
                (g != null ? g.size() : "null") +
                ", required: " +
                arg.minNodeNum +
                "-" +
                arg.maxNodeNum +
                ")"
            );
        }
    }

    public void newreportbeforeswap(Graph g, int id) throws IOException {
        Graph queryGraph = TEDSProcessor.getQueryGraphReference();
        if (
            queryGraph != null && !SubgraphValidator.isSubgraph(queryGraph, g)
        ) {
            System.out.println(
                "Skip BeforeSwap graph " +
                id +
                ": query is not subgraph (target: " +
                g.size() +
                " vertices, " +
                g.getEdgeSize() +
                " edges)"
            );
            return;
        }

        if (
            g != null &&
            !(arg.maxNodeNum >= arg.minNodeNum && g.size() > arg.maxNodeNum) &&
            !(arg.minNodeNum > 0 && g.size() < arg.minNodeNum)
        ) {
            int sup = 0;
            int ID = id;
            os.write(
                "BeforeSwap t # " +
                ID +
                " * " +
                sup +
                System.getProperty("line.separator")
            );
            g.write(os);
            ++ID;
        } else {
            System.out.println(
                "Skip graph with invalid node count (nodes: " +
                g.size() +
                ", required: " +
                arg.minNodeNum +
                "-" +
                arg.maxNodeNum +
                ")"
            );
        }
    }

    public Boolean reportwithCoveredEdges(
        int sup,
        Projected projected,
        DFSCode DFS_CODE
    ) throws IOException {
        if (
            arg.maxNodeNum >= arg.minNodeNum &&
            DFS_CODE.countNode() > arg.maxNodeNum
        ) return false;
        if (
            arg.minNodeNum > 0 && DFS_CODE.countNode() < arg.minNodeNum
        ) return false;

        Graph tempGraph = new Graph(directed);
        DFS_CODE.toGraph(tempGraph);
        if (
            tempGraph.size() > arg.maxNodeNum ||
            tempGraph.size() < arg.minNodeNum
        ) {
            return false;
        }

        if (
            arg.strategy.equals("greedy") ||
            allGraphs.size() < arg.numberofpatterns
        ) {
            if (arg.isPESIndex) {
                Insert(projected, allGraphs.size(), DFS_CODE);
            } else if (arg.isSimpleIndex) {
                InsertWithSimpleIndex(projected, allGraphs.size(), DFS_CODE);
            }

            if (allGraphs.size() == arg.numberofpatterns) {
                int count = 0;
                for (Graph tempg : allGraphs) {
                    newreportbeforeswap(tempg, count++);
                }
                System.out.println(
                    "Before swapping, covered edges: " +
                    indexManager.getAllCoveredEdges().size()
                );
                int totalegdes = 0;
                for (int i = 0; i < TRANS.size(); i++) {
                    totalegdes += TRANS.get(i).getEdgeSize();
                }
                System.out.println("Total edges: " + totalegdes);
                System.out.println(
                    "Coverage rate: " +
                    (indexManager.getAllCoveredEdges().size() * 1.0) /
                    totalegdes
                );

                if (arg.isSimpleIndex && arg.strategy.equals("topk")) {
                    updateMinimumPattern();
                }
            }
        } else {
            return handleSwappingLogic(sup, projected, DFS_CODE);
        }
        return false;
    }

    public void reportwithCoveredEdges_Initial(
        int sup,
        Projected projected,
        DFSCode DFS_CODE
    ) throws IOException {
        if (
            arg.maxNodeNum >= arg.minNodeNum &&
            DFS_CODE.countNode() > arg.maxNodeNum
        ) return;
        if (arg.minNodeNum > 0 && DFS_CODE.countNode() < arg.minNodeNum) return;

        Graph tempGraph = new Graph(directed);
        DFS_CODE.toGraph(tempGraph);
        if (
            tempGraph.size() > arg.maxNodeNum ||
            tempGraph.size() < arg.minNodeNum
        ) {
            System.out.println(
                "reportwithCoveredEdges_Initial: ignoring graph with invalid node count (nodes: " +
                tempGraph.size() +
                ", required: " +
                arg.minNodeNum +
                "-" +
                arg.maxNodeNum +
                ")"
            );
            return;
        }

        if (allGraphs.size() < arg.numberofpatterns) {
            Graph g = new Graph(directed);
            DFS_CODE.toGraph(g);
            os.write(
                "Initial****t # " +
                allGraphs.size() +
                " * " +
                sup +
                System.getProperty("line.separator")
            );
            g.write(os);

            if (arg.isPESIndex) {
                Insert(projected, allGraphs.size(), DFS_CODE);
            } else if (arg.isSimpleIndex) {
                InsertWithSimpleIndex(projected, allGraphs.size(), DFS_CODE);
            }

            if (allGraphs.size() == arg.numberofpatterns) {
                handleInitialReportCompletion();
            }
        } else {
            handleInitialSwappingLogic(sup, projected, DFS_CODE);
        }
    }

    public int getBenefitScore(Set<Integer> coverededges) {
        int unique_count = 0;
        Iterator it = coverededges.iterator();
        while (it.hasNext()) {
            if (indexManager.getAllCoveredEdges().contains(it.next())) continue;
            unique_count++;
        }
        return unique_count;
    }

    public int getLossScore(Set<Integer> dropededges, Long deleteid) {
        int loss_count = 0;

        Set<Integer> set_temp = new HashSet<Integer>();
        for (Long key : indexManager.getCoveredEdges_patterns().keySet()) {
            if (key != deleteid) set_temp.addAll(
                indexManager.getCoveredEdges_patterns().get(key)
            );
        }
        Iterator it = dropededges.iterator();
        while (it.hasNext()) {
            Integer edgeid = (Integer) it.next();
            if (!set_temp.contains(edgeid)) {
                loss_count++;
            }
        }
        return loss_count;
    }

    public void Delete(int deleteid) {
        allGraphs.set(deleteid, null);

        indexManager
            .getRpriv_i()
            .get(indexManager.getPriv_pattern().get(deleteid))
            .remove(deleteid);

        Set<Integer> coverededges_pattern = indexManager
            .getCoveredEdges_EachPattern()
            .get(deleteid);

        for (Integer e : coverededges_pattern) {
            indexManager.getRcov_edge().get(e).remove(deleteid);

            if (indexManager.getRcov_edge().get(e).size() == 0) {
                indexManager.setNumberofcovered(
                    indexManager.getNumberofcovered() - 1
                );
                indexManager.getAllCoveredEdges().remove(e);
            } else if (indexManager.getRcov_edge().get(e).size() == 1) {
                int tempid = indexManager
                    .getRcov_edge()
                    .get(e)
                    .iterator()
                    .next();
                int temp = indexManager.getPriv_pattern().get(tempid);
                indexManager.getPriv_pattern().set(tempid, temp + 1);
                indexManager.getRpriv_i().get(temp).remove(tempid);
                if (
                    indexManager.getRpriv_i().get(temp + 1) == null
                ) indexManager
                    .getRpriv_i()
                    .put(temp + 1, new HashSet<Integer>());
                indexManager.getRpriv_i().get(temp + 1).add(tempid);
            }
        }

        indexManager.getCoveredEdges_OriginalGraphs().clear();
        for (Integer edgeid : indexManager.getAllCoveredEdges()) {
            Integer gid = edgeid / 1000;
            Set<Integer> temp = indexManager
                .getCoveredEdges_OriginalGraphs()
                .get(gid);
            if (temp == null) temp = new HashSet<Integer>();
            temp.add(edgeid);
            indexManager.getCoveredEdges_OriginalGraphs().put(gid, temp);
        }

        indexManager.getPriv_pattern().set(deleteid, -1);
        indexManager
            .getCoveredEdges_EachPattern()
            .set(deleteid, new HashSet<Integer>());
    }

    public void Insert(Projected projected, int insertid, DFSCode DFS_CODE) {
        Graph g = new Graph(directed);
        DFS_CODE.toGraph(g);

        Graph queryGraph = TEDSProcessor.getQueryGraphReference();
        if (
            queryGraph != null && !SubgraphValidator.isSubgraph(queryGraph, g)
        ) {
            System.out.println(
                "Skip graph " +
                insertid +
                ": query is not subgraph (target: " +
                g.size() +
                " vertices, " +
                g.getEdgeSize() +
                " edges)"
            );
            return;
        }

        if (
            g != null &&
            !(arg.maxNodeNum >= arg.minNodeNum && g.size() > arg.maxNodeNum) &&
            !(arg.minNodeNum > 0 && g.size() < arg.minNodeNum)
        ) {
            if (allGraphs.size() < arg.numberofpatterns) allGraphs.add(g);
            else allGraphs.set(insertid, g);
        } else {
            return;
        }

        Set<Integer> coverededges_pattern = new HashSet<Integer>();
        if (
            indexManager.getPriv_pattern().size() < arg.numberofpatterns
        ) indexManager.getPriv_pattern().add(0);
        else indexManager.getPriv_pattern().set(insertid, 0);

        for (PDFS aProjected : projected) {
            int id = aProjected.id;
            Set<Integer> tempedges = new HashSet<Integer>();
            for (PDFS p = aProjected; p != null; p = p.prev) {
                Integer temp = 1000 * id + p.edge.id;
                coverededges_pattern.add(temp);
                tempedges.add(temp);
                indexManager.getAllCoveredEdges().add(temp);
            }

            if (indexManager.getCoveredEdges_OriginalGraphs().containsKey(id)) {
                tempedges.addAll(
                    indexManager.getCoveredEdges_OriginalGraphs().get(id)
                );
                indexManager
                    .getCoveredEdges_OriginalGraphs()
                    .replace(id, tempedges);
            } else {
                indexManager
                    .getCoveredEdges_OriginalGraphs()
                    .put(id, tempedges);
            }
        }

        if (
            indexManager.getCoveredEdges_EachPattern().size() <
            arg.numberofpatterns
        ) indexManager.getCoveredEdges_EachPattern().add(coverededges_pattern);
        else indexManager
            .getCoveredEdges_EachPattern()
            .set(insertid, coverededges_pattern);

        updateCoverageStatistics(coverededges_pattern, insertid);
    }

    public void InsertWithSimpleIndex(
        Projected projected,
        int insertid,
        DFSCode DFS_CODE
    ) {
        Graph g = new Graph(directed);
        DFS_CODE.toGraph(g);

        Graph queryGraph = TEDSProcessor.getQueryGraphReference();
        if (
            queryGraph != null && !SubgraphValidator.isSubgraph(queryGraph, g)
        ) {
            System.out.println(
                "Skip graph " +
                insertid +
                ": query is not subgraph (target: " +
                g.size() +
                " vertices, " +
                g.getEdgeSize() +
                " edges)"
            );
            return;
        }

        if (
            g != null &&
            !(arg.maxNodeNum >= arg.minNodeNum && g.size() > arg.maxNodeNum) &&
            !(arg.minNodeNum > 0 && g.size() < arg.minNodeNum)
        ) {
            if (allGraphs.size() < arg.numberofpatterns) allGraphs.add(g);
            else if (allGraphs.size() > insertid) allGraphs.set(insertid, g);
            else allGraphs.add(g);
        } else {
            return;
        }

        Set<Integer> coverededges_pattern = new HashSet<Integer>();
        for (PDFS aProjected : projected) {
            int id = aProjected.id;
            Set<Integer> tempedges = new HashSet<Integer>();
            for (PDFS p = aProjected; p != null; p = p.prev) {
                Integer temp = 1000 * id + p.edge.id;
                coverededges_pattern.add(temp);

                if (indexManager.getPatternsID_edges().containsKey(temp)) {
                    Set<Integer> temppatternids = indexManager
                        .getPatternsID_edges()
                        .get(temp);
                    temppatternids.add(id);
                    indexManager
                        .getPatternsID_edges()
                        .replace(temp, temppatternids);
                } else {
                    Set<Integer> temppatternids = new HashSet<Integer>();
                    temppatternids.add(id);
                    indexManager
                        .getPatternsID_edges()
                        .put(temp, temppatternids);
                }

                indexManager.getAllCoveredEdges().add(temp);
                tempedges.add(temp);
            }

            if (indexManager.getCoveredEdges_OriginalGraphs().containsKey(id)) {
                tempedges.addAll(
                    indexManager.getCoveredEdges_OriginalGraphs().get(id)
                );
                indexManager
                    .getCoveredEdges_OriginalGraphs()
                    .replace(id, tempedges);
            } else {
                indexManager
                    .getCoveredEdges_OriginalGraphs()
                    .put(id, tempedges);
            }
        }

        indexManager
            .getCoveredEdges_patterns()
            .put((long) insertid, coverededges_pattern);
    }

    private void updateMinimumPattern() {
        int patternid_min = 0;
        int loss_score_min = Integer.MAX_VALUE;
        for (Long key : indexManager.getCoveredEdges_patterns().keySet()) {
            Set<Integer> dropededges = indexManager
                .getCoveredEdges_patterns()
                .get(key);
            int loss_score = getLossScore(dropededges, key);
            if (loss_score < loss_score_min) {
                loss_score_min = loss_score;
                patternid_min = key.intValue();
            }
        }
        indexManager.setMinimumpattern_score(loss_score_min);
        indexManager.setMinimumpattern_id(patternid_min);
    }

    private Boolean handleSwappingLogic(
        int sup,
        Projected projected,
        DFSCode DFS_CODE
    ) throws IOException {
        int benefit_score = 0;
        int patternid_min = -1;
        int loss_score_min = Integer.MAX_VALUE;

        if (arg.isPESIndex) {
            return handlePESIndexSwapping(
                sup,
                projected,
                DFS_CODE,
                benefit_score,
                patternid_min,
                loss_score_min
            );
        } else if (arg.isSimpleIndex) {
            return handleSimpleIndexSwapping(
                sup,
                projected,
                DFS_CODE,
                benefit_score,
                patternid_min,
                loss_score_min
            );
        }

        return false;
    }

    private Boolean handlePESIndexSwapping(
        int sup,
        Projected projected,
        DFSCode DFS_CODE,
        int benefit_score,
        int patternid_min,
        int loss_score_min
    ) throws IOException {
        Set<Integer> coverededges_pattern = new HashSet<Integer>();
        for (PDFS aProjected : projected) {
            int id = aProjected.id;
            for (PDFS p = aProjected; p != null; p = p.prev) {
                Integer temp = 1000 * id + p.edge.id;
                coverededges_pattern.add(temp);
            }
        }

        for (Integer e : coverededges_pattern) {
            if (
                indexManager.getRcov_edge().get(e) == null ||
                indexManager.getRcov_edge().get(e).size() == 0
            ) {
                benefit_score++;
            }
        }

        patternid_min = indexManager.getMinimumpattern_id();
        loss_score_min = indexManager.getMinimumpattern_score();

        Boolean swapflag = shouldSwap(benefit_score, loss_score_min);

        if (swapflag) {
            os.write(patternid_min + " is swapped out!");
            os.write(
                "(Swapping Phase) benefit_score: " +
                benefit_score +
                ", loss_score_min: " +
                loss_score_min +
                "\n"
            );
            allGraphs.get(patternid_min).write(os);

            Delete(patternid_min);
            Insert(projected, patternid_min, DFS_CODE);
        }

        return swapflag;
    }

    private Boolean handleSimpleIndexSwapping(
        int sup,
        Projected projected,
        DFSCode DFS_CODE,
        int benefit_score,
        int patternid_min,
        int loss_score_min
    ) throws IOException {
        Set<Integer> coverededges_pattern = new HashSet<Integer>();
        for (PDFS aProjected : projected) {
            int id = aProjected.id;
            for (PDFS p = aProjected; p != null; p = p.prev) {
                Integer temp = 1000 * id + p.edge.id;
                coverededges_pattern.add(temp);
            }
        }

        benefit_score = getBenefitScore(coverededges_pattern);
        loss_score_min = indexManager.getMinimumpattern_score();
        patternid_min = indexManager.getMinimumpattern_id();

        Boolean swapflag = shouldSwap(benefit_score, loss_score_min);

        if (swapflag) {
            performSimpleIndexSwap(
                patternid_min,
                benefit_score,
                loss_score_min,
                coverededges_pattern,
                DFS_CODE
            );
        }

        return swapflag;
    }

    private boolean shouldSwap(int benefit_score, int loss_score_min) {
        if (arg.swapcondition.equals("swap1")) {
            return benefit_score > 2 * loss_score_min;
        } else if (arg.swapcondition.equals("swap2")) {
            return (
                benefit_score >
                loss_score_min +
                (indexManager.getAllCoveredEdges().size() * 1.0) /
                arg.numberofpatterns
            );
        } else {
            return (
                benefit_score >
                (1 + arg.swapAlpha) * loss_score_min +
                (1 - arg.swapAlpha) *
                ((indexManager.getAllCoveredEdges().size() * 1.0) /
                    arg.numberofpatterns)
            );
        }
    }

    private void performSimpleIndexSwap(
        int patternid_min,
        int benefit_score,
        int loss_score_min,
        Set<Integer> coverededges_pattern,
        DFSCode DFS_CODE
    ) throws IOException {
        os.write(patternid_min + " is swapped out!");
        os.write(
            "(Swapping Phase) benefit_score: " +
            benefit_score +
            ", loss_score_min: " +
            loss_score_min
        );
        allGraphs.get(patternid_min).write(os);

        Graph g = new Graph(directed);
        DFS_CODE.toGraph(g);
        allGraphs.set(patternid_min, g);

        indexManager
            .getCoveredEdges_patterns()
            .replace((long) patternid_min, coverededges_pattern);

        indexManager.getAllCoveredEdges().clear();
        for (Long key : indexManager.getCoveredEdges_patterns().keySet()) {
            Set<Integer> temp = indexManager
                .getCoveredEdges_patterns()
                .get(key);
            indexManager.getAllCoveredEdges().addAll(temp);
        }

        updateOriginalGraphCoveredEdges();

        updatePatternsIDEdges();

        updateMinimumPattern();
    }

    private void updateOriginalGraphCoveredEdges() {
        indexManager.getCoveredEdges_OriginalGraphs().clear();
        for (Integer edgeid : indexManager.getAllCoveredEdges()) {
            Integer gid = edgeid / 1000;
            Set<Integer> temp = indexManager
                .getCoveredEdges_OriginalGraphs()
                .get(gid);
            if (temp == null) temp = new HashSet<Integer>();
            temp.add(edgeid);
            indexManager.getCoveredEdges_OriginalGraphs().put(gid, temp);
        }
    }

    private void updatePatternsIDEdges() {
        indexManager.getPatternsID_edges().clear();
        for (Integer edgeid : indexManager.getAllCoveredEdges()) {
            Set<Integer> tempedges = new HashSet<Integer>();
            for (Long key : indexManager.getCoveredEdges_patterns().keySet()) {
                Set<Integer> temp = indexManager
                    .getCoveredEdges_patterns()
                    .get(key);
                if (temp.contains(edgeid)) {
                    tempedges.add(key.intValue());
                }
            }
            indexManager.getPatternsID_edges().put(edgeid, tempedges);
        }
    }

    private void updateCoverageStatistics(
        Set<Integer> coverededges_pattern,
        int insertid
    ) {
        for (int temp : coverededges_pattern) {
            if (indexManager.getRcov_edge().get(temp) == null) indexManager
                .getRcov_edge()
                .put(temp, new HashSet<Integer>());

            indexManager.getRcov_edge().get(temp).add(insertid);

            if (indexManager.getRcov_edge().get(temp).size() == 1) {
                indexManager
                    .getPriv_pattern()
                    .set(
                        insertid,
                        indexManager.getPriv_pattern().get(insertid) + 1
                    );
                indexManager.setNumberofcovered(
                    indexManager.getNumberofcovered() + 1
                );
            } else if (indexManager.getRcov_edge().get(temp).size() == 2) {
                int tempid = -1;
                for (int e : indexManager.getRcov_edge().get(temp)) if (
                    e != insertid
                ) tempid = e;

                indexManager
                    .getPriv_pattern()
                    .set(
                        tempid,
                        indexManager.getPriv_pattern().get(tempid) - 1
                    );

                indexManager
                    .getRpriv_i()
                    .get(indexManager.getPriv_pattern().get(tempid) + 1)
                    .remove(tempid);
                if (
                    indexManager
                        .getRpriv_i()
                        .get(indexManager.getPriv_pattern().get(tempid)) ==
                    null
                ) indexManager
                    .getRpriv_i()
                    .put(
                        indexManager.getPriv_pattern().get(tempid),
                        new HashSet<Integer>()
                    );
                indexManager
                    .getRpriv_i()
                    .get(indexManager.getPriv_pattern().get(tempid))
                    .add(tempid);
            }
        }

        if (
            indexManager
                .getRpriv_i()
                .get(indexManager.getPriv_pattern().get(insertid)) ==
            null
        ) indexManager
            .getRpriv_i()
            .put(
                indexManager.getPriv_pattern().get(insertid),
                new HashSet<Integer>()
            );
        indexManager
            .getRpriv_i()
            .get(indexManager.getPriv_pattern().get(insertid))
            .add(insertid);

        for (
            int i = 0;
            i <= indexManager.getPriv_pattern().get(insertid);
            i++
        ) {
            if (
                indexManager.getRpriv_i().get(i) != null &&
                indexManager.getRpriv_i().get(i).size() > 0
            ) {
                indexManager.setMinimumpattern_id(
                    indexManager.getRpriv_i().get(i).iterator().next()
                );
                indexManager.setMinimumpattern_score(
                    indexManager
                        .getPriv_pattern()
                        .get(indexManager.getMinimumpattern_id())
                );
                break;
            }
        }
    }

    private void handleInitialReportCompletion() throws IOException {
        int count = 0;
        for (Graph tempg : allGraphs) {
            newreportbeforeswap(tempg, count++);
        }
        System.out.println(
            "Before swapping, covered edges: " +
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

        if (arg.isSimpleIndex && arg.strategy.equals("topk")) {
            updateMinimumPattern();
        }
    }

    private void handleInitialSwappingLogic(
        int sup,
        Projected projected,
        DFSCode DFS_CODE
    ) throws IOException {
        int benefit_score = 0;
        int patternid_min = -1;
        int loss_score_min = Integer.MAX_VALUE;

        if (arg.isPESIndex) {
            handleInitialPESIndexSwapping(
                sup,
                projected,
                DFS_CODE,
                benefit_score,
                patternid_min,
                loss_score_min
            );
        } else if (arg.isSimpleIndex) {
            handleInitialSimpleIndexSwapping(
                sup,
                projected,
                DFS_CODE,
                benefit_score,
                patternid_min,
                loss_score_min
            );
        }
    }

    private void handleInitialPESIndexSwapping(
        int sup,
        Projected projected,
        DFSCode DFS_CODE,
        int benefit_score,
        int patternid_min,
        int loss_score_min
    ) throws IOException {
        Set<Integer> coverededges_pattern = new HashSet<Integer>();
        for (PDFS aProjected : projected) {
            int id = aProjected.id;
            for (PDFS p = aProjected; p != null; p = p.prev) {
                Integer temp = 1000 * id + p.edge.id;
                coverededges_pattern.add(temp);
            }
        }

        for (Integer e : coverededges_pattern) {
            if (
                indexManager.getRcov_edge().get(e) == null ||
                indexManager.getRcov_edge().get(e).size() == 0
            ) {
                benefit_score++;
            }
        }

        patternid_min = indexManager.getMinimumpattern_id();
        loss_score_min = indexManager.getMinimumpattern_score();

        Boolean swapflag = shouldSwap(benefit_score, loss_score_min);

        if (swapflag) {
            os.write(patternid_min + " is swapped out!");
            os.write(
                "Initial Swapping, benefit_score: " +
                benefit_score +
                ", loss_score_min: " +
                loss_score_min
            );
            allGraphs.get(patternid_min).write(os);

            Delete(patternid_min);
            Insert(projected, patternid_min, DFS_CODE);
        }
    }

    private void handleInitialSimpleIndexSwapping(
        int sup,
        Projected projected,
        DFSCode DFS_CODE,
        int benefit_score,
        int patternid_min,
        int loss_score_min
    ) throws IOException {
        Set<Integer> coverededges_pattern = new HashSet<Integer>();
        for (PDFS aProjected : projected) {
            int id = aProjected.id;
            for (PDFS p = aProjected; p != null; p = p.prev) {
                Integer temp = 1000 * id + p.edge.id;
                coverededges_pattern.add(temp);
            }
        }

        benefit_score = getBenefitScore(coverededges_pattern);
        loss_score_min = indexManager.getMinimumpattern_score();
        patternid_min = indexManager.getMinimumpattern_id();

        Boolean swapflag = shouldSwap(benefit_score, loss_score_min);

        if (swapflag) {
            performInitialSimpleIndexSwap(
                patternid_min,
                benefit_score,
                loss_score_min,
                coverededges_pattern,
                DFS_CODE
            );
        }
    }

    private void performInitialSimpleIndexSwap(
        int patternid_min,
        int benefit_score,
        int loss_score_min,
        Set<Integer> coverededges_pattern,
        DFSCode DFS_CODE
    ) throws IOException {
        os.write(patternid_min + " is swapped out!");
        os.write(
            "Initial Swapping, benefit_score: " +
            benefit_score +
            ", loss_score_min: " +
            loss_score_min
        );
        allGraphs.get(patternid_min).write(os);

        Graph g = new Graph(directed);
        DFS_CODE.toGraph(g);
        allGraphs.set(patternid_min, g);

        indexManager
            .getCoveredEdges_patterns()
            .replace((long) patternid_min, coverededges_pattern);

        indexManager.getAllCoveredEdges().clear();
        for (Long key : indexManager.getCoveredEdges_patterns().keySet()) {
            Set<Integer> temp = indexManager
                .getCoveredEdges_patterns()
                .get(key);
            indexManager.getAllCoveredEdges().addAll(temp);
        }

        updateOriginalGraphCoveredEdges();
        updatePatternsIDEdges();
        updateMinimumPattern();
    }

    public void finalReportWithDeduplication() throws IOException {
        if (allGraphs.isEmpty()) {
            System.out.println("No graphs to output");
            return;
        }

        System.out.println(
            "Starting final output, original graph count: " + allGraphs.size()
        );

        List<Graph> uniqueGraphs =
            IsomorphismRemover.removeIsomorphicGraphsPreservingOrder(
                allGraphs,
                true
            );

        Collections.sort(uniqueGraphs, (g1, g2) -> {
            int nodeDiff = g1.size() - g2.size();
            if (nodeDiff != 0) {
                return nodeDiff;
            }

            return g1.getEdgeSize() - g2.getEdgeSize();
        });

        System.out.println(
            "After deduplication kept " +
            uniqueGraphs.size() +
            " unique graphs, starting output..."
        );

        int count = 0;
        for (Graph g : uniqueGraphs) {
            newreport(g, count++);
        }

        int removedCount = allGraphs.size() - uniqueGraphs.size();
        System.out.printf(
            "Deduplication statistics: original %d graphs -> kept %d graphs (removed %d isomorphic graphs)\n",
            allGraphs.size(),
            uniqueGraphs.size(),
            removedCount
        );

        if (removedCount > 0) {
            System.out.printf(
                "Deduplication rate: %.2f%%\n",
                ((removedCount * 100.0) / allGraphs.size())
            );
        }
    }

    public void analyzeIsomorphism() {
        if (allGraphs.isEmpty()) {
            System.out.println(
                "Graph collection is empty, cannot analyze isomorphic graphs"
            );
            return;
        }

        System.out.println("\n=== Isomorphic Graph Analysis ===");
        System.out.println("Total graphs: " + allGraphs.size());

        List<Graph> uniqueGraphs = IsomorphismRemover.removeIsomorphicGraphs(
            allGraphs
        );
        int isomorphicCount = allGraphs.size() - uniqueGraphs.size();

        System.out.println("Unique graphs: " + uniqueGraphs.size());
        System.out.println("Isomorphic graphs: " + isomorphicCount);

        if (isomorphicCount > 0) {
            System.out.printf(
                "Isomorphic graph ratio: %.2f%%\n",
                ((isomorphicCount * 100.0) / allGraphs.size())
            );
            System.out.println("Suggest deduplication before final output");
        } else {
            System.out.println("No isomorphic graphs found");
        }

        System.out.println("==================\n");
    }
}
