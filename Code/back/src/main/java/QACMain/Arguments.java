package QACMain;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.List;

public class Arguments {

    public String[] args;
    public String inFilePath =
        "data" + File.separator + "pubchem1000000clean_10k_result.txt";
    public String graphgPath = "Outputs" + File.separator + "G.txt";
    public String TEDPath = "Outputs" + File.separator + "TED.txt";
    public String resultPath = null;
    public String queryFile = "data" + File.separator + "query.txt";
    public String candidateFilePath =
        "Outputs" + File.separator + "candidate.txt";
    public long minSup = 1;
    public long[] gl = {};

    public String swapcondition = "swap1";
    public Double swapAlpha = 0.99;
    public long minNodeNum = 7;
    public long maxNodeNum = 8;
    public String strategy = "topk";
    public Integer numberofpatterns = 10;
    public Integer numberofgraphs = 30;

    public Boolean hasPRM = true;
    public Boolean isPESIndex = true;
    public Boolean isSimpleIndex = !isPESIndex;

    public Boolean hasDSS = true;
    public Boolean hasInitialPatternGenerator = false;

    public Boolean isLightVersion = false;
    public Integer ReadNumInEachBatch = 100000;
    public Double AvgE = 43.783167;
    public Double SampleEdgeNum = 500000.0;

    public Boolean isSimplified = false;
    public List<Integer> IDs = null;
    public Boolean verbose = true;
    public Boolean isRerun = false;

    public String outputDir = "Outputs";

    public Arguments(String[] args) {
        this.args = args;

        File outputDirFile = new File(outputDir);
        if (!outputDirFile.exists()) {
            outputDirFile.mkdirs();
        }

        this.resultPath = this.outputDir + File.separator + "result.txt";

        if (args.length > 0) {
            String fileName =
                args[0].replace("/", File.separator).replace(
                        "\\",
                        File.separator
                    );
            if (!fileName.startsWith("data" + File.separator)) {
                this.inFilePath = "data" + File.separator + fileName;
            } else {
                this.inFilePath = fileName;
            }

            if (args.length > 1) {
                String queryFileName =
                    args[1].replace("/", File.separator).replace(
                            "\\",
                            File.separator
                        );
                if (!queryFileName.startsWith("data" + File.separator)) {
                    this.queryFile = "data" + File.separator + queryFileName;
                } else {
                    this.queryFile = queryFileName;
                }
            }

            if (args.length > 2) {
                try {
                    this.numberofpatterns = Integer.parseInt(args[2]);
                } catch (NumberFormatException e) {
                    System.err.println(
                        "Invalid numberofpatterns parameter, using default: " +
                        this.numberofpatterns
                    );
                }
            }

            if (args.length > 3) {
                try {
                    this.minNodeNum = Long.parseLong(args[3]);
                } catch (NumberFormatException e) {
                    System.err.println(
                        "Invalid minnode parameter, using default: " +
                        this.minNodeNum
                    );
                }
            }

            if (args.length > 4) {
                try {
                    this.maxNodeNum = Long.parseLong(args[4]);
                } catch (NumberFormatException e) {
                    System.err.println(
                        "Invalid maxnode parameter, using default: " +
                        this.maxNodeNum
                    );
                }
            }

            if (args.length > 5) {
                String strategyParam = args[5].toLowerCase();
                if (
                    strategyParam.equals("greedy") ||
                    strategyParam.equals("topk")
                ) {
                    this.strategy = strategyParam;
                } else {
                    System.err.println(
                        "Invalid strategy parameter '" +
                        args[5] +
                        "', using default: " +
                        this.strategy
                    );
                }
            }

            if (args.length > 6) {
                this.outputDir = args[6];

                this.graphgPath = this.outputDir + File.separator + "G.txt";
                this.TEDPath = this.outputDir + File.separator + "TED.txt";
                this.candidateFilePath =
                    this.outputDir + File.separator + "candidate.txt";
                this.resultPath =
                    this.outputDir + File.separator + "result.txt";
            } else {
                this.resultPath =
                    this.outputDir + File.separator + "result.txt";
            }

            if (args.length > 7 && args[7].equalsIgnoreCase("reuse")) {
                this.isRerun = true;
                System.out.println(
                    "Enable reuse mode: only check the last matching graph in G.txt"
                );
            } else {
                this.isRerun = false;
                System.out.println(
                    "Reuse mode disabled: will search entire database"
                );
            }
        }

        printConfigurationSummary();
    }

    /**
     * Print current configuration summary
     */
    private void printConfigurationSummary() {
        System.out.println(
            "Config: " +
            inFilePath.replace("data/", "") +
            " | Query: " +
            queryFile.replace("data/", "") +
            " | Recommendations: " +
            numberofpatterns +
            " | Nodes: " +
            minNodeNum +
            "-" +
            maxNodeNum +
            " | Strategy: " +
            strategy
        );
    }

    /**
     * Print usage instructions
     */
    public static void printUsage() {
        System.out.println("\n=== QAC System Usage ===");
        System.out.println(
            "Usage: java -jar target/my-java-project-1.0-SNAPSHOT-shaded.jar <parameters>"
        );
        System.out.println("\nParameters:");
        System.out.println(
            "  1. <input_file>     - Graph database file (required)"
        );
        System.out.println(
            "  2. <query_file>     - Query graph file (required)"
        );
        System.out.println(
            "  3. <num_patterns>   - Number of recommendations (default: 10)"
        );
        System.out.println(
            "  4. <min_nodes>      - Minimum nodes (default: 2)"
        );
        System.out.println(
            "  5. <max_nodes>      - Maximum nodes (default: 10)"
        );
        System.out.println(
            "  6. <strategy>       - topk|greedy (default: topk)"
        );
        System.out.println(
            "  7. <output_dir>     - Output directory (default: Outputs)"
        );
        System.out.println("  8. <reuse_mode>     - reuse (optional)");
        System.out.println("\nExamples:");
        System.out.println("  # Basic usage");
        System.out.println(
            "  java -jar target/my-java-project-1.0-SNAPSHOT-shaded.jar emolecul10000 query.txt 10 16 19 topk"
        );
        System.out.println("\n  # Use greedy strategy");
        System.out.println(
            "  java -jar target/my-java-project-1.0-SNAPSHOT-shaded.jar emolecul10000 query.txt 10 16 19 greedy"
        );
        System.out.println("=========================\n");
    }

    public String getInFilePath() {
        return inFilePath;
    }

    public String getGraphgPath() {
        return graphgPath;
    }

    public String getQueryFile() {
        return queryFile;
    }

    public long getMinSup() {
        return minSup;
    }

    public long getMinNodeNum() {
        return minNodeNum;
    }

    public long getMaxNodeNum() {
        return maxNodeNum;
    }

    public String getOutFilePath() {
        return graphgPath;
    }

    public String getCandidateFilePath() {
        return candidateFilePath;
    }

    public void setIDs(List<Integer> IDs) {
        this.IDs = IDs;

        this.gl = IDs.stream().mapToLong(Integer::longValue).toArray();
    }

    public Boolean getIsRerun() {
        return isRerun;
    }

    private String readLastLine(File file) throws IOException {
        try (RandomAccessFile raf = new RandomAccessFile(file, "r")) {
            long fileLength = raf.length();

            if (fileLength == 0) {
                return null;
            }

            long pos = fileLength - 1;
            StringBuilder sb = new StringBuilder();
            boolean foundLineEnd = false;

            while (pos >= 0) {
                raf.seek(pos);
                int b = raf.read();

                if (b == '\n' || b == '\r') {
                    if (foundLineEnd) {
                        break;
                    }
                    foundLineEnd = true;
                } else {
                    foundLineEnd = false;
                    sb.insert(0, (char) b);
                }

                pos--;
            }

            return sb.toString();
        }
    }

    public String getResultPath() {
        return resultPath;
    }
}
