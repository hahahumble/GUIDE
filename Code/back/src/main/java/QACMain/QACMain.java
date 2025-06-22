package QACMain;

import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.List;

public class QACMain {
    public static void main(String[] args) throws IOException {
        // Check for help request
        if (args.length == 1 && (args[0].equals("-h") || args[0].equals("--help") || args[0].equals("help"))) {
            Arguments.printUsage();
            return;
        }
        
        try {
            System.out.println("Starting QAC system...");
            
            // Parse arguments - use defaults if no arguments provided
            Arguments arguments;
            if (args.length == 0) {
                System.out.println("No arguments provided, using default configuration");
                arguments = new Arguments(new String[]{});  // Use default parameters
            } else {
                arguments = new Arguments(args);
            }
            
            long startTime = System.currentTimeMillis();

            File inFile = new File(arguments.getInFilePath());
            File outFile = new File(arguments.getGraphgPath());

            // Check if input file exists
            if (!inFile.exists()) {
                System.err.println("Error: Input file not found: " + arguments.getInFilePath());
                System.err.println("Hint: Ensure data file is in correct location or specify file path");
                Arguments.printUsage();
                return;
            }
            
            File queryFile = new File(arguments.getQueryFile());
            if (!queryFile.exists()) {
                System.err.println("Error: Query file not found: " + arguments.getQueryFile());
                System.err.println("Hint: Ensure query file is in correct location or specify file path");
                Arguments.printUsage();
                return;
            }

            // If not in rerun mode, ensure IDs are null
            if (!arguments.isRerun) {
                arguments.IDs = null;
                System.out.println("Rerun mode disabled, searching entire database");
                
                // We'll let QACProcessor clear G.txt instead of clearing it here
                // This avoids clearing the file twice
            }
            // If in rerun mode and G.txt exists and is not empty, extract graph IDs
            else if (arguments.isRerun && outFile.exists() && outFile.length() > 0) {
                List<Integer> previousMatchedIDs = my_VF2.extractGraphIDs(arguments.getGraphgPath());
                if (!previousMatchedIDs.isEmpty()) {
                    arguments.setIDs(previousMatchedIDs);
                } else {
                    System.out.println("No previous matching graph IDs found");
                }
            }

            try (FileReader reader = new FileReader(inFile);
                 FileWriter writer = new FileWriter(outFile)) {
                QACProcessor processor = new QACProcessor();
                System.out.println("Starting QAC processor...");
                processor.run(reader, writer, arguments);
                System.out.println("QAC processor completed");
            }
 
            long endTime = System.currentTimeMillis();
            System.out.println("Time(s): " + (endTime - startTime) / 1000.0);
        } catch (IOException e) {
            System.err.println("Error: " + e.getMessage());
        }
    }
}