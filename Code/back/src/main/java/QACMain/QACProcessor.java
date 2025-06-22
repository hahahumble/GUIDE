package QACMain;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class QACProcessor {
    public void run(FileReader reader, FileWriter writer, Arguments arguments) throws IOException {
        
        // Create VF2 instance
        my_VF2.Vf vf2 = new my_VF2.Vf();
        
        // Use correct file path - graphgPath for VF2 output
        String vf2OutputFile = arguments.getGraphgPath(); // G.txt
        
        // Clear IDs when not in rerun mode to force a full database search
        if (!arguments.isRerun) {
            arguments.IDs = null;
            
            // Clear G.txt file if this is a fresh search
            java.io.File gFile = new java.io.File(vf2OutputFile);
            if (gFile.exists()) {
                try (FileWriter gWriter = new FileWriter(gFile)) {
                    // Write an empty file
                    gWriter.write("");
                } catch (IOException e) {
                    System.err.println("Error clearing G.txt: " + e.getMessage());
                }
            }
        }
        // Only try to read from G.txt when Arguments has no IDs and isRerun is true
        // Avoid duplicate reading and potential race conditions
        else if (arguments.isRerun && (arguments.IDs == null || arguments.IDs.isEmpty())) {
            java.io.File file = new java.io.File(vf2OutputFile);
            if (file.exists() && file.length() > 0) {
                List<Integer> previousMatchedIDs = my_VF2.extractGraphIDs(vf2OutputFile);
                if (!previousMatchedIDs.isEmpty()) {
                    arguments.setIDs(previousMatchedIDs);
                    System.out.println("QACProcessor: Loaded " + previousMatchedIDs.size() + " matching graph IDs");
                } else {
                    System.out.println("QACProcessor: No valid graph IDs found");
                }
            } else {
                System.out.println("QACProcessor: File does not exist or is empty");
            }
        }
        
        // Record VF2 start time
        long vf2StartTime = System.currentTimeMillis();
        
        // Run VF2 with the new method name and pass the Arguments instance
        vf2.vf2run(arguments.getInFilePath(), arguments.getQueryFile(), vf2OutputFile, arguments.isRerun, arguments);
        
        // Calculate and output VF2 running time
        long vf2EndTime = System.currentTimeMillis();
        double vf2TimeSeconds = (vf2EndTime - vf2StartTime) / 1000.0;
        System.out.println("VF2 Time(s): " + vf2TimeSeconds);
        
        // Extract VF2 result graph IDs from G.txt
        List<Integer> matchedGraphIDs = extractGraphIDs(vf2OutputFile);
        if (!matchedGraphIDs.isEmpty()) {
            arguments.setIDs(matchedGraphIDs);
            System.out.println("SUCCESS: Found " + matchedGraphIDs.size() + " matching graphs");
        } else {
            System.out.println("WARNING: No matching graphs found");
            // Check if G.txt exists and has content
            java.io.File gFile = new java.io.File(vf2OutputFile);
            if (gFile.exists() && gFile.length() > 0) {
                System.out.println("WARNING: File exists but no graph IDs could be extracted");
                System.out.println("         This may indicate a format mismatch");
                // Try to print the first few lines of G.txt for debugging
                try (BufferedReader br = new BufferedReader(new FileReader(vf2OutputFile))) {
                    System.out.println("G.txt content preview:");
                    for (int i = 0; i < 5; i++) {
                        String line = br.readLine();
                        if (line == null) break;
                        System.out.println("    " + line);
                    }
                } catch (IOException e) {
                    System.err.println("Error reading G.txt for preview: " + e.getMessage());
                }
            } else {
                System.out.println("INFO: File is empty or does not exist");
            }
        }
        
        // TED processing section - use TEDPath to store TED results
        try (FileReader queryReader = new FileReader(arguments.getQueryFile());
             FileWriter tedWriter = new FileWriter(arguments.TEDPath)) { // Use TEDPath as TED output

            TEDSProcessor processor = new TEDSProcessor();
            processor.run(reader, tedWriter, arguments, queryReader); // Note: using tedWriter here
        }
    }
    
    /**
     * Extract graph IDs from the VF2 results file
     * @param filePath Path to the VF2 results file
     * @return List of graph IDs that match the query
     */
    private List<Integer> extractGraphIDs(String filePath) {
        List<Integer> graphIDs = new ArrayList<>();
        
        try (BufferedReader reader = new BufferedReader(new FileReader(filePath), 16384)) {
            String line;
            
            // Read through file looking for array pattern [id1, id2, ...]
            while ((line = reader.readLine()) != null) {
                
                if (line.startsWith("[") && line.endsWith("]")) {
                    // Found array line with format [id1, id2, ...]
                    String content = line.substring(1, line.length() - 1);
                    
                    // Handle case of single ID without commas e.g. "[1312]"
                    if (!content.contains(",") && !content.trim().isEmpty()) {
                        try {
                            int singleId = Integer.parseInt(content.trim());
                            graphIDs.add(singleId);
                        } catch (NumberFormatException e) {
                            System.err.println("Error parsing single ID: " + content);
                        }
                    }
                    // Handle comma-separated list e.g. "[456, 457, 522]"
                    else if (content.contains(",")) {
                        String[] idStrings = content.split(", ");
                        for (String idString : idStrings) {
                            try {
                                graphIDs.add(Integer.parseInt(idString.trim()));
                            } catch (NumberFormatException e) {
                                System.err.println("Error parsing ID in array: " + idString);
                            }
                        }
                    }
                    break; // Exit after finding array line
                }
            }
            
        } catch (IOException e) {
            System.err.println("Error reading VF2 results file: " + e.getMessage());
        }
        return graphIDs;
    }
}