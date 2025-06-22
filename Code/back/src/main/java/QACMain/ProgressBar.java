package QACMain;

import java.io.PrintStream;
import java.text.DecimalFormat;

/**
 * A simplified console-based progress bar utility for TEDSProcessor
 */
public class ProgressBar {
    private final PrintStream out;
    private final String taskName;
    private final long total;
    private final int barLength;
    private final char fillChar;
    private final char emptyChar;
    private final DecimalFormat percentFormat;
    
    private long current;
    private long startTime;
    private String lastProgressLine = "";
    
    public ProgressBar(String taskName, long total, int barLength) {
        this.taskName = taskName;
        this.total = total;
        this.barLength = barLength;
        this.fillChar = '█';
        this.emptyChar = '░';
        this.out = System.out;
        this.current = 0;
        this.startTime = System.currentTimeMillis();
        this.percentFormat = new DecimalFormat("##0.0");
    }
    
    /**
     * Update progress to a specific value
     */
    public synchronized void update(long current) {
        this.current = Math.min(current, total);
        display();
    }
    
    /**
     * Set progress to completion
     */
    public synchronized void finish() {
        update(total);
        out.println(); // Move to next line
    }
    
    /**
     * Display the current progress bar
     */
    private void display() {
        if (total <= 0) return;
        
        double percentage = (double) current / total * 100;
        int filled = (int) (barLength * current / total);
        
        StringBuilder bar = new StringBuilder();
        bar.append('\r'); // Return to beginning of line
        bar.append(taskName).append(": ");
        
        // Progress bar
        bar.append('[');
        for (int i = 0; i < barLength; i++) {
            if (i < filled) {
                bar.append(fillChar);
            } else {
                bar.append(emptyChar);
            }
        }
        bar.append(']');
        
        // Percentage
        bar.append(' ').append(percentFormat.format(percentage)).append('%');
        
        // Current/Total
        bar.append(' ').append(current).append('/').append(total);
        
        // ETA
        if (current > 0) {
            long elapsed = System.currentTimeMillis() - startTime;
            long estimated = (long) (elapsed * total / current);
            long remaining = estimated - elapsed;
            
            if (remaining > 0) {
                bar.append(' ').append("ETA: ").append(formatTime(remaining));
            }
        }
        
        // Speed
        if (current > 0) {
            long elapsed = System.currentTimeMillis() - startTime;
            if (elapsed > 0) {
                double speed = (double) current * 1000 / elapsed;
                bar.append(' ').append('(').append(formatSpeed(speed)).append(')');
            }
        }
        
        // Pad with spaces to clear previous line
        int currentLength = bar.length() - 1; // -1 for \r
        int lastLength = lastProgressLine.length();
        if (currentLength < lastLength) {
            for (int i = 0; i < lastLength - currentLength; i++) {
                bar.append(' ');
            }
        }
        
        String progressLine = bar.toString();
        out.print(progressLine);
        out.flush();
        
        lastProgressLine = progressLine.substring(1); // Remove \r for length calculation
    }
    
    /**
     * Format time in human readable format
     */
    private String formatTime(long millis) {
        long seconds = millis / 1000;
        long minutes = seconds / 60;
        long hours = minutes / 60;
        
        if (hours > 0) {
            return String.format("%d:%02d:%02d", hours, minutes % 60, seconds % 60);
        } else if (minutes > 0) {
            return String.format("%d:%02d", minutes, seconds % 60);
        } else {
            return seconds + "s";
        }
    }
    
    /**
     * Format processing speed
     */
    private String formatSpeed(double itemsPerSecond) {
        if (itemsPerSecond >= 1000000) {
            return String.format("%.1fM/s", itemsPerSecond / 1000000);
        } else if (itemsPerSecond >= 1000) {
            return String.format("%.1fK/s", itemsPerSecond / 1000);
        } else {
            return String.format("%.1f/s", itemsPerSecond);
        }
    }
    
    /**
     * Create a progress bar for TEDSProcessor
     */
    public static ProgressBar create(String taskName, long total, int barLength) {
        return new ProgressBar(taskName, total, barLength);
    }
} 