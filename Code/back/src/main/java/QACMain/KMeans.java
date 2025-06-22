package QACMain;

import java.util.HashSet;
import java.util.Random;

public class KMeans {

    /***********************************************************************
     * Data structures
     **********************************************************************/

    private int k;
    private double[][] points;

    private int iterations;
    private boolean pp;
    private double epsilon;
    private boolean useEpsilon;

    private boolean L1norm;

    private int m;
    private int n;

    private double[][] centroids;
    private int[] assignment;
    private double WCSS;

    private long start;
    private long end;

    /***********************************************************************
     * Constructors
     **********************************************************************/

    /**
     * Empty constructor is private to ensure that clients have to use the
     * Builder inner class to create a KMeans object.
     */
    public KMeans() {}

    /**
     * The proper way to construct a KMeans object: from an inner class object.
     * @param builder See inner class named Builder
     */
    public KMeans(Builder builder) {
        start = System.currentTimeMillis();

        k = builder.k;
        points = builder.points;
        iterations = builder.iterations;
        pp = builder.pp;
        epsilon = builder.epsilon;
        useEpsilon = builder.useEpsilon;
        L1norm = builder.L1norm;

        m = points.length;
        n = points[0].length;

        run();

        end = System.currentTimeMillis();
    }

    /**
     * Builder class for constructing KMeans objects.
     *
     * For descriptions of the fields in this (inner) class, see outer class.
     */
    public static class Builder {

        private final int k;
        public final double[][] points;

        private int iterations = 10;
        private boolean pp = true;
        private double epsilon = .001;
        private boolean useEpsilon = true;
        private boolean L1norm = true;

        /**
         * Sets required parameters and checks that are a sufficient # of distinct
         * points to run KMeans.
         */
        public Builder(int k, double[][] points) {
            if (k > points.length) throw new IllegalArgumentException(
                "Required: # of points >= # of clusters"
            );

            HashSet<double[]> hashSet = new HashSet<>(k);
            int distinct = 0;

            for (int i = 0; i < points.length; i++) {
                if (!hashSet.contains(points[i])) {
                    distinct++;
                    if (distinct >= k) break;
                    hashSet.add(points[i]);
                }
            }

            if (distinct < k) throw new IllegalArgumentException(
                "Required: # of distinct points >= # of clusters"
            );

            this.k = k;
            this.points = points;
        }

        /**
         * Sets optional parameter. Default value is 50.
         */
        public Builder iterations(int iterations) {
            if (iterations < 1) throw new IllegalArgumentException(
                "Required: non-negative number of iterations. Ex: 50"
            );
            this.iterations = iterations;
            return this;
        }

        /**
         * Sets optional parameter. Default value is true.
         */
        public Builder pp(boolean pp) {
            this.pp = pp;
            return this;
        }

        /**
         * Sets optional parameter. Default value is .001.
         */
        public Builder epsilon(double epsilon) {
            if (epsilon < 0.0) throw new IllegalArgumentException(
                "Required: non-negative value of epsilon. Ex: .001"
            );

            this.epsilon = epsilon;
            return this;
        }

        /**
         * Sets optional parameter. Default value is true.
         */
        public Builder useEpsilon(boolean useEpsilon) {
            this.useEpsilon = useEpsilon;
            return this;
        }

        /**
         * Sets optional parameter. Default value is true
         */
        public Builder useL1norm(boolean L1norm) {
            this.L1norm = L1norm;
            return this;
        }

        /**
         * Build a KMeans object
         */
        public KMeans build() {
            return new KMeans(this);
        }
    }

    /***********************************************************************
     * KMeans clustering algorithm
     **********************************************************************/

    /**
     * Run KMeans algorithm
     */
    public void run() {
        double bestWCSS = Double.POSITIVE_INFINITY;
        double[][] bestCentroids = new double[0][0];
        int[] bestAssignment = new int[0];

        for (int n = 0; n < iterations; n++) {
            cluster();

            if (WCSS < bestWCSS) {
                bestWCSS = WCSS;
                bestCentroids = centroids;
                bestAssignment = assignment;
            }
        }

        WCSS = bestWCSS;
        centroids = bestCentroids;
        assignment = bestAssignment;
    }

    /**
     * Perform KMeans clustering algorithm once.
     */
    public void cluster() {
        chooseInitialCentroids();
        WCSS = Double.POSITIVE_INFINITY;
        double prevWCSS;
        do {
            assignmentStep();
            updateStep();
            prevWCSS = WCSS;
            calcWCSS();
        } while (!stop(prevWCSS));
    }

    /**
     * Assigns to each data point the nearest centroid.
     */
    public void assignmentStep() {
        assignment = new int[m];

        double tempDist;
        double minValue;
        int minLocation;

        for (int i = 0; i < m; i++) {
            minLocation = 0;
            minValue = Double.POSITIVE_INFINITY;
            for (int j = 0; j < k; j++) {
                tempDist = distance(points[i], centroids[j]);
                if (tempDist < minValue) {
                    minValue = tempDist;
                    minLocation = j;
                }
            }

            assignment[i] = minLocation;
        }
    }

    /**
     * Updates the centroids.
     */
    public void updateStep() {
        for (int i = 0; i < k; i++) for (
            int j = 0;
            j < n;
            j++
        ) centroids[i][j] = 0;

        int[] clustSize = new int[k];

        for (int i = 0; i < m; i++) {
            clustSize[assignment[i]]++;
            for (int j = 0; j < n; j++) centroids[assignment[i]][j] +=
                points[i][j];
        }

        HashSet<Integer> emptyCentroids = new HashSet<Integer>();

        for (int i = 0; i < k; i++) {
            if (clustSize[i] == 0) emptyCentroids.add(i);
            else for (int j = 0; j < n; j++) centroids[i][j] /= clustSize[i];
        }

        if (emptyCentroids.size() != 0) {
            HashSet<double[]> nonemptyCentroids = new HashSet<double[]>(
                k - emptyCentroids.size()
            );
            for (int i = 0; i < k; i++) if (
                !emptyCentroids.contains(i)
            ) nonemptyCentroids.add(centroids[i]);

            Random r = new Random();
            for (int i : emptyCentroids) while (true) {
                int rand = r.nextInt(points.length);
                if (!nonemptyCentroids.contains(points[rand])) {
                    nonemptyCentroids.add(points[rand]);
                    centroids[i] = points[rand];
                    break;
                }
            }
        }
    }

    /***********************************************************************
     * Choose initial centroids
     **********************************************************************/
    /**
     * Uses either plusplus (KMeans++) or a basic randoms sample to choose initial centroids
     */
    public void chooseInitialCentroids() {
        if (pp) plusplus();
        else basicRandSample();
    }

    /**
     * Randomly chooses (without replacement) k data points as initial centroids.
     */
    public void basicRandSample() {
        centroids = new double[k][n];
        double[][] copy = points;

        Random gen = new Random();

        int rand;
        for (int i = 0; i < k; i++) {
            rand = gen.nextInt(m - i);
            for (int j = 0; j < n; j++) {
                centroids[i][j] = copy[rand][j];
                copy[rand][j] = copy[m - 1 - i][j];
            }
        }
    }

    public void basicRandSample2() {
        centroids = new double[k][n];
        double[][] copy = points;

        int rand;
        for (int i = 0; i < k; i++) {
            rand = (int) (m - i) / 2;
            for (int j = 0; j < n; j++) {
                centroids[i][j] = copy[rand][j];
                copy[rand][j] = copy[m - 1 - i][j];
            }
        }
    }

    /**
     * Randomly chooses (without replacement) k data points as initial centroids using a
     * weighted probability distribution (proportional to D(x)^2 where D(x) is the
     * distance from a data point to the nearest, already chosen centroid).
     */

    public void plusplus() {
        centroids = new double[k][n];
        double[] distToClosestCentroid = new double[m];
        double[] weightedDistribution = new double[m];

        Random gen = new Random();
        int choose = 0;

        for (int c = 0; c < k; c++) {
            if (c == 0) choose = gen.nextInt(m);
            else {
                for (int p = 0; p < m; p++) {
                    double tempDistance = Distance.L2(
                        points[p],
                        centroids[c - 1]
                    );

                    if (c == 1) distToClosestCentroid[p] = tempDistance;
                    else {
                        if (
                            tempDistance < distToClosestCentroid[p]
                        ) distToClosestCentroid[p] = tempDistance;
                    }

                    if (p == 0) weightedDistribution[0] =
                        distToClosestCentroid[0];
                    else weightedDistribution[p] =
                        weightedDistribution[p - 1] + distToClosestCentroid[p];
                }

                double rand = gen.nextDouble();
                for (int j = m - 1; j > 0; j--) {
                    if (
                        rand >
                        weightedDistribution[j - 1] /
                        weightedDistribution[m - 1]
                    ) {
                        choose = j;
                        break;
                    } else choose = 0;
                }
            }

            for (int i = 0; i < n; i++) centroids[c][i] = points[choose][i];
        }
    }

    /***********************************************************************
     * Cutoff to stop clustering
     **********************************************************************/

    /**
     * Calculates whether to stop the run
     * @param prevWCSS error from previous step in the run
     * @return
     */
    public boolean stop(double prevWCSS) {
        if (useEpsilon) return epsilonTest(prevWCSS);
        else return prevWCSS == WCSS;
    }

    /**
     * Signals to stop running KMeans when the marginal improvement in WCSS
     * from the last step is small.
     * @param prevWCSS error from previous step in the run
     * @return
     */
    public boolean epsilonTest(double prevWCSS) {
        return epsilon > 1 - (WCSS / prevWCSS);
    }

    /***********************************************************************
     * Utility functions
     **********************************************************************/
    /**
     * Calculates distance between two n-dimensional points.
     * @param x
     * @param y
     * @return
     */
    public double distance(double[] x, double[] y) {
        return L1norm ? Distance.L1(x, y) : Distance.L2(x, y);
    }

    public static class Distance {

        /**
         * L1 norm: distance(X,Y) = sum_i=1:n[|x_i - y_i|]
         * <P> Minkowski distance of order 1.
         * @param x
         * @param y
         * @return
         */
        public static double L1(double[] x, double[] y) {
            if (x.length != y.length) throw new IllegalArgumentException(
                "dimension error"
            );
            double dist = 0;
            for (int i = 0; i < x.length; i++) dist += Math.abs(x[i] - y[i]);
            return dist;
        }

        /**
         * L2 norm: distance(X,Y) = sqrt(sum_i=1:n[(x_i-y_i)^2])
         * <P> Euclidean distance, or Minkowski distance of order 2.
         * @param x
         * @param y
         * @return
         */
        public static double L2(double[] x, double[] y) {
            if (x.length != y.length) throw new IllegalArgumentException(
                "dimension error"
            );
            double dist = 0;
            for (int i = 0; i < x.length; i++) dist += Math.abs(
                (x[i] - y[i]) * (x[i] - y[i])
            );
            return dist;
        }
    }

    /**
     * Calculates WCSS (Within-Cluster-Sum-of-Squares), a measure of the clustering's error.
     */
    public void calcWCSS() {
        double WCSS = 0;
        int assignedClust;

        for (int i = 0; i < m; i++) {
            assignedClust = assignment[i];
            WCSS += distance(points[i], centroids[assignedClust]);
        }

        this.WCSS = WCSS;
    }

    /***********************************************************************
     * Accessors
     ***********************************************************************/
    public int[] getAssignment() {
        return assignment;
    }

    public double[][] getCentroids() {
        return centroids;
    }

    public double getWCSS() {
        return WCSS;
    }

    public String getTiming() {
        return "KMeans++ took: " + (double) (end - start) / 1000.0 + " seconds";
    }
    /***********************************************************************
     * Unit testing
     **********************************************************************/
    /*
   public static void main(String args[]) throws IOException {


      String data = "TestData.csv";
      int numPoints = 3000;
      int dimensions = 2;
      int k = 4;
      double[][] points = CSVreader.read(data, numPoints, dimensions);


      final long startTime = System.currentTimeMillis();
      KMeans clustering = new KMeans.Builder(k, points)
                                    .iterations(50)
                                    .pp(true)
                                    .epsilon(.001)
                                    .useEpsilon(true)
                                    .build();
      final long endTime = System.currentTimeMillis();


      final long elapsed = endTime - startTime;
      System.out.println("Clustering took " + (double) elapsed/1000 + " seconds");
      System.out.println();


      double[][] centroids = clustering.getCentroids();
      double WCSS          = clustering.getWCSS();



      for (int i = 0; i < k; i++)
         System.out.println("(" + centroids[i][0] + ", " + centroids[i][1] + ")");
      System.out.println();

      System.out.println("The within-cluster sum-of-squares (WCSS) = " + WCSS);
      System.out.println();



   }
   */
}
