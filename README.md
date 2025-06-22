# GUIDE: Towards Progressive Visual Query Autocompletion for Graph Databases

## Abstract
The growing prevalence of graph data in domains such as bioinformatics necessitates intuitive query formulation methods. While visual query interfaces mitigate the inherent complexity of graph query languages, constructing queries remains challenging. Current Graph Query Autocompletion (GQAC) systems assist users in iteratively building query graphs through visual interfaces by autocompleting partial queries, thereby reducing cognitive load. However, they suffer critical limitations such as tedious incremental suggestions (1-2 edges per iteration) and reliance on days-long preprocessing that fails for dynamic data. We propose GUIDE, an online visual query autocompletion framework that eliminates preprocessing by operating directly on the underlying database. Unlike existing systems constrained to suggesting small substructures, GUIDE generates suggestions of arbitrary size. This approach significantly reduces iterative interactions and cognitive burden, accelerating complex query formulation. We further introduce novel optimizations to ensure efficient autocompletion. Extensive experiments demonstrate GUIDE’s significant superiority over traditional techniques.

## Requirement
- JDK 1.8
- Maven 3.6.0+
- Node.js

## Web Interface

Step 1: Start the web server:
```bash
cd HRTFYP
npm install
node app.js
```

Step 2: Open your browser and navigate to `http://localhost:3000`

Step 3: Use the web interface to:
- Draw or use default pattern as query graph
- Set search parameters
- View and choose one of the results

## Command Line Usage

1. Run with default parameters:
```bash
java -jar target/my-java-project-1.0-SNAPSHOT.jar
```

2. Specifying input files and query files:
```bash
java -jar target/my-java-project-1.0-SNAPSHOT.jar AIDS10K_normalized.txt query.txt
```

3. Full parameter run:
```bash
java -jar target/my-java-project-1.0-SNAPSHOT.jar AIDS10K_normalized.txt query.txt 10 7 8 topk Outputs
```

## Output Files

- `G.txt`: Graph matching results
- `result.txt`: Final Top-k suggestions
