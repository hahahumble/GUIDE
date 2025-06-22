const fs = require('fs-extra')
const {prim} = require("./js/dagre");
const readline = require('readline');

exports.appendLog = function (filename, msg) {
    fs.appendFile(filename, `[${new Date().toLocaleString()}]\n${msg}\n`)
}

exports.readLocalPatternsFile = function (filename) {
    let patterns = []
    try {
        let data = fs.readFileSync(filename).toString()
        let lines = data.split(/\n/).map(line => line = line.trim())
        let nodeCnt = 0, nodeMap = {}, pattern = {}
        lines.forEach(line => {
            if (0 == line.length) {
                if (nodeCnt > 0) {
                    patterns.push(pattern)
                    nodeCnt = 0
                    nodeMap = {}
                    pattern = {}
                }
            }
            else if ('t' == line[0]) {
                if (nodeCnt > 0) {
                    patterns.push(pattern)
                    nodeCnt = 0
                    nodeMap = {}
                    pattern = {}
                }
                let a = line.split(' ')
                pattern.id = parseInt(a[2])
                nodeCnt = 0
                pattern.n = parseInt(a[3])
                pattern.e = []
                pattern.labels = []
            }
            else if ('v' == line[0]) {
                let a = line.split(' ')
                nodeMap[parseInt(a[1])] = nodeCnt++
                pattern.labels.push(parseInt(a[2]))
            }
            else if ('e' == line[0]) {
                let a = line.split(' ')
                pattern.e.push([
                    nodeMap[parseInt(a[1])],
                    nodeMap[parseInt(a[2])]
                ])
            }
        });
        if (nodeCnt > 0)
            patterns.push(pattern)
        return patterns
    }
    catch (ignore) {
        return []
    }
}

exports.readLocalPatternsFileV3 = function (path) {
    return new Promise((resolve, reject) => {
        try {
            const patterns = [];
            const datastore = '';
            
            const fileStream = fs.createReadStream(path);
            const rl = readline.createInterface({
                input: fileStream,
                crlfDelay: Infinity
            });

            let lineCount = 0;
            
            rl.on('line', (line) => {
                lineCount++;
                if (!line.trim()) return;
                
                try {
                    let pattern = {};
                    let data = line.split(' ');
                    
                    if (!data || data.length < 2) {
                        console.log(`Invalid line format at line ${lineCount}:`, line);
                        return;
                    }

                    pattern.id = data[0];
                    pattern.type = parseInt(data[1]);
                    pattern.isDefault = false;
                    
                    pattern.n = [];
                    pattern.e = [];
                    pattern.labels = [];
                    
                    let nodeNum = parseInt(data[2]);
                    for (let i = 0; i < nodeNum; i++) {
                        pattern.n.push(i);
                        pattern.labels.push('');
                    }
                    
                    let edgeStart = 3;
                    let edgeNum = parseInt(data[edgeStart]);
                    for (let i = 0; i < edgeNum; i++) {
                        let source = parseInt(data[edgeStart + 1 + i * 2]);
                        let target = parseInt(data[edgeStart + 2 + i * 2]);
                        pattern.e.push({
                            source: source,
                            target: target
                        });
                    }
                    
                    patterns.push(pattern);
                } catch (err) {
                    console.log(`Error processing line ${lineCount}:`, err);
                }
            });

            rl.on('close', () => {
                console.log("Total lines processed:", lineCount);
                console.log("Patterns found:", patterns.length);
                resolve({
                    datastore: datastore,
                    patterns: patterns
                });
            });

            rl.on('error', (err) => {
                console.error("Error reading file:", err);
                reject(err);
            });
            
        } catch (err) {
            console.error("Error setting up file reading:", err);
            reject(err);
        }
    });
};

exports.readQueryResults = function (filename) {
    try {
        const data = fs.readFileSync(filename, 'utf8');
        const lines = data.trim().split('\n');
        const results = [];
        let currentGraph = null;
        
        lines.forEach(line => {
            line = line.trim();
            if (line.startsWith('Final t #')) {
                if (currentGraph) {
                    results.push(currentGraph);
                }
                currentGraph = {
                    id: line.split('#')[1].split('*')[0].trim(),
                    nodes: [],
                    edges: []
                };
            } else if (line.startsWith('v ')) {
                const [_, id, label] = line.split(' ');
                currentGraph.nodes.push({
                    id: id,
                    label: label
                });
            } else if (line.startsWith('e ')) {
                const [_, source, target, label] = line.split(' ');
                currentGraph.edges.push({
                    source: source,
                    target: target,
                    label: label
                });
            }
        });
        
        if (currentGraph) {
            results.push(currentGraph);
        }
        
        return {
            success: true,
            results: results
        };
    } catch (error) {
        return {
            success: false,
            error: error.message
        };
    }
};