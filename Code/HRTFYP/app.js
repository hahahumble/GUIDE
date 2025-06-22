const express = require('express')
const busboy = require('connect-busboy')
const app = express()
const fs = require('fs-extra')
const {spawn, spawnSync} = require('child_process')
const utilities = require('./utilities')
const graphlib = require('graphlib');
const {constructGraph, subgraphIsomorphism} = require("./utilities");
const {Graph} = graphlib;
const path = require('path');
const { exec } = require('child_process');

// File to store isRerun state
const RERUN_STATE_FILE = path.join(__dirname, 'Outputs', 'rerun_state.txt');

// Atomic label mapping configuration for datasets
const DATASET_ATOMIC_LABELS = {
    'AIDS': [
        "C", "O", "Cu", "N", "S", "P", "Cl", "Zn", "B", "Br", "Co", "Mn", "As", "Al", "Ni", "Se",
        "Si", "V", "Sn", "I", "F", "Li", "Sb", "Fe", "Pd", "Hg", "Bi", "Na", "Ca", "Ti", "Ho", "Ge",
        "Pt", "Ru", "Rh", "Cr", "Ga", "K", "Ag", "Au", "Tb", "Ir", "Te", "Mg", "Pb", "W", "Cs", "Mo",
        "Re", "Cd", "Os", "Pr", "Nd", "Sm", "Gd", "Yb", "Er", "U", "Tl", "Ac"
    ],
    'emolecule': [
        "Cs", "Cu", "Yb", "Cl", "Pt", "Pr", "Co", "Cr", "Li", "Cd", "Ce", "Hg", "Hf", "La", "Lu",
        "Pd", "Tl", "Tm", "Ho", "Pb", "*", "Ti", "Te", "Dy", "Ta", "Os", "Mg", "Tb", "Au", "Se",
        "F", "Sc", "Fe", "In", "Si", "B", "C", "As", "Sn", "N", "Ba", "O", "Eu", "H", "Sr", "I", "Mo",
        "Mn", "K", "Ir", "Er", "Ru", "Ag", "W", "V", "Ni", "P", "S", "Nb",
        "Y", "Na", "Sb", "Al", "Ge", "Rb", "Re", "Gd", "Ga", "Br", "Rh", "Ca", "Bi", "Zn", "Zr",
        "R#", "R","X","R1","A","U", "Ar", "Kr", "Xe", "e", ".", "Tc",  "Mu", "Mu-", "He", "Ps", "At",
        "Po", "Be", "Ne","Rn", "Fr", "Ra", "Ac", "Rf", "Db", "Sg","Bh", "Hs",  "Mt",
        "Ds", "Rg", "Nd","Pm",  "Sm", "Th", "Pa", "Np","Pu", "Am", "Cm", "Bk",
        "Cf","Es", "Fm", "Md", "No", "Lr","0", "Uub", "R2", "R3", "R4", "D", "R5", "ACP"
    ],
    'pubchem': [
        "H", "C", "O", "N", "Cl", "S", "F", "P", "Br", "I", "Na", "Si",
        "As", "Hg", "Ca", "K", "B", "Sn", "Se", "Al", "Fe", "Mg", "Zn", "Pb", "Co", "Cu",
        "Cr", "Mn", "Sb", "Cd", "Ni", "Be", "Ag", "Li", "Tl", "Sr", "Bi", "Ce", "Ba", "U", "Ge",
        "Pt", "Te", "V", "Zr", "Cs", "Au", "Mo", "W", "La", "Ti", "Rh", "Lu", "Pd", "In", "Eu", "Ga",
        "Pr", "Ho", "Th", "Ta", "Tc", "Tb", "Ir", "Nd", "Nb", "Rb", "Kr", "Yb", "Cm", "Pu", "Cf", "Hf",
        "He", "Pa", "Tm", "Pm", "Po", "Xe", "Dy", "Os", "Md", "Sc", "Ar", "At", "Sm", "Er", "Ru",
        "Es", "Ac", "Am", "Ne", "Y", "Re", "Gd", "No", "Rn", "Np", "Fm", "Bk", "Lr"
    ],
    'default': [
        "C", "N", "O", "H", "S", "P", "F", "Cl", "Br", "I", "Na", "Si"
    ]
};

// Add label frequency configuration for datasets
const DATASET_LABEL_FREQUENCY = {
    'AIDS': [
        // Label indices sorted by frequency (based on your statistics)
        0, 1, 3, 4, 6, 21, 5, 9, 16, 20, 8, 28, 19, 10, 2, 25, 14, 33, 15, 12,
        34, 48, 26, 35, 11, 46, 32, 38, 7, 27, 42, 37, 36, 45, 23, 24, 40, 44,
        49, 59, 13, 17, 22, 30, 39, 43, 47, 51, 52, 53, 57
    ],
    'emolecule': [
        // Label indices sorted by frequency (based on your emolecule statistics)
        36, 41, 39, 3, 30, 57, 68, 56, 45, 60, 34, 43, 35, 48, 38, 29, 8, 26, 32,
        15, 47, 0, 1, 4, 55, 71, 72, 6, 25, 52, 63, 69, 70, 7, 19, 51, 61, 65,
        21, 40, 44, 46, 49, 53, 62
    ],
    'pubchem': [
        // Label indices sorted by frequency (based on your pubchem23238 statistics)
        1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19,
        20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37,
        38, 39, 40, 41, 42, 43, 44, 45, 46, 47, 48, 49, 50, 51, 52, 53, 54, 55,
        56, 57, 58, 59, 60, 61, 62, 63, 64, 65, 66, 67, 68, 69, 70, 71, 72, 73,
        74, 75, 76, 77, 78, 79, 80, 81, 82, 83, 84, 85, 86, 87, 88, 89, 90, 91,
        92, 93, 94, 95, 96, 97, 98, 99,0
    ],
    'default': [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11] // Default order by index
};

// Current dataset type
let currentDataset = 'default';

// Detect dataset type
function detectDatasetType(datasetName) {
    if (!datasetName) return 'default';
    
    const name = datasetName.toLowerCase();
    if (name.includes('aids')) {
        return 'AIDS';
    } else if (name.includes('emolecule') || name.includes('emol') || name.includes('emodulo')) {
        return 'emolecule';
    } else if (name.includes('pubchem')) {
        return 'pubchem';
    }
    return 'default';
}

// Get atomic label mapping for current dataset
function getCurrentAtomicLabels() {
    const labels = DATASET_ATOMIC_LABELS[currentDataset];
    const mapping = {};
    labels.forEach((label, index) => {
        mapping[label] = index;
    });
    return mapping;
}

// Get reverse mapping for current dataset
function getCurrentReverseAtomicLabels() {
    const labels = DATASET_ATOMIC_LABELS[currentDataset];
    const reverseMapping = {};
    labels.forEach((label, index) => {
        reverseMapping[index] = label;
    });
    return reverseMapping;
}

// Function to read isRerun state from file
function getIsRerun() {
    try {
        if (fs.existsSync(RERUN_STATE_FILE)) {
            const state = fs.readFileSync(RERUN_STATE_FILE, 'utf8').trim();
            return state === 'true';
        }
    } catch (error) {
        console.error('Error reading rerun state file:', error);
    }
    return false; // Default to false if file doesn't exist or there's an error
}

// Function to write isRerun state to file
function setIsRerun(value) {
    try {
        fs.writeFileSync(RERUN_STATE_FILE, value.toString(), 'utf8');
        // Reduce verbose logging
        if (value) {
            console.log("Reuse mode enabled for next query");
        }
    } catch (error) {
        console.error('Error writing rerun state file:', error);
    }
}

// Initialize isRerun to false always on startup
let isRerun = false;
// Write false to the file to ensure consistency
setIsRerun(false);
console.log(`Initialized isRerun to false on startup`);

app.use(busboy())
app.use(express.json());  // Handle JSON request body
app.use(express.urlencoded({extended: true}))

app.use('/css', express.static('css'))
app.use('/js', express.static('js'))
app.use('/fonts', express.static('fonts'))
app.use('/img', express.static('img'))

app.get('/', (req, res) => res.sendFile(`${__dirname}/AVATAR.html`))
app.get('/v2', (req, res) => res.sendFile(`${__dirname}/AVATAR_v2.html`))
app.get('/v3', (req, res) => res.sendFile(`${__dirname}/AVATAR_v3.html`))
app.get('/catapult', (req, res) => res.sendFile(`${__dirname}/DAVINCI.html`))
app.get('/midas', (req, res) => res.sendFile(`${__dirname}/MIDAS.html`))
app.get('/hyperq', (req, res) => res.sendFile(`${__dirname}/HyperQ.html`))
app.get('/HyperQ', (req, res) => res.sendFile(`${__dirname}/HyperQ.html`))

app.get('/datastores', (req, res) => {
    res.json(fs.readdirSync(`${__dirname}/data`)
        .filter(name => fs.existsSync(`${__dirname}/data/${name}/graph`))
        .map(name => {
            return {
                name: name,
                status: fs.existsSync(`${__dirname}/data/${name}/patterns.json`)
                && fs.existsSync(`${__dirname}/data/${name}/labels.json`)
                && fs.existsSync(`${__dirname}/data/${name}/limits.json`)
                    ? 'Ready'
                    : fs.existsSync(`${__dirname}/data/${name}/error`)
                        ? `Failed (${fs.readFileSync(`${__dirname}/data/${name}/error`)})`
                        : fs.existsSync(`${__dirname}/data/${name}/progress`)
                            ? `Preparing (${fs.readFileSync(`${__dirname}/data/${name}/progress`)}%)`
                            : 'Pending'
            }
        }))
})

app.post('/datastores', (req, res) => {
    req.busboy.on('field', (key, value) => {
        let name = value
        try {
            fs.mkdirSync(`${__dirname}/data/${name}`)
        } catch (err) {
            if ('EEXIST' == err.code) {
                res.json({
                    success: false,
                    message: 'Duplicate datastore name or invalid datastore name!'
                })
                return
            } else if ('ENOENT' == err.code) {
                res.json({
                    success: false,
                    message: 'Invalid datastore name!'
                })
                return
            }
        }
        
        req.busboy.on('file', (fieldname, file, filename) => {
            // Directly save uploaded graph data file as input file
            let inFilePath = `${__dirname}/data/${name}/graph`
            let fstream = fs.createWriteStream(inFilePath)
            file.pipe(fstream)
            fstream.on('close', function () {
                // Save file path for subsequent queries
                fs.writeFileSync(`${__dirname}/data/${name}/inFilePath`, inFilePath)
                res.json({success: true})
            })
        })
    })
    req.pipe(req.busboy)
})

app.get('/datastores/:name/labels', (req, res) => {
    let name = req.params['name']
    let content = fs.readFileSync(`${__dirname}/data/${name}/labels.json`, 'utf8')
    res.json(JSON.parse(content))
})

app.get('/datastores/:name/limits', (req, res) => {
    let name = req.params['name']
    let content = fs.readFileSync(`${__dirname}/data/${name}/limits.json`, 'utf8')
    res.json(JSON.parse(content))
})

app.get('/datastores/:name/infos', (req, res) => {
    let name = req.params['name']
    let content = fs.readFileSync(`${__dirname}/data/${name}/GraphInfo.json`, 'utf8')
    res.json(JSON.parse(content))
})

app.get('/datastores/:name/patterns', (req, res) => {
    let name = req.params['name']
    let num = req.query['num']
    let minSize = req.query['minSize']
    let maxSize = req.query['maxSize']
    let davinci = spawn('./Davinci', [name, '-step2', num, minSize, maxSize])
    davinci.stdout.on('data', (data) => {
        utilities.appendLog(`${__dirname}/data/${name}/patterns${num}-${minSize}-${maxSize}_log`, `stdout: ${data}`)
    })
    davinci.stderr.on('data', (data) => {
        utilities.appendLog(`${__dirname}/data/${name}/patterns${num}-${minSize}-${maxSize}_log`, `stderr: ${data}`)
    })
    davinci.on('close', (code) => {
        utilities.appendLog(`${__dirname}/data/${name}/patterns${num}-${minSize}-${maxSize}_log`, `child process exited with code ${code}`)
        let content = fs.readFileSync(`${__dirname}/data/${name}/patterns${num}-${minSize}-${maxSize}.json`, 'utf8')
        res.json(JSON.parse(content))
    })
})

app.get('/datastores/:name/patterns_catapult', (req, res) => {
    console.log("In patterns_catapult")
    let name = req.query['name']
    let num = req.query['num']
    let minSize = req.query['minSize']
    let maxSize = req.query['maxSize']

    console.log("name=" + name + " num=" + num + " minSize=" + minSize + " maxSize=" + maxSize)
    console.log("spawn catapult")
    let catapult = spawn('./dist/davinci', [name, minSize, maxSize, num])
    catapult.stdout.on('data', (data) => {
        utilities.appendLog(`${__dirname}/dist/data/${name}/patterns${num}-${minSize}-${maxSize}_log`, `stdout: ${data}`)
    })
    catapult.stderr.on('data', (data) => {
        utilities.appendLog(`${__dirname}/dist/data/${name}/patterns${num}-${minSize}-${maxSize}_log`, `stderr: ${data}`)
    })
    catapult.on('close', (code) => {
        utilities.appendLog(`${__dirname}/dist/data/${name}/patterns${num}-${minSize}-${maxSize}_log`, `child process exited with code ${code}`)
        //let content = fs.readFileSync(`${__dirname}/dist/data/${name}/patterns${num}-${minSize}-${maxSize}.json`, 'utf8')
        console.log("done generating patterns")
        res.json({success: true})
    })
    //res.json({ success: true })
})

app.get('/datastores/:name/patterns_davinci', (req, res) => {
//app.post('/datastores/:name/patterns_davinci', (req, res) => {
    let name = req.query['labelType']
    let num = req.query['num']
    let minSize = req.query['minSize']
    let maxSize = req.query['maxSize']
    let file = req.query['file']

    console.log(name)
    console.log(file)

//    let davinci = spawn('./dist/davinci', [name, minSize, maxSize, num])
//    davinci.stdout.on('data', (data) => {
//        utilities.appendLog(`${__dirname}/data/${name}/patterns${num}-${minSize}-${maxSize}_log`, `stdout: ${data}`)
//    })
//    davinci.stderr.on('data', (data) => {
//        utilities.appendLog(`${__dirname}/data/${name}/patterns${num}-${minSize}-${maxSize}_log`, `stderr: ${data}`)
//    })
//    davinci.on('close', (code) => {
//        console.log("Done generating patterns using DAVINCI.exe")
//        //utilities.appendLog(`${__dirname}/data/${name}/patterns${num}-${minSize}-${maxSize}_log`, `child process exited with code ${code}`)
//        //let content = fs.readFileSync(`${__dirname}/data/${name}/patterns${num}-${minSize}-${maxSize}.json`, 'utf8')
//        //res.json(JSON.parse(content))

//        let patterns = utilities.readLocalPatternsFile(`${__dirname}/data/${name}.txt`)
//        console.log("Done readLocalPatternsFile")
//            if (patterns.length > 0)
//                res.json({
//                    success: true,
//                    patterns_davinci: patterns
//                })
//            else
//                res.json({
//                    success: false,
//                    message: 'Error'
//                })

//        //let name = new Date().getTime()
//        //let fstream = fs.createWriteStream(`${__dirname}/data/${name}.txt`)
//        //let readStream = fs.createReadStream(file)
//        //file.pipe(fstream)
//        //fstream.on('close', function () {
//        //    let patterns = utilities.readLocalPatternsFile(`${__dirname}/data/${name}.txt`)
//        //    if (patterns.length > 0)
//        //        res.json({
//        //            success: true,
//        //            patterns: patterns
//        //        })
//        //    else
//        //        res.json({
//        //            success: false,
//        //            message: 'Error'
//        //        })
//        //})
//        //req.pipe(readStream)
//    })
    res.send(davinci)
})

app.post('/load', (req, res) => {
    req.busboy.on('file', (fieldname, file, filename) => {
        let name = new Date().getTime()
        let fstream = fs.createWriteStream(`${__dirname}/data/${name}.txt`)
        file.pipe(fstream)
        fstream.on('close', function () {
            let patterns = utilities.readLocalPatternsFile(`${__dirname}/data/${name}.txt`)
            if (patterns.length > 0)
                res.json({
                    success: true,
                    patterns: patterns
                })
            else
                res.json({
                    success: false,
                    message: 'Error'
                })
        })
    })
    req.pipe(req.busboy)
})


app.post('/query_process', (req, res) => {
    try {
        console.log("Starting query process...");
        
        // Read current isRerun state from file
        isRerun = getIsRerun();
        
        const numberofpatterns = req.body.numberofpatterns;
        const minnode = req.body.minnode;
        const maxnode = req.body.maxnode;
        const queryId = req.body.queryId;
        
        // Read selected database and map to actual filename
        const selectedDb = fs.readFileSync(`${__dirname}/data/selected_database.txt`, 'utf8').trim();
        const dbMapping = {
            '1': 'AIDS10K.txt',      // Display as AIDS10K
            '2': 'emodulo.txt',      // Display as emolecul, but actual filename remains emodulo.txt
            '3': 'pubchem23238'      // Display as pubchem23238
        };
        const actualDbName = dbMapping[selectedDb];
        
        // Select different jar files based on database type
        let jarFileName;
        if (selectedDb === '3') { // pubchem database
            jarFileName = 'pubchem-my-java-project-1.0-SNAPSHOT.jar'; // Special jar file for pubchem
            console.log("Using pubchem-specific jar file for pubchem database");
        } else {
            jarFileName = 'my-java-project-1.0-SNAPSHOT.jar'; // Original jar file
            console.log("Using original jar file for non-pubchem database");
        }
        
        // Use original path format, Java code will automatically add data/ prefix
        const dbPath = actualDbName;
        console.log("Database path:", dbPath);
        console.log("Selected jar file:", jarFileName);
        
        // Ensure query file exists
        const queryFilePath = 'query.txt';
        if (!fs.existsSync(path.join(__dirname, 'data', queryFilePath))) {
            throw new Error('Query file not found: ' + queryFilePath);
        }
        
        console.log("Search mode:", isRerun ? "Using previous matches" : "Full database search");
        
        console.log("Spawning java process...");
        
        // Construct Java argument array, reuse is not output directory, should be in args[6] position
        const javaArgs = [
            '-jar',
            jarFileName,                     // Use jar file based on database type
            dbPath,                          // args[0]: Database path
            queryFilePath,                   // args[1]: Query file path
            numberofpatterns.toString(),     // args[2]: Mode count
            minnode.toString(),              // args[3]: Minimum node count
            maxnode.toString(),              // args[4]: Maximum node count
            'Outputs'                        // args[5]: Output directory (fixed to Outputs)
        ];
        
        // Add reuse flag to args[6] position
        if (isRerun) {
            javaArgs.push('reuse');          // args[6]: reuse flag
        }
        
        console.log("Java args:", javaArgs);
        
        // Create Java process, restore original working directory
        const javaProcess = spawn('java', javaArgs);
        
        let vf2Time = null;
        let totalTime = null;
        
        // Handle Java process output
        javaProcess.stdout.on('data', (data) => {
            const output = data.toString();
            console.log('Java process output:', output);
            
            // Parse VF2 time
            const vf2Match = output.match(/VF2 Time\(s\): ([\d.]+)/);
            if (vf2Match) {
                vf2Time = parseFloat(vf2Match[1]);
            }
            
            // Parse total time
            const totalMatch = output.match(/Time\(s\): ([\d.]+)/);
            if (totalMatch) {
                totalTime = parseFloat(totalMatch[1]);
            }
        });
        
        javaProcess.stderr.on('data', (data) => {
            console.error('Java process error:', data.toString());
        });
        
        javaProcess.on('close', (code) => {
            console.log(`Java process exited with code ${code}`);
            
            try {
                if (code === 0) {
                    isRerun = true;
                    setIsRerun(true);
                }
                
                // Always read results from Outputs directory
                const resultDir = 'Outputs';
                const resultPath = path.join(__dirname, resultDir, 'result.txt');
                console.log(`Reading results from: ${resultPath}`);
                
                const results = fs.readFileSync(resultPath, 'utf8');
                const parsedResults = parseResultFile(results);
                
                console.log(`Parsed ${parsedResults.length} results from ${resultDir}/result.txt`);
                
                // Return results, including time information
                res.json({
                    success: true,
                    results: parsedResults,
                    vf2Time: vf2Time,
                    totalTime: totalTime,
                    isRerunSet: isRerun,
                    resultSource: resultDir  // Add result source information
                });
            } catch (err) {
                console.error("Error reading results file:", err);
                res.json({
                    success: false,
                    error: 'Failed to read results file: ' + err.message
                });
            }
        });
        
    } catch (error) {
        console.error('Error in query process:', error);
        res.json({ success: false, error: error.message });
    }
});

function parseResultFile(fileContent) {
    const results = [];
    const lines = fileContent.split('\n');
    let currentResult = null;

    lines.forEach(line => {
        if (line.startsWith('Final t #')) {
            if (currentResult) {
                results.push(currentResult);
            }
            currentResult = {
                id: line.split('#')[1].split('*')[0].trim(),
                nodes: [],
                edges: []
            };
        } else if (line.startsWith('v ')) {
            const [_, id, label] = line.split(' ');
            currentResult.nodes.push({id: parseInt(id), label: parseInt(label)});
        } else if (line.startsWith('e ')) {
            const [_, source, target, type] = line.split(' ');
            currentResult.edges.push({
                source: parseInt(source), 
                target: parseInt(target),
                type: parseInt(type) || 0  // Keep original edge type value, default to 0
            });
        }
    });

    if (currentResult) {
        results.push(currentResult);
    }

    return results;
}

// Add a simple test route
app.get('/test', (req, res) => {
    console.log("Test endpoint hit");
    res.send('Test successful');
});

app.post('/gen_pattern', (req, res) => {
    console.log(req.body);  // Print request body

    res.json({})
})

app.post('/load_graph', (req, res) => {
    const dbChoice = req.body.database;
    
    try {
        // Save database selection to global variable and file
        global.selectedDatabase = dbChoice;
        fs.writeFileSync(`${__dirname}/data/selected_database.txt`, dbChoice);
        
        // Select different jar files based on database type
        const dbMapping = {
            '1': 'AIDS10K.txt',      // Display as AIDS10K
            '2': 'emodulo.txt',      // Display as emolecul, but actual filename remains emodulo.txt
            '3': 'pubchem23238'      // Display as pubchem23238
        };
        
        const dbName = dbMapping[dbChoice] || 'default';
        
        // Detect and set current dataset type
        const detectedDataset = detectDatasetType(dbName);
        currentDataset = detectedDataset;
        
        console.log(`Database selection saved: ${dbChoice} (${dbName})`);
        console.log(`Dataset type detected and set to: ${currentDataset}`);
        
        res.json({
            success: true,
            message: 'Database selection saved successfully',
            dataset: currentDataset,
            dbName: dbName
        });

    } catch (error) {
        console.error("Error saving database selection:", error);
        res.json({
            success: false,
            message: error.message
        });
    }
});

// Add route to get preset patterns
app.get('/get_default_patterns', (req, res) => {
    try {
        // Read current selected database
        const selectedDb = fs.readFileSync(`${__dirname}/data/selected_database.txt`, 'utf8').trim();
        const dbMapping = {
            '1': 'AIDS_defaultpattern.rtf',
            '2': 'emo_defaultpattern.rtf',
            '3': 'pubchem_defaultpattern.rtf'
        };
        
        const patternFileName = dbMapping[selectedDb];
        let patterns = [];
        
        if (patternFileName && patternFileName.endsWith('.rtf')) {
            const patternFilePath = path.join(__dirname, 'data', patternFileName);
            
            if (fs.existsSync(patternFilePath)) {
                const fileContent = fs.readFileSync(patternFilePath, 'utf8');
                patterns = parsePatternFile(fileContent);
            }
        }
        
        // If no file found or parsing fails, return default patterns
        if (patterns.length === 0) {
            patterns = getDefaultPatterns();
        }
        
        res.json({
            success: true,
            patterns: patterns,
            database: selectedDb
        });
        
    } catch (error) {
        console.error("Error loading default patterns:", error);
        res.json({
            success: false,
            message: error.message,
            patterns: getDefaultPatterns() // Return default patterns as fallback
        });
    }
});

// Add route to get current dataset atomic label mapping
app.get('/get_atomic_labels', (req, res) => {
    try {
        const atomicLabels = getCurrentAtomicLabels();
        const reverseAtomicLabels = getCurrentReverseAtomicLabels();
        const labels = DATASET_ATOMIC_LABELS[currentDataset];
        
        // Get ordered label list by frequency
        const orderedLabels = getLabelsOrderedByFrequency();
        
        res.json({
            success: true,
            dataset: currentDataset,
            atomicLabels: atomicLabels,
            reverseAtomicLabels: reverseAtomicLabels,
            labels: labels,
            orderedLabels: orderedLabels  // Add ordered label list
        });
        
    } catch (error) {
        console.error("Error getting atomic labels:", error);
        res.json({
            success: false,
            message: error.message
        });
    }
});

// Function to parse pattern file
function parsePatternFile(fileContent) {
    const patterns = [];
    const lines = fileContent.split('\n');
    let currentPattern = null;
    
    lines.forEach(line => {
        line = line.trim();
        if (line.startsWith('t #')) {
            // Start new pattern
            if (currentPattern) {
                patterns.push(currentPattern);
            }
            const patternId = line.split('#')[1].split('*')[0].trim();
            currentPattern = {
                type: 'default',
                title: `Pattern ${patterns.length + 1}`,
                id: patternId,
                nodes: [],
                edges: []
            };
        } else if (line.startsWith('v ') && currentPattern) {
            // Add node
            const parts = line.split(' ');
            if (parts.length >= 3) {
                const nodeId = parseInt(parts[1]);
                const label = parseInt(parts[2]);
                currentPattern.nodes.push({
                    id: nodeId,
                    label: label
                });
            }
        } else if (line.startsWith('e ') && currentPattern) {
            // Add edge
            const parts = line.split(' ');
            if (parts.length >= 4) {
                const source = parseInt(parts[1]);
                const target = parseInt(parts[2]);
                const type = parseInt(parts[3]);
                currentPattern.edges.push({
                    source: source,
                    target: target,
                    type: type
                });
            }
        }
    });
    
    // Add last pattern
    if (currentPattern) {
        patterns.push(currentPattern);
    }
    
    return patterns;
}

// Function to get default patterns
function getDefaultPatterns() {
    return [
        {
            type: 'default',
            title: 'Single node',
            nodes: [{id: 0, label: 1}],
            edges: []
        },
        {
            type: 'default', 
            title: 'Edge',
            nodes: [{id: 0, label: 1}, {id: 1, label: 1}],
            edges: [{source: 0, target: 1, type: 0}]
        },
        {
            type: 'default',
            title: 'Triangle',
            nodes: [{id: 0, label: 1}, {id: 1, label: 1}, {id: 2, label: 1}],
            edges: [{source: 0, target: 1, type: 0}, {source: 1, target: 2, type: 0}, {source: 2, target: 0, type: 0}]
        },
        {
            type: 'hub',
            title: 'Customized ego',
            degree: 4
        },
        {
            type: 'ring',
            title: 'Customized ring', 
            length: 6
        }
    ];
}

// Function to get ordered label list by frequency
function getLabelsOrderedByFrequency() {
    const frequencyOrder = DATASET_LABEL_FREQUENCY[currentDataset] || DATASET_LABEL_FREQUENCY['default'];
    const labels = DATASET_ATOMIC_LABELS[currentDataset];
    
    // Reorder labels based on frequency
    const orderedLabels = {};
    frequencyOrder.forEach((originalIndex, newIndex) => {
        if (originalIndex < labels.length) {
            const atomicSymbol = labels[originalIndex];
            // Display numbers start from 1, but correspond to frequency order
            orderedLabels[atomicSymbol] = String(originalIndex); // Keep original index as value
        }
    });
    
    return orderedLabels;
}

// Modify save graph route
app.post('/save_graph', (req, res) => {
    try {
        const graphData = req.body;
        const queryFilePath = 'data/query.txt';
        
        // Save original graph
        let output = 't # 0\n';
        
        // Re-map node IDs, ensure starting from 0
        const nodeIdMap = {};
        graphData.nodes.forEach((node, index) => {
            nodeIdMap[node.id] = index;
        });
        
        // Add node information, use new IDs
        graphData.nodes.forEach((node, index) => {
            output += `v ${index} ${node.label}\n`;
        });
        
        // Handle edge information, use new node IDs
        const sortedEdges = graphData.edges.map(edge => {
            const source = nodeIdMap[edge.source];
            const target = nodeIdMap[edge.target];
            const type = Number(edge.type) || 0;
            return { source, target, type };
        }).sort((a, b) => {
            if (a.source !== b.source) return a.source - b.source;
            return a.target - b.target;
        });
        
        // Add edge information
        sortedEdges.forEach(edge => {
            output += `e ${edge.source} ${edge.target} ${edge.type}\n`;
        });
        
        output += 't # -1\n';
        
        console.log("Writing to query.txt:", output);
        
        // Write original graph
        fs.writeFileSync(queryFilePath, output, { encoding: 'utf8', flag: 'w' });
        
        // Return success directly, without calling any jar file
                res.json({
                    success: true
        });
        
    } catch (error) {
        console.error("Error saving graph:", error);
        res.json({
            success: false,
            message: error.message
        });
    }
});

// Add endpoint to toggle isRerun flag
app.post('/toggle_rerun', (req, res) => {
    isRerun = !isRerun;
    
    // Write to file
    setIsRerun(isRerun);
    
    res.json({ 
        success: true, 
        isRerun: isRerun,
        message: isRerun ? "Reuse mode enabled: will only search previously matched graphs" : "Reuse mode disabled: will search entire database"
    });
});

// Add endpoint to check current isRerun status
app.get('/check_rerun_status', (req, res) => {
    // Read from file
    isRerun = getIsRerun();
    // Reducing verbosity
    res.json({ isRerun: isRerun });
});

// Add endpoint to reset isRerun flag
app.post('/reset_rerun', (req, res) => {
    console.log("🔴 Reset endpoint called! Current value:", isRerun);
    isRerun = false;
    
    try {
        // Write to file with extra verification
        fs.writeFileSync(RERUN_STATE_FILE, 'false', 'utf8');
        
        res.json({ success: true });
    } catch (error) {
        res.status(500).json({ success: false, error: error.message });
    }
});

// TED verification route
app.post('/verify_ted', (req, res) => {
    console.log("Starting TED verification and filtering process...");
    
    try {
        // Set Python script path
        const pythonScript = path.join(__dirname, 'ted_aids_verify.py');
        
        // Execute Python verification script, pass parameter to indicate using filtering function
        // Try using python3, if failed then use python
        const pythonCmd = process.platform === 'win32' ? 'python' : 'python3';
        const pythonProcess = spawn(pythonCmd, [pythonScript, '--filter'], {
            cwd: __dirname,
            timeout: 60000 // 60 seconds timeout
        });
        
        let stdoutData = '';
        let stderrData = '';
        
        pythonProcess.stdout.on('data', (data) => {
            stdoutData += data.toString();
        });
        
        pythonProcess.stderr.on('data', (data) => {
            stderrData += data.toString();
        });
        
        pythonProcess.on('close', (code) => {
            console.log(`TED verification process exited with code ${code}`);
            
            if (code === 0) {
                console.log("TED verification completed successfully");
                
                // Parse Python script output, check if graph needs to be replaced
                const needsReplacement = checkIfReplacementNeeded(stdoutData);
                
                // Analyze verification results, provide more detailed information
                const verificationAnalysis = analyzeVerificationOutput(stdoutData);
                
                if (verificationAnalysis.needsReplacement) {
                    console.log("TED verification found that a graph needs to be replaced");
                    
                    // Get replacement graphs from AIDS database
                    const replacementGraphs = getReplacementGraphs();
                    
                    res.json({
                        success: true,
                        needsReplacement: true,
                        replacementGraphs: replacementGraphs,
                        verificationOutput: stdoutData,
                        verificationSummary: verificationAnalysis.summary,
                        verificationPassed: false
                    });
                } else {
                    console.log("TED verification passed, no replacement needed");
                    res.json({
                        success: true,
                        needsReplacement: false,
                        verificationOutput: stdoutData,
                        verificationSummary: verificationAnalysis.summary,
                        verificationPassed: verificationAnalysis.isPerfect
                    });
                }
            } else {
                console.error("TED verification failed:", stderrData);
                res.json({
                    success: false,
                    error: `Verification failed with code ${code}: ${stderrData}`,
                    verificationOutput: stdoutData
                });
            }
        });
        
        pythonProcess.on('error', (error) => {
            console.error("Error executing TED verification:", error);
            res.json({
                success: false,
                error: `Process error: ${error.message}`
            });
        });
        
    } catch (error) {
        console.error("Error in TED verification:", error);
        res.json({
            success: false,
            error: error.message
        });
    }
});

// Helper function to check if graph needs to be replaced
function checkIfReplacementNeeded(verificationOutput) {
    // Analyze Python script output, determine if any TED graph does not match target graph or query graph is not TED subgraph
    const lines = verificationOutput.split('\n');
    
    for (let line of lines) {
        
        // Check for comprehensive verification failure situation
        if (line.includes(' Verification failed:') || 
            line.includes(' Structure verification failed:')) {
            return true;
        }
    }
    
    return false;
}

// Helper function to analyze verification output
function analyzeVerificationOutput(verificationOutput) {
    const lines = verificationOutput.split('\n');
    let analysis = {
        needsReplacement: false,
        isPerfect: false,
        summary: '',
        details: {
            subgraphValid: false,
            tedMatches: 0,
            totalTed: 0,
            subgraphMatchRate: 0,
            wasFiltered: false,
            originalTedCount: 0,
            filteredTedCount: 0
        }
    };
    
    // Parse key information
    for (let line of lines) {
        // Check if filtering function is used
        if (line.includes('Using comprehensive verification with filtering') || line.includes('Starting TED graph filtering')) {
            analysis.details.wasFiltered = true;
        }
        
        // Parse filtering statistics
        const originalCountMatch = line.match(/Original TED graph count: (\d+)/);
        if (originalCountMatch) {
            analysis.details.originalTedCount = parseInt(originalCountMatch[1]);
        }
        
        const filteredCountMatch = line.match(/Retained TED graph count: (\d+)|Filtered TED graph count: (\d+)/);
        if (filteredCountMatch) {
            analysis.details.filteredTedCount = parseInt(filteredCountMatch[1] || filteredCountMatch[2]);
        }
        
        // Check subgraph verification result
        if (line.includes('') || line.includes('All TED graphs contain query graph: Yes')) {
            analysis.details.subgraphValid = true;
        } else if (line.includes('Query graph is TED subgraph: No') || line.includes('All TED graphs contain query graph: No')) {
            analysis.details.subgraphValid = false;
        }
        
        // Parse TED graph statistics (adapt to new output format)
        const tedTotalMatch = line.match(/TED graph total: (\d+)|Filtered TED graph count: (\d+)/);
        if (tedTotalMatch) {
            analysis.details.totalTed = parseInt(tedTotalMatch[1] || tedTotalMatch[2]);
        }
        
        const tedMatchesMatch = line.match(/TED graphs with matches in AIDS: (\d+)/);
        if (tedMatchesMatch) {
            analysis.details.tedMatches = parseInt(tedMatchesMatch[1]);
        }
        
        const subgraphRateMatch = line.match(/Subgraph match rate: ([\d.]+)%/);
        if (subgraphRateMatch) {
            analysis.details.subgraphMatchRate = parseFloat(subgraphRateMatch[1]);
        }
    }
    
    // Check if replacement is needed
    analysis.needsReplacement = checkIfReplacementNeeded(verificationOutput);
    
    // Check if verification is perfect
    analysis.isPerfect = analysis.details.subgraphValid && 
                        analysis.details.tedMatches === analysis.details.totalTed &&
                        analysis.details.subgraphMatchRate >= 50;
    
    // Generate summary information
    if (analysis.details.wasFiltered) {
        // Summary for filtered functionality
        const filterInfo = analysis.details.originalTedCount > 0 ? 
            `(filtered from ${analysis.details.originalTedCount} to ${analysis.details.filteredTedCount})` : '';
        
        if (analysis.isPerfect) {
            analysis.summary = ` Perfect verification: TED graphs filtered${filterInfo}, all retained TED graphs have matches in database`;
        } else if (analysis.details.subgraphValid && analysis.details.tedMatches > 0) {
            analysis.summary = ` Filter successful, partial database match: TED graphs filtered${filterInfo}, ${analysis.details.tedMatches}/${analysis.details.totalTed} TED graphs have matches in database`;
        } else if (analysis.details.subgraphValid && analysis.details.tedMatches === 0) {
            analysis.summary = ` Filter successful but no database match: TED graphs filtered${filterInfo}, but no matches found in database`;
        } else if (analysis.details.filteredTedCount === 0) {
            analysis.summary = ` Filter failed: No TED graphs contain query graph as subgraph, all TED graphs removed`;
        } else {
            analysis.summary = `Filter exception: After filtering, some TED graphs still don't contain query graph`;
        }
    } else {
        // Summary without filtering (maintain original logic)
        if (analysis.isPerfect) {
            analysis.summary = " Perfect verification: Query graph is subgraph of TED graphs, all TED graphs have matches in database";
        } else if (analysis.details.subgraphValid && analysis.details.tedMatches > 0) {
            analysis.summary = ` Partial verification passed: Query graph is subgraph of TED graphs (${analysis.details.subgraphMatchRate.toFixed(1)}%), ${analysis.details.tedMatches}/${analysis.details.totalTed} TED graphs have matches in database`;
        } else if (analysis.details.subgraphValid && analysis.details.tedMatches === 0) {
            analysis.summary = ` Structure verification passed but database match failed: Query graph is subgraph of TED graphs, but TED graphs not found in database`;
        } else if (!analysis.details.subgraphValid && analysis.details.tedMatches > 0) {
            analysis.summary = `Structure verification failed: Query graph is not subgraph of TED graphs, but some TED graphs have matches in database`;
        } else {
            analysis.summary = ` Complete verification failed: Query graph is not subgraph of TED graphs, and TED graphs not found in database`;
        }
    }
    
    return analysis;
}

// Helper function to get replacement graphs
function getReplacementGraphs() {
    try {
        // Randomly select several graphs from the selected database file as replacements
        const selectedDbPath = path.join(__dirname, 'data', 'selected_database.txt');
        let databaseFile = 'data/AIDS10K.txt'; // Default value
        
        if (fs.existsSync(selectedDbPath)) {
            const selectedDb = fs.readFileSync(selectedDbPath, 'utf8').trim();
            switch(selectedDb) {
                case '1':
                    databaseFile = 'data/AIDS10K.txt';
                    break;
                case '2':
                    databaseFile = 'data/emolecul.rtf';
                    break;
                case '3':
                    databaseFile = 'data/pubchem23238';
                    break;
            }
        }
        
        const dbPath = path.join(__dirname, databaseFile);
        if (!fs.existsSync(dbPath)) {
            console.warn(`Database file not found: ${dbPath}, using fallback`);
            return getDefaultReplacementGraphs();
        }
        
        const graphs = parseGraphDatabase(dbPath);
        const smallGraphs = graphs.filter(g => g.nodes.length <= 8 && g.nodes.length >= 3); // Select medium-sized graphs
        
        if (smallGraphs.length === 0) {
            return getDefaultReplacementGraphs();
        }
        
        const numReplacements = Math.min(3, smallGraphs.length);
        const selectedGraphs = [];
        const shuffled = smallGraphs.sort(() => 0.5 - Math.random());
        
        for (let i = 0; i < numReplacements; i++) {
            selectedGraphs.push(shuffled[i]);
        }
        
        console.log(`Selected ${selectedGraphs.length} replacement graphs`);
        return selectedGraphs;
        
    } catch (error) {
        console.error("Error getting replacement graphs:", error);
        return getDefaultReplacementGraphs();
    }
}

function parseGraphDatabase(filePath) {
    const content = fs.readFileSync(filePath, 'utf8');
    const lines = content.split('\n');
    const graphs = [];
    let currentGraph = null;
    
    for (let line of lines) {
        line = line.trim();
        if (!line) continue;
        
        const parts = line.split(/\s+/);
        
        if (parts[0] === 't' && parts[1] === '#') {
            if (currentGraph) {
                graphs.push(currentGraph);
            }
            
            const graphId = parseInt(parts[2]);
            currentGraph = {
                id: graphId,
                nodes: [],
                edges: []
            };
        } else if (parts[0] === 'v' && currentGraph) {
            const nodeId = parseInt(parts[1]);
            const label = parseInt(parts[2]);
            currentGraph.nodes.push({ id: nodeId, label: label });
        } else if (parts[0] === 'e' && currentGraph) {
            const source = parseInt(parts[1]);
            const target = parseInt(parts[2]);
            const type = parseInt(parts[3]) || 0;
            currentGraph.edges.push({ source: source, target: target, type: type });
        }
    }
    
    if (currentGraph) {
        graphs.push(currentGraph);
    }
    
    return graphs;
}

// Default replacement graphs (if unable to get from database)
function getDefaultReplacementGraphs() {
    return [
        {
            id: 'default1',
            nodes: [
                { id: 0, label: 1 },
                { id: 1, label: 1 },
                { id: 2, label: 2 }
            ],
            edges: [
                { source: 0, target: 1, type: 0 },
                { source: 1, target: 2, type: 0 }
            ]
        },
        {
            id: 'default2', 
            nodes: [
                { id: 0, label: 1 },
                { id: 1, label: 1 },
                { id: 2, label: 1 },
                { id: 3, label: 2 }
            ],
            edges: [
                { source: 0, target: 1, type: 0 },
                { source: 1, target: 2, type: 0 },
                { source: 2, target: 3, type: 0 },
                { source: 3, target: 0, type: 0 }
            ]
        }
    ];
}

app.post('/save_query_stats', (req, res) => {
    try {
        const { queryId, stats } = req.body;
        const statsDir = path.join(__dirname, 'data', 'query_stats');
        
        // Ensure directory exists
        if (!fs.existsSync(statsDir)) {
            fs.mkdirSync(statsDir, { recursive: true });
        }
        
        // Use query ID as filename
        const filename = path.join(statsDir, `query_stats_${queryId}.json`);
        
        // Save all statistics
        fs.writeFileSync(filename, JSON.stringify(stats, null, 2));
        
        res.json({ success: true });
    } catch (error) {
        console.error('Error saving query statistics:', error);
        res.json({ success: false, message: error.message });
    }
});

const server = app.listen(3000)
server.setTimeout(0)
console.log("Server started on port 3000");
