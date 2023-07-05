const { searchElastic,closeElasticClient, insertBulkElastic, deleteElastic } = require("../../database/elastic");
const { ES, errorLogFile, logFile, DIRECTORIES, CONFIGS } = require("../../conf.json");
const { insertLog } = require("../../Logs/Script/formatLogs");
const os = require("os");
const fs = require('fs');
const path = require('path');
const readline = require('readline');

const currentUserName = CONFIGS.userName;
let userApplication = CONFIGS.userApplication; 
let applicationName= CONFIGS.applicationName;
let functionName= CONFIGS.functionName;

let txtMisiune = "";
let txtLocatie = "";
let txtCodAdapter = "";

let currentCSVfileAP = [];
let currentCSVfileBTLE = [];
let currentCSVfilePR = [];

let columnAPNames = [];
let columnPRNames = [];
let columnBTLENames = [];
let columnBTLENamesOldVers = [];

let foundAP = false;
let foundPR = false;
let foundBTLE = false;

let pairAP = {};
let pairPR = {};
let pairBTLE = {};

let forceContinueOnNextFile = false;

let sourceDirWifiCSV = DIRECTORIES.sourceDirWifiCSV;
let sourceDirWigle = DIRECTORIES.sourceDirWigle;
let sourceDirOUI = DIRECTORIES.sourceDirOUI;
let destDirFaulty = DIRECTORIES.destDirFaulty;
let destDirImported = DIRECTORIES.destDirImported;

const getComputerIp = () => {
  const interfaces = os.networkInterfaces();
  let ipAddress;

  for (const interfaceKey in interfaces) {
    const interfaceList = interfaces[interfaceKey];

    for (const interfaceInfo of interfaceList) {
      if (!interfaceInfo.internal && interfaceInfo.family === "IPv4") {
        ipAddress = interfaceInfo.address;
        break;
      }
    }

    if (ipAddress) {
      break;
    }
  }

  if (ipAddress) {
    return ipAddress;
  } else {
    throw new Error("Unable to retrieve IP Address.");
  }
};

const logging = (
  ipAddress,
  userName,
  message,
  logFile
) => {
  try {
    insertLog(
      {
        ipAddress: ipAddress,
        userName: userName,
        message: message,
      },
      logFile
    );
  } catch (error) {
    
    throw new Error(`Error while writing logs: ${error}`);
  }
};

const createDirectoryIfNotExists = (dirPath) => {
    if (dirPath && !fs.existsSync(dirPath)) {
      fs.mkdirSync(dirPath);
    }
  }

const appendPathSeparatorIfMissing = (dirPath) => {
    if (!dirPath.endsWith(path.sep)) {
      return dirPath + path.sep;
    }
    return dirPath;
  }

const logAndStopIfNotExisting = (type, dirPath, ipAddress) =>{
    if(!fs.existsSync(dirPath)){
        logging(ipAddress, currentUserName, `Error: ${type} ${dirPath} does not exist on disk`, errorLogFile);
        logging(ipAddress, currentUserName, `Application End`, logFile);
        throw new Error(`Unable to find ${type} on disk.`);
    }
}

const getFilesFromDirectory= (directoryPath, pattern) => {
    const files = [];
    try {
      //TODO: to use the asynchronous version fs.readdir with a callback or utilize fs.promises.readdir with promises or async/await.
      const dirents = fs.readdirSync(directoryPath, { withFileTypes: true });
      for (const dirent of dirents) {
        
        const filePath = path.join(directoryPath, dirent.name);
        
        if (dirent.isFile() && filePath.match(pattern)) {
            
          const stats = fs.statSync(filePath);
    
          const fileInfo = {
            name: dirent.name,
            path: filePath,
            size: stats.size,
            createdAt: stats.birthtime,
            modifiedAt: stats.mtime
          };
              
              files.push(fileInfo);
        }
      }
    } catch (error) {
        throw new Error(`Error getting ${pattern} files from ${directoryPath} directory.`);
    }
    return files;
  }

// const checkForFileMask = async(ipAddress) =>{

//     let pairMaskFile = {};
//     try{
//       const searchQuery = {"select d.cname,d.cValueInteger from swise.tWiseParameterDescription d where EXISTS (SELECT 1 FROM swise.tState t Where d.cType=t.cID AND t.cCategory ilike 'SystemParameter' and t.cState ilike 'FileMask')"};
    
//     let elasticResult = await searchElastic(searchQuery, ES.INDEX_WISE);
    
//     if(elasticResult)
//         elasticResult.hits.hits.map(hit => {pairMaskFile[hit._source["d.cname"]] = hit._source["d.cValueInteger"];});

//     logging(ipAddress, currentUserName, "Function checkForFileMask: checking done.", logFile);
//     }
//     catch (error){
//         logging(ipAddress, currentUserName, `Function checkForFileMask: Error: ${error}`, errorLogFile);       
//     }
//     return pairMaskFile;    
// }

const isFileLocked = (file) => {
    let stream = null;
    try {
        stream = fs.openSync(file,'r');
    } catch (error) {
        return true;
    } finally {
        if (stream !== null) {
            fs.closeSync(stream);
        }
    }   
    return false;
}

// const processTxtFiles = (ipAddress) => {
//     try{
//         const txtFileList = getFilesFromDirectory(sourceDirWifiCSV, ".txt");
//         if (txtFileList.length > 0) {
//             logging(ipAddress, currentUserName, `Txt file type...`, logFile);
//             logging(ipAddress, currentUserName, `Get all ${txtFileList.length} txt files.....done.`, logFile);              
            
//             //const pairFileMask = checkForFileMask(ipAddress);

//             txtFileList.map(file => {
//                 try {
//                     let fileNameValues = file.name.split('_');
            
//                     if (fileNameValues.length > 3) {
//                         txtMisiune = fileNameValues[0];
//                         txtLocatie = fileNameValues[1];
//                         txtCodAdapter = fileNameValues[2];
//                     } else {
//                         return; // Continue to the next iteration of the loop
//                     }
//                 } catch (error) {
//                     logging(ipAddress, currentUserName, `Critical error while getting values from TXT File name ${file.name}. Error: ${error}`, errorLogFile);
//                 }

//                 try
//                     {
//                         logging(ipAddress, currentUserName, `Read TXT File ${file.name} .....done.`, logFile);
//                         if (isFileLocked(file.path))
//                         {
//                             throw new Error(`Txt file ${file.name} is locked`);
//                         }
//                         logging(ipAddress, currentUserName, `TXT File ${file.name} is not locked.`, logFile);
                        
                        
//                        // Create a readable stream to read the CSV file
//                       let stream = fs.createReadStream(file.path);

//                       // Create a readline interface
//                       let rl = readline.createInterface({
//                         input: stream,
//                         crlfDelay: Infinity
//                       });


//                       let fisOk = true;
//                       // Event handler for each line
//                       rl.on('line', (line) => {
                          
//                             if(!forceContinueOnNextFile)
//                             {
//                               if (line!=="") {
//                                   let coords = line.split(" ");
//                                   if (coords.length >= 2) {
//                                       let latit = parseFloat(coords[0]);
//                                       let longit = parseFloat(coords[1]);
                          
  
//                                       try {
//                                         const searchQuery = {
//                                           query: {
//                                             function_score: {
//                                               query: {
//                                                 bool: {
//                                                   must: [
//                                                     { match: { fieldName: `${new Date().toISOString()}` } },
//                                                     { match: { fieldName: `${userApplication}` } },
//                                                     { match: { fieldName: `${ipAddress}` } },
//                                                     { match: { fieldName: `${applicationName}` } },
//                                                     { match: { fieldName: `${functionName}` } },
//                                                     { match: { fieldName: `${txtLocatie}` } },
//                                                     { match: { fieldName: `${longit} ${latit}` } },
//                                                     { match: { fieldName: `${txtLocatie}` } }
//                                                   ]
//                                                 }
//                                               },
//                                               functions: [
//                                                 {
//                                                   filter: { match_all: {} },
//                                                   script_score: {
//                                                     script: {
//                                                       source: "Math.random()"
//                                                     }
//                                                   }
//                                                 }
//                                               ],
//                                               boost_mode: "replace"
//                                             }
//                                           }
//                                         };
                                      
//                                       let elasticResult = searchElastic(searchQuery, ES.INDEX_WISE);
//                                       } catch (error) {
//                                           logging(ipAddress, currentUserName, `Eroare la inserarea coordonatelor unei locatii noi: ${error}`, errorLogFile);
//                                       }
                          
                                      
//                                        try {
//                                         const searchQuery = {
//                                           query: {
//                                             function_score: {
//                                               query: {
//                                                 bool: {
//                                                   must: [
//                                                     { match: { fieldName: `${new Date().toISOString()}` } },
//                                                     { match: { fieldName: `${userApplication}` } },
//                                                     { match: { fieldName: `${ipAddress}` } },
//                                                     { match: { fieldName: `${applicationName}` } },
//                                                     { match: { fieldName: `${functionName}` } },
//                                                     { match: { fieldName: `` } }, // Add the appropriate field name for this match
//                                                     { match: { fieldName: `${txtCodAdapter}` } },
//                                                     { match: { fieldName: `` } }, // Add the appropriate field name for this match
//                                                     { match: { fieldName: `${txtCodAdapter}` } },
//                                                     { match: { fieldName: `` } }, // Add the appropriate field name for this match
//                                                     { match: { fieldName: `1` } },
//                                                     { match: { fieldName: `${txtLocatie}` } },
//                                                     { match: { fieldName: `${txtMisiune}` } },
//                                                     { match: { fieldName: `Enabled` } }
//                                                   ]
//                                                 }
//                                               },
//                                               functions: [
//                                                 {
//                                                   filter: { match_all: {} },
//                                                   script_score: {
//                                                     script: {
//                                                       source: "Math.random()"
//                                                     }
//                                                   }
//                                                 }
//                                               ],
//                                               boost_mode: "replace"
//                                             }
//                                           }
//                                         };
//                                         let elasticResult = searchElastic(searchQuery, ES.INDEX_WISE);
//                                       } catch (error) {
//                                           logging(ipAddress, currentUserName, `Eroare la inserarea coordonatelor unui echipament nou: ${error}`, errorLogFile);
//                                       }
//                                   } else {
//                                       fisOk = false;
//                                   }
//                               }
//                           }
//                         });
                        
//                         stream.close();
                        
//                         if (fisOk === true)
//                         {
//                             //moveFileTo(file, "prelucrate");
                            
//                             if (moveFileTo(file, destDirImported + "\\" + file.name.toLowerCase().replace(".txt", `_${new Date().toISOString().replace(/[-:.]/g, '')}.txt`)))
//                             {
//                                logging(ipAddress, currentUserName, `TXT File ${file.name} moved to ${destDirImported + file.name.toLowerCase()} .....done.`, logFile);                         
//                             }
//                             else
//                             {
//                                 logging(ipAddress, currentUserName, `Error moving TXT File ${file.name} to ${destDirImported + file.name.toLowerCase()}`, errorLogFile);              
//                             }
                            
//                         }
//                         else
//                         {
//                             //moveFileTo(file, "necofnorme");
//                             if (moveFileTo(file, destDirFaulty + "\\" + file.name.toLowerCase().replace(".txt", `_${new Date().toISOString().replace(/[-:.]/g, '')}.txt`)))
//                             {
//                                 logging(ipAddress, currentUserName, `TXT File ${file.name} moved to ${destDirFaulty + file.name.toLowerCase()} .....done.`, logFile);              
//                             }
//                             else
//                             {
//                                 logging(ipAddress, currentUserName, `Error moving TXT File ${file.name} to ${destDirFaulty + file.name.toLowerCase()}`, errorLogFile);              
//                             }
//                         }

//                     }catch (error)
//                         {
//                         logging(ipAddress, currentUserName, `Error ${error}`, errorLogFile);
//                         }
//                 });}
//                 else
//                 {
//                     logging(ipAddress, currentUserName, `Txt file type not found!`, logFile);                
//                 }
//     }
//     catch(error){
//         throw new Error(`Error while processing txt files: ${error}`);
//     }
// }

const moveFileTo = (file, destinationPath) => {
    try {
      
        fs.renameSync(file.path, destinationPath);
    } catch (ex) {
        return false;
    }
    return true;
}


// const getIDFromWiseDatabase = (xUserApplication, xIP, xApplicationName, xFunctionName, xFileName, xDescriptionFile,  xSizeFile) => 
//         {
//             let result = -1;
//             try
//             {
//                 const searchQuery = {
//                                         query: {
//                                           function_score: {
//                                             query: {
//                                               bool: {
//                                                 must: [
//                                                   { match: { fieldName: `${new Date().toISOString()}` } },
//                                                   { match: { fieldName: `${xUserApplication}` } },
//                                                   { match: { fieldName: `${xIP}` } },
//                                                   { match: { fieldName: `${xApplicationName}` } },
//                                                   { match: { fieldName: `${xFunctionName}` } },
//                                                   { match: { fieldName: `${xFileName}` } },
//                                                   { match: { fieldName: `${xDescriptionFile}` } },
//                                                   { match: { fieldName: `${xSizeFile}` } },
//                                                   { match: { fieldName: `${xIP}` } }
//                                                 ]
//                                               }
//                                             },
//                                             functions: [
//                                               {
//                                                 filter: { match_all: {} },
//                                                 script_score: {
//                                                   script: {
//                                                     source: "Math.random()"
//                                                   }
//                                                 }
//                                               }
//                                             ],
//                                             boost_mode: "replace"
//                                           }
//                                         }
//                                       };

//                 result = searchElastic(searchQuery, ES.INDEX_WISE); 
            
//                 return result;
//             }
//             catch (error)
//             {
//                 logging(ipAddress, currentUserName, `Function getIDFromWiseDatabase: Error: ${error}`, errorLogFile);
//                 return result;
//             }
            
//         }


const insertRecordInDatabaseBulk = ( ipAddress, xUserTime, xUserx, xUserIP, xUserApplication, xUserSender, xCodeMission, xLocation, xCodeAdapter, xCodeSystem, xMacMain, xRecordType, xFirstTimeSeen, xLastTimeSeen, xChannel, xSpeed, xPrivacy, xCipher, xAuthentication, xPower, xBeacons, xIv, xLanIP, xIdLength, xSSID, xKey, xPackets, xMacAP, xMessage, xObservations, xIDFileName, xLatitude, xLongitude, xImportFlag) =>
        {
            
            try
            {
                if (xLongitude === "0" || xLongitude === "")
                {
                    xLongitude = "NULL";
                }
                if (xLatitude === "0" || xLatitude === "")
                {
                    xLatitude = "NULL";
                }
                xSSID = xSSID.replace("'", "''");

                const records = [
                  {
                    userTime: xUserTime,
                    user: xUserx,
                    userIP: xUserIP,
                    userApplication: xUserApplication,
                    userSender: xUserSender,
                    ccodemission: xCodeMission,
                    location: xLocation,
                    codeAdapter: xCodeAdapter,
                    codeSystem: xCodeSystem,
                    macmain: xMacMain,
                    recordtype: xRecordType,
                    firsttimeseen: xFirstTimeSeen,
                    lasttimeseen: xLastTimeSeen,
                    channel: xChannel,
                    speed: xSpeed,
                    privacy: xPrivacy,
                    cipher: xCipher,
                    authentication: xAuthentication,
                    power: xPower,
                    beacons: xBeacons,
                    iv: xIv,
                    lanip: xLanIP,
                    idlength: xIdLength,
                    essid: xSSID,
                    key: xKey,
                    packets: xPackets,
                    macap: xMacAP,
                    message: xMessage,
                    observations: xObservations,
                    idfilename: xIDFileName,
                    latitude: xLatitude,
                    longitude: xLongitude,
                    importflag: xImportFlag
                  }
                ];

                insertBulkElastic(records, ES.INDEX_WISE);

                return true;
               
            }
            catch (error)
            {
                logging(ipAddress, currentUserName, `Function fRecordSet_New: Error: ${error}`, errorLogFile);
                return false;
            }
            
        }


const deleteRecordsFromDatabase = async(xIDFileName, ipAddress) =>
        {
            try
            {
              const searchQuery = {
                term: {
                  cIDFileName: xIDFileName
                }
              };
                await deleteElastic(searchQuery); //,indexName);

                logging(ipAddress, currentUserName, `Rows deleted from database: ${rowsDeleted}`, logFile);
                return true;
            }
            catch (error)
            {
                logging(ipAddress, currentUserName, `Function fRecordSet_New: Error: ${error}`, errorLogFile);
                return false;
            }
        }


const csvFunction = (ipAddress) => {

  const csvFileList = getFilesFromDirectory(sourceDirWifiCSV, ".csv");

  logging(ipAddress, currentUserName, "CSV files type", logFile);
  logging(ipAddress, currentUserName, `Get all ${csvFileList.length} CSV Files.....done.`, logFile);

  if (csvFileList.length > 0)
    {
     
     csvFileList.map(file =>{
        forceContinueOnNextFile = false;
        currentCSVfileAP = [];
        currentCSVfilePR = [];
        currentCSVfileBTLE = [];
        foundAP = false;
        foundPR = false;
        foundBTLE = false;


         //********************************** RESET DICTIONARY VALUES****************************
        columnAPNames = ["BSSID", "First Time Seen", "Last Time Seen", "Channel", "Speed", "Privacy", "Cipher", "Authentication", "Power", "#Beacons", "#IV", "LAN IP", "ID-Length", "ESSID", "Key", "Latitudine", "Longitudine"];
        columnPRNames = ["Station MAC", "First Time Seen", "Last Time Seen", "Power", "#Packets", "BSSID", "Latitudine" ,"Longitudine","Probed ESSID"]; 
        columnBTLENames = ["BTLE Station MAC", "BTLE First Time Seen", "BTLE Last Time Seen", "BTLE Power", "BTLE #Packets", "BTLE BSSID","BTLE ESSID", "BTLE Latitudine", "BTLE Longitudine"];
        columnBTLENamesOldVers = ["BTLE Station MAC", "BTLE First Time Seen", "BTLE Last Time Seen", "BTLE Power", "BTLE #Packets", "BTLE BSSID", "BTLE Latitudine", "BTLE Longitudine"];
        //**************************************************************************************
            try
              {
                  let fileNameValues = file.name.split('_');
                  fileNameLocation = fileNameValues[0];
                  fileNameMission = fileNameValues[1];
                  fileNameSystemCode = fileNameValues[2];
                  fileNameAdapterCode = fileNameValues[3];
                  //pairFileMask = checkForFileMask(ipAddress);
                  // fileNameLocation = pairFileMask["codeLocation"] === -1 ? "-" : fileNameValues[pairFileMask["codeLocation"]];
                  // fileNameMission = pairFileMask["codeMission"] === -1 ? "-" : fileNameValues[pairFileMask["codeMission"]];
                  // fileNameSystemCode = pairFileMask["codeSystem"] === -1 ? "-" : fileNameValues[pairFileMask["codeSystem"]];
                  // fileNameAdapterCode = pairFileMask["codeAdapter"] === -1 ? "-" : fileNameValues[pairFileMask["codeAdapter"]];
              }
              catch (error)
              {
                  
                  logging(ipAddress, currentUserName, `Critical error while getting values from CSV File name ${file.name}. Error: ${error}`, errorLogFile);
                  forceContinueOnNextFile = true;
                  return;
              }

           //get an ID from dataBase to store session for each file. ID will be used to rollback data if error occur
          //  uniqueIDFile = getIDFromWiseDatabase(userApplication,ipAddress,applicationName,functionName,file.name,"",file.length);
          //  if (uniqueIDFile === -1){
          //      logging(ipAddress, currentUserName, `Critical error while getting ID from Database for CSV File ${file.name}.`, errorLogFile);
          //      forceContinueOnNextFile=true;
          //      return; 
          //  }
           
           try
                {
                  
                    logging(ipAddress, currentUserName, `Read CSV File ${file.name} .....done.`, logFile);

                    if (isFileLocked(file.path))
                        {
                            throw new Error(`CSV file ${file.name} is locked`);
                        }
                    logging(ipAddress, currentUserName, `CSV File ${file.name}  is not locked.`, logFile);
                    
                     
                    fs.readFile(file.path, 'utf8', (err, data) => {
                      if (err) {
                        console.error(err);
                        return;
                      }
                      
                      const lines = data.split('\r\n');
                      
                      lines.forEach((line) => {
                        // Process the line here
                        
                          if(forceContinueOnNextFile===false && line!=="") {
                            
                            // Check if it is AP by comparing first 5                       
                            if (line.indexOf(columnAPNames[0]) >= 0 && line.indexOf(columnAPNames[1]) >= 0 && line.indexOf(columnAPNames[2]) >= 0 && line.indexOf(columnAPNames[3]) >= 0 && line.indexOf(columnAPNames[4]) >= 0)
                            {
                                
                                foundAP = true;
                                foundPR = false;
                                foundBTLE = false;
                                
                            
                                let columnAPNameCSV = line.split(',');
                                
                                // CHECK FOR INTEGRITY (lenght of column names and equality)
                                if (columnAPNameCSV.length === columnAPNames.length)
                                {
                                    // Check Column Names
                                    for (let i = 0; i < columnAPNameCSV.length; i++)
                                    {            
                                        if (columnAPNames[i].toLowerCase().trim() !== columnAPNameCSV[i].toLowerCase().trim())
                                        {
                                          
                                            logging(ipAddress, currentUserName, `Column name (AP) from CSV File ${file.name} are not the same with what was declared`, errorLogFile);
                                            
                                            forceContinueOnNextFile = true;
                                            break;
                                        }
                                    }
                                    
                                }
                                else
                                {
                                  
                                    logging(ipAddress, currentUserName, `No of column (AP) from CSV File ${file.name} ${columnAPNames.length} is not equal with what was declared ${columnAPNameCSV.length}`, errorLogFile);
                                    forceContinueOnNextFile = true;
                                }
                                
                            }
                           
                            //Check for PR

                            else if (!line.includes("BTLE") && line.indexOf(columnPRNames[0]) >= 0 && line.indexOf(columnPRNames[1]) >= 0 && line.indexOf(columnPRNames[2]) >= 0) 
                            {
                                
                                foundPR = true;
                                foundAP = false;
                                foundBTLE = false;
                                tryForPR = false;
                               
                                let columnPRNameCSV = line.split(',');
                                                    
                                if (columnPRNameCSV.length === columnPRNames.length)
                                {
                                    //verific denumirea coloanelor
                                    for (let i = 0; i < columnPRNameCSV.length; i++)
                                    {
                                        
                                        if (columnPRNames[i].toLowerCase() !== columnPRNameCSV[i].toLowerCase().trim())
                                        {
                                            logging(ipAddress, currentUserName, `Column name (PR) from CSV File ${file.name} are not the same with what was declared`, errorLogFile);
                                            forceContinueOnNextFile = true;
                                            break;
                                        }
                                    }
                                    
                                }
                                else
                                {
                                    logging(ipAddress, currentUserName, `No of column (PR) from CSV File ${file.name} ${columnPRNames.length} is not equal with what was declared ${columnPRNameCSV.length}`, errorLogFile);
                                    forceContinueOnNextFile = true;
                                }
                                
                            }

                            // Check for BTLE import
                            
                            if (line.includes("BTLE") && line.indexOf(columnBTLENames[0]) >= 0 && line.indexOf(columnBTLENames[1]) >= 0 && line.indexOf(columnBTLENames[2]) >= 0)
                            {
                                
                                if (!line.includes("BTLE ESSID"))
                                {
                                    columnBTLENames = columnBTLENamesOldVers;
                                }
                                foundPR = false;
                                foundAP = false;
                                foundBTLE = true;
                                let columnBTLENameCSV = line.split(',');
                                
                                if (columnBTLENameCSV.length === columnBTLENames.length)
                                {
                                    //verific denumirea coloanelor
                                    for (let i = 0; i < columnBTLENameCSV.length; i++)
                                    {
                                        if (columnBTLENames[i].toLowerCase() !== columnBTLENameCSV[i].toLowerCase().trim())
                                        {
                                            logging(ipAddress, currentUserName, `Column name (BTLE) from CSV File ${file.name} are not the same with what was declared`, errorLogFile);
                                            forceContinueOnNextFile = true;
                                            break;
                                        }
                                    }
                                    
                                }
                                else
                                {
                                    logging(ipAddress, currentUserName, `No of column (BTLE) from CSV File ${file.name} ${columnBTLENameCSV.length} is not equal with what was declared ${columnBTLENames.length}`, errorLogFile);
                                   
                                }
                                
                            }
                            
                            
                            //****************

                            // GET VALUES FROM CSV LINE BY LINE (AP STRUCTURE)
                            
                            if (foundAP && !forceContinueOnNextFile)
                            {
                                
                                csvAPValues = line.split(',');
                                
                                if (csvAPValues.length === columnAPNames.length)
                                {

                                    for (let i = 0; i < csvAPValues.length; i++)
                                    {
                                        //add a line with values in dictionary
                                        pairAP[columnAPNames[i]] = csvAPValues[i].trim();
                                        
                                    }
                                    
                                    //add line (dictionary) in csvList
                                    pairAPLine = pairAP;                                   
                                    currentCSVfileAP.push(pairAPLine);
                                    
                                    pairAP={};
                                    return;
                                }
                                else
                                {
                                  if(line.trim().length === 0){return; }
                                   logging(ipAddress, currentUserName, `No of values (AP) from CSV File ${file.name} is not equal with no of declared column.`, errorLogFile);
                                   //TODO: Ce fac cand o coloana nu are nr de campuri corecte?
                                }

                            }

                             // GET VALUES FROM CSV LINE BY LINE (PR STRUCTURE)
                             
                             if (foundPR && !forceContinueOnNextFile)
                             {
                                
                                 csvPRValues = line.split(',').slice(0,columnPRNames.length-1);
                                
                                 probedESSID = line.split(',');
                                 let ESSID = [];
                                 if(probedESSID.length>columnPRNames.length)
                                    {probedESSID = probedESSID.slice(csvPRValues.length, probedESSID.length);
                                    ESSID = probedESSID.join(',');
                                    }
                                  else
                                  {ESSID = probedESSID[probedESSID.length-1];}
                                 
                                 
                                 if (csvPRValues.length === columnPRNames.length-1)
                                 {
 
                                     for (let i = 0; i < csvPRValues.length; i++)
                                     {
                                         //add a line with values in dictionary
                                         pairPR[columnPRNames[i]] = csvPRValues[i].trim();
                                     }
                                     //add last line with probed ESSID
                                     pairPR["Probed ESSID"] = ESSID;
                                     //add line (dictionary) in csvList
                                     
                                     let pairRPLine = pairPR;
                                     currentCSVfilePR.push(pairRPLine);
                                  
                                     pairPR = {};
                                     return;
                                 }
                                 else
                                 {
                                     if (line.trim().length === 0) { return; } //Daca s-a terminat brusc fisierul
                                     logging(ipAddress, currentUserName, `No of values (PR) from CSV File ${file.name} is not equal with no of declared column.`, errorLogFile);
                                    
                                 }
                             }
                             
                             
                             // GET VALUES FROM CSV LINE BY LINE (BTLE STRUCTURE)
                             if (foundBTLE && !forceContinueOnNextFile)
                             {
                              
                                 csvBTLEValues = line.split(',');
                                // string[] probedESSID = line.Split(',').Skip(columnPRNames.Length - 1).ToArray();
                                // string ESSID = string.Join(",", probedESSID);
                                 if (csvBTLEValues.length === columnBTLENames.length)
                                 {
 
                                     for (let i = 0; i < csvBTLEValues.length; i++)
                                     {
                                         //add a line with values in dictionary
                                         pairBTLE[columnBTLENames[i]] = csvBTLEValues[i].trim();
                                     }
                                     //add last line with probed ESSID                                    
                                     //pairBTLE.Add("Probed ESSID", csvBTLEValues["BTLE ESSID"]);
                                     //add line (dictionary) in csvList
                                     if(!pairBTLE["BTLE ESSID"])  //for backword compatibility SM 20230213
                                     {
                                         pairBTLE["BTLE ESSID"] = "";
                                     }
                                     let pairBTLELine = pairBTLE;
                                     
                                     currentCSVfileBTLE.push(pairBTLELine);
                                     pairBTLE = {};
                                     return;
                                 }
                                 else
                                 {
                                     if (line.trim().length === 0) { return; } //Daca s-a terminat brusc fisierul
                                     logging(ipAddress, currentUserName, `No of values (BTLE) from CSV File ${file.name} is not equal with no of declared column.`, errorLogFile);
                                     forceContinueOnNextFile = true;
                                 }
                             }
                            
                             
                      }});
                      
                      
                      
                                     
                    //finished to parse CSV file. Next check for error and if zero errors import in database
                    
                    if (currentCSVfileAP.length > 0 && !forceContinueOnNextFile)
                    {
                        
                        //Importam in baza de date AP
                        currentCSVfileAP.map( item => 
                        {
                          
                            try
                            {
                      // insert AP STRUCTURE IN DATABASE     
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                       
                                if (!insertRecordInDatabaseBulk(ipAddress, new Date().toISOString().replace(/[-:.]/g, ''), currentUserName, ipAddress, applicationName, "InsertAPStructure", fileNameMission, fileNameLocation, fileNameAdapterCode, fileNameSystemCode, item["BSSID"], "AP", item["First time seen"], item["Last time seen"], item["channel"], item["Speed"], item["Privacy"], item["Cipher"], item["Authentication"], item["Power"], item["# beacons"], item["# IV"], item["LAN IP"], item["ID-length"], item["ESSID"].replace("\0", " "), item["Key"], "", item["BSSID"], "", "", "uniqueIDFile", item.hasOwnProperty("Longitudine") ? item["Longitudine"] : "NULL", item.hasOwnProperty("Latitudine") ? item["Latitudine"] : "NULL", "0"))
                                {
                                    throw new Error("insertRecordInDatabase error! Return not empty");
                                }

                            }
                            catch (error)
                            {
                                logging(ipAddress, currentUserName, `Error importing AP structure from CSV File ${file.name}. AP will not be imported in database! Detail error ${error}`, errorLogFile);
                                deleteRecordsFromDatabase("uniqueIDFile", ipAddress);                                
                                forceContinueOnNextFile = true;
                            } 
                        });
                    }
                    

                    
                    if (currentCSVfilePR.length > 0 && !forceContinueOnNextFile)
                    {
                      
                        //Importam in baza de date PR
                        currentCSVfilePR.map(item =>
                        {
                            try
                            {
//insert PROBE REQUEST STRUCTURE IN DATABASE
                                if (!insertRecordInDatabaseBulk(ipAddress, new Date().toISOString().replace(/[-:.]/g, ''), currentUserName, ipAddress, applicationName, "InsertAPStructure", fileNameMission, fileNameLocation, fileNameAdapterCode, fileNameSystemCode, item["Station MAC"], "PR", item["First time seen"], item["Last time seen"], "NULL", "NULL", "NULL", "NULL", "NULL", item["Power"], "NULL", "NULL", "NULL", "NULL", item["Probed ESSID"].replace("\0", " "), "NULL", item["# packets"], item["BSSID"], "", "", "uniqueIDFile", item.hasOwnProperty("Longitudine") ? item["Longitudine"] : "NULL", item.hasOwnProperty("Latitudine") ? item["Latitudine"] : "NULL", "0", ))
                                {
                                    throw new Error("insertRecordInDatabase error! Return not empty");
                                }

                            }
                            catch (error)
                            {
                                logging(ipAddress, currentUserName, `Error found in CSV File ${file.name}. PR will not be imported in database. Detail error ${error}`, errorLogFile);
                                deleteRecordsFromDatabase("uniqueIDFile", ipAddress);                                
                                forceContinueOnNextFile = true;
                            }
                        });}
                        

                        // INSERT BTLE IN DATABASE
                        
                        if (currentCSVfileBTLE.length> 0 && !forceContinueOnNextFile)
                        {
                          
                            //Importam in baza de date PR
                           currentCSVfileBTLE.map(item =>
                            {
                                try
                                {
                                    // insert BTLE STRUCTURE IN DATABASE
                                    //DateTime.Now.toString("yyyy-MM-dd HH:mm:ss")
                                    if (!insertRecordInDatabaseBulk(ipAddress, new Date().toISOString().replace(/[-:.]/g, ''), currentUserName, ipAddress, applicationName, "InsertAPStructure", fileNameMission, fileNameLocation, fileNameAdapterCode, fileNameSystemCode, item["BTLE Station MAC"], "BTLE", item["BTLE First time seen"], item["BTLE Last time seen"], "NULL", "NULL", "NULL", "NULL", "NULL", item["BTLE Power"], "NULL", "NULL", "NULL", "NULL", item["BTLE ESSID"].replace("\0", " "), "NULL", item["BTLE # packets"], item["BTLE BSSID"], "", "", "uniqueIDFile", item["BTLE Latitudine"], item["BTLE Longitudine"], "0"))
                                    {
                                        throw new Error("insertRecordInDatabase error! Return not empty");
                                    }
                                   
                                }
                                catch (error)
                                {
                                    logging(ipAddress, currentUserName, `Error found in CSV File ${file.name}. BTLE will not be imported in database. Detail error ${error}`, errorLogFile);
                                    deleteRecordsFromDatabase("uniqueIDFile", ipAddress);
                                 forceContinueOnNextFile = true;
                                }
                            });}
                            
                        // INSERT BTLE IN DATABASE
            
                        
                        if (!forceContinueOnNextFile){ 
// move file into destination directory 
                            if (moveFileTo(file, destDirImported + '\\' + file.name.toLowerCase().replace(".csv", `_${"uniqueIDFile"}.csv`))){
                             logging(ipAddress, currentUserName, `CSV File ${file.name} moved to ${destDirImported + "\\" + file.name.toLowerCase().replace(".csv",`_${"uniqueIDFile"}.csv`)} .....done.`, logFile);
                        }
                        else{
                            logging(ipAddress, currentUserName, `Error moving CSV File ${file.name} to ${destDirImported + "\\" + file.name.toLowerCase().replace(".csv",`_${"uniqueIDFile"}.csv`)}`, errorLogFile);
                        }
                        }
                  
                    else
                    {
                     
                        logging(ipAddress, currentUserName, `Error found in CSV File ${file.name}. It will not be moved to Imported Files`, errorLogFile);
                      forceContinueOnNextFile = true;
                    }

                    
                    if (forceContinueOnNextFile)
                    {

                        logging(ipAddress, currentUserName, `Error detected (forceContinueNextFile=true) for CSV File ${file.name}`, errorLogFile);
                        
                        if (moveFileTo(file, destDirFaulty + '\\' + file.name.toLowerCase().replace(".csv", `_${"uniqueIDFile"}.csv`))){
                        logging(ipAddress, currentUserName, `CSV File ${file.name} moved to ${destDirFaulty + file.name.toLowerCase()} .....done.`, logFile);
                        }
                        else{
                            logging(ipAddress, currentUserName, `Error moving CSV File ${file.name} to ${destDirFaulty + file.name.toLowerCase()}`, errorLogFile);
                        }

                    }
                  }); 
                    //application end?
                    
                }// Main try                   
                
                catch (error)
                {
                    logging(ipAddress, currentUserName, `${error}` , errorLogFile);
                }
                
          
     });
   
    }
    else
            {
                logging(ipAddress, currentUserName, "No CSV files on disk", logFile);
            }
          

            logging(ipAddress, currentUserName, "Application End", logFile);
            closeElasticClient();
            //Close DB connection if open.
     
}

const mainImporter = async (req, res, next) => {
    const ipAddress = getComputerIp();
    try {

        logging(ipAddress, currentUserName, "Application Start", logFile);

        appendPathSeparatorIfMissing(sourceDirWifiCSV);
        //appendPathSeparatorIfMissing(sourceDirWigle);
        //appendPathSeparatorIfMissing(sourceDirOUI);
        appendPathSeparatorIfMissing(destDirFaulty);
        appendPathSeparatorIfMissing(destDirImported);

        logAndStopIfNotExisting("sourceDirWifiCSV", sourceDirWifiCSV, ipAddress);
        //logAndStopIfNotExisting("sourceDirWigle", sourceDirWigle, ipAddress);
        //logAndStopIfNotExisting("sourceDirOUI", sourceDirOUI, ipAddress);

        createDirectoryIfNotExists(destDirFaulty);
        //createDirectoryIfNotExists(destDirFaulty + "CSV\\");
        //createDirectoryIfNotExists(destDirFaulty + "Wigle\\");
        //createDirectoryIfNotExists(destDirFaulty + "OUI\\");
        createDirectoryIfNotExists(destDirImported);
       // createDirectoryIfNotExists(destDirImported + "CSV\\");
        //createDirectoryIfNotExists(destDirImported + "Wigle\\");
        //createDirectoryIfNotExists(destDirImported + "OUI\\");

        logging(ipAddress, currentUserName, "Read configuration file.....done.", logFile);

        //processTxtFiles(ipAddress);
        csvFunction(ipAddress);

  } catch (error) {
      logging(ipAddress, currentUserName, `Error found while running script: ${error}`, errorLogFile);
  }
  finally{
    return;
  }
};

module.exports = {
  mainImporter,
};
