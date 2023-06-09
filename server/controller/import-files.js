const { insertBulkElastic} = require("../../database/elastic");
const { ES, errorLogFile, logFile, DIRECTORIES, CONFIGS, DICTIONARY_VALUES } = require("../../conf.json");
const { insertLog } = require("../../Logs/Script/formatLogs");
const os = require("os");
const fs = require('fs').promises;
const fss = require('fs');
const path = require('path');

const options = {timeZone: 'Europe/Bucharest', year: 'numeric', month: '2-digit', day: '2-digit', hour: '2-digit', minute: '2-digit', second: '2-digit', hour12: false};

let ouiList;

const currentUserName = CONFIGS.userName;
const applicationName= CONFIGS.applicationName;

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

let bulkInsertValues = [];

let forceContinueOnNextFile = false;

const sourceDirWifiCSV = DIRECTORIES.sourceDirWifiCSV;

const sourceDirOUI = DIRECTORIES.sourceDirOUI;
const destDirFaulty = DIRECTORIES.destDirFaulty;
const destDirImported = DIRECTORIES.destDirImported;

const getComputerIp = () => {
  const interfaces = os.networkInterfaces();
  const ipAddress = Object.values(interfaces)
    .flat()
    .find(interfaceInfo => !interfaceInfo.internal && interfaceInfo.family === "IPv4")?.address;

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
    if (dirPath && !fss.existsSync(dirPath)) {
      fss.mkdirSync(dirPath);
    }
  }

const appendPathSeparatorIfMissing = (dirPath) => {
    if (!dirPath.endsWith(path.sep)) {
      return dirPath + path.sep;
    }
    return dirPath;
  }

const logAndStopIfNotExisting = (type, dirPath, ipAddress) =>{
    if(!fss.existsSync(dirPath)){
        logging(ipAddress, currentUserName, `Error: ${type} ${dirPath} does not exist on disk`, errorLogFile);
        logging(ipAddress, currentUserName, `Application End`, logFile);
        throw new Error(`Unable to find ${type} on disk.`);
    }
}

const getFilesFromDirectory = async (directoryPath, pattern, batchSize) => {
    try {
      const dirents = await fs.readdir(directoryPath, { withFileTypes: true });
      
      const files = [];
      let count = 0;
  
      for (const dirent of dirents) {
        if (count >= batchSize) {
          break;
        }
        const filePath = path.join(directoryPath, dirent.name);
  
        if (dirent.isFile() && filePath.match(pattern)) {
          const stats = await fs.stat(filePath);
  
          const fileInfo = {
            name: dirent.name,
            path: filePath,
            size: stats.size,
            createdAt: stats.birthtime,
            modifiedAt: stats.mtime,
          };
  
          files.push(fileInfo);
          count ++;
        }
      }
      
      return files;
    } catch (error) {
      throw new Error(`Error getting ${pattern} files from ${directoryPath} directory.`);
    }
  };

const isFileLocked = (file) => {
    let stream = null;
    try {
        stream = fss.openSync(file,'r');
    } catch (error) {
        return true;
    } finally {
        if (stream !== null) {
            fss.closeSync(stream);
        }
    }   
    return false;
}

const moveFileTo = (file, destinationPath) => {
    try {
      
        fss.renameSync(file.path, destinationPath);
    } catch (ex) {
        return false;
    }
    return true;
}

const insertRecordInDatabaseBulk = ( ipAddress, xCodeMission, xLocation, xCodeAdapter, xCodeSystem, xMacMain, xRecordType, xFirstTimeSeen, xLastTimeSeen, xChannel, xSpeed, xPrivacy, xCipher, xAuthentication, xPower, xBeacons, xIv, xLanIP, xIdLength, xSSID, xKey, xPackets, xMacAP, xMessage, xObservations, xIDFileName, xLatitude, xLongitude, xImportFlag) =>
        {
            let currentTime = new Date().toLocaleString('en-US', options).replace(/[/,\s:]/g, '').substr(4, 4) + new Date().toLocaleString('en-US', options).replace(/[/,\s:]/g, '').substr(0, 4) + "T" + new Date().toLocaleString('en-US', options).replace(/[/,\s:]/g, '').substr(8);
            
            let xMacProducer = assignProducerToOUI(xMacMain, ouiList);

            const isRandomMac = xMacMain[1] === '2' || xMacMain[1] === '6' || xMacMain[1].toLowerCase() === 'a' || xMacMain[1].toLowerCase() === 'e';
//What if it's random generated, but it is found in OUI list?

            try
            {

                const records =
                  {
                    userTime: currentTime,
                    user: currentUserName,
                    userIP: ipAddress,
                    userApplication: applicationName,
                    codeMission: xCodeMission,
                    location: xLocation,
                    codeAdapter: xCodeAdapter,
                    codeSystem: xCodeSystem,
                    macmain: xMacMain,
                    producer: xMacProducer,
                    isMacRandom: isRandomMac,
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
                    latitude: (xLatitude === "0" || xLatitude === "") ? "" : xLatitude,
                    longitude: (xLongitude === "0" || xLongitude === "") ? "" : xLongitude,
                    importflag: xImportFlag
                  };

                bulkInsertValues.push(records);
                
                return true;
               
            }
            catch (error)
            {
                logging(ipAddress, currentUserName, `Function fRecordSet_New: Error: ${error}`, errorLogFile);
                return false;
            }
            
        }

const csvFunction = async (ipAddress) => {

  const csvFileList = await getFilesFromDirectory(sourceDirWifiCSV, ".csv", CONFIGS.batchSize);
  
  logging(ipAddress, currentUserName, "CSV files type", logFile);
  logging(ipAddress, currentUserName, `Get all ${csvFileList.length} CSV Files.....done.`, logFile);

    //********************************** RESET DICTIONARY VALUES****************************
    columnAPNames = DICTIONARY_VALUES.columnAPNames;
    columnPRNames = DICTIONARY_VALUES.columnPRNames;
    columnBTLENames = DICTIONARY_VALUES.columnBTLENames;
    columnBTLENamesOldVers = DICTIONARY_VALUES.columnBTLENamesOldVers;
    //**************************************************************************************


  if (csvFileList.length > 0) {
    
    await Promise.all(csvFileList.map(async (file) => {
      
      try {
        let fileNameValues = file.name.split('_');
        fileNameLocation = fileNameValues[0];
        fileNameMission = fileNameValues[1];
        fileNameSystemCode = fileNameValues[2];
        fileNameAdapterCode = fileNameValues[3];
        uniqueIDFile = fileNameMission + "_" + fileNameAdapterCode + "_id_";
      } catch (error) {
        logging(ipAddress, currentUserName, `Critical error while getting values from CSV File name ${file.name}. Error: ${error}`, errorLogFile);
        forceContinueOnNextFile = true;
        return;
      }

      try {
        logging(ipAddress, currentUserName, `Read CSV File ${file.name} .....done.`, logFile);

        if (isFileLocked(file.path)) {
          throw new Error(`CSV file ${file.name} is locked`);
        }
        logging(ipAddress, currentUserName, `CSV File ${file.name}  is not locked.`, logFile);

        const data = await fs.readFile(file.path, 'utf8');

        bulkInsertValues = [];
      forceContinueOnNextFile = false;
      let notImportedLine = false;
      currentCSVfileAP = [];
      currentCSVfilePR = [];
      currentCSVfileBTLE = [];
      foundAP = false;
      foundPR = false;
      foundBTLE = false;
      let uniqueIDFile = "";
        
        const lines = data.split('\r\n');

        notImportedLine = false;
        
        await Promise.all(lines.map(async (line) => { {
         
          if (forceContinueOnNextFile === false && line !== "") {
                            
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
                            
                            else if (line.includes("BTLE") && line.indexOf(columnBTLENames[0]) >= 0 && line.indexOf(columnBTLENames[1]) >= 0 && line.indexOf(columnBTLENames[2]) >= 0)
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
                                   notImportedLine = true;
                                }

                            }
                            
                             // GET VALUES FROM CSV LINE BY LINE (PR STRUCTURE)
                             
                            else if (foundPR && !forceContinueOnNextFile)
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
                                     
                                     let pairPRLine = pairPR;
                                     
                                     currentCSVfilePR.push(pairPRLine);
                                     
                                     pairPR = {};
                                     return;
                                 }
                                 else
                                 {
                                    
                                     if (line.trim().length === 0) { return; } //Daca s-a terminat brusc fisierul
                                     logging(ipAddress, currentUserName, `No of values (PR) from CSV File ${file.name} is not equal with no of declared column.`, errorLogFile);
                                    notImportedLine = true;
                                 }
                             }
                             
                             
                             // GET VALUES FROM CSV LINE BY LINE (BTLE STRUCTURE)
                             else if (foundBTLE && !forceContinueOnNextFile)
                             {
                              
                                 csvBTLEValues = line.split(',');
                                
                                 if (csvBTLEValues.length === columnBTLENames.length)
                                 {
 
                                     for (let i = 0; i < csvBTLEValues.length; i++)
                                     {
                                         //add a line with values in dictionary
                                         pairBTLE[columnBTLENames[i]] = csvBTLEValues[i].trim();
                                     }
                                     
                                     if(!pairBTLE["BTLE ESSID"])  
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
                                     notImportedLine = true;
                                 }
                             }
          }
          
        }       }));            
                                    
                    //finished to parse CSV file. Next check for error and if zero errors import in database
                    
                    if (currentCSVfileAP.length > 0 && !forceContinueOnNextFile)
                    {
                        
                        currentCSVfileAP = currentCSVfileAP.slice(1,currentCSVfileAP.length);
                        //Importam in baza de date AP
                       
                        await Promise.all(currentCSVfileAP.map(async (item) => {
                            try {
                             
                              // insert AP STRUCTURE IN DATABASE
                              if (!insertRecordInDatabaseBulk(ipAddress, fileNameMission, fileNameLocation, fileNameAdapterCode, fileNameSystemCode, item["BSSID"], "AP", item["First Time Seen"], item["Last Time Seen"], item["Channel"], item["Speed"], item["Privacy"], item["Cipher"], item["Authentication"], item["Power"], item["#Beacons"], item["#IV"], item["LAN IP"], item["ID-Length"], item["ESSID"].replace("\0", " ").replace("'", "''"), item["Key"], "", item["BSSID"], "", "", uniqueIDFile + item["First Time Seen"], item["Longitudine"], item["Latitudine"], "0")) {
                                throw new Error("insertRecordInDatabase error! Return not empty");
                              }
                            } catch (error) {
                              logging(ipAddress, currentUserName, `Error importing AP file ${file.name} into DataBase. Detail error ${error}`, errorLogFile);
                              forceContinueOnNextFile = true;
                            }
                          }));

                        insertBulkElastic(bulkInsertValues, ES.INDEX_WISE);
                        
                    }
                    
                    else if (currentCSVfilePR.length > 0 && !forceContinueOnNextFile)
                    {
                        currentCSVfilePR=currentCSVfilePR.slice(1,currentCSVfilePR.length);
                        //Importam in baza de date PR
                        await Promise.all(currentCSVfilePR.map(async (item) => 
                        {
                            try
                            {

                                if (!insertRecordInDatabaseBulk(ipAddress, fileNameMission, fileNameLocation, fileNameAdapterCode, fileNameSystemCode, item["Station MAC"], "PR", item["First Time Seen"], item["Last Time Seen"], "", "", "", "", "", item["Power"], "", "", "", "", item["Probed ESSID"].replace("\0", " ").replace("'", "''"), "", item["#Packets"], item["BSSID"], "", "", uniqueIDFile + item["First Time Seen"], item["Longitudine"], item["Latitudine"], "0", ))
                                {
                                    throw new Error("insertRecordInDatabase error! Return not empty");
                                }

                            }
                            catch (error)
                            {
                                logging(ipAddress, currentUserName, `Error importing PR file ${file.name} into DataBase. Detail error ${error}`, errorLogFile);                              
                                forceContinueOnNextFile = true;
                            }
                        }));

                        
                        insertBulkElastic(bulkInsertValues, ES.INDEX_WISE);
                    }
                        

                        // INSERT BTLE IN DATABASE
                        
                    else if (currentCSVfileBTLE.length> 0 && !forceContinueOnNextFile)
                        {
                            currentCSVfileBTLE=currentCSVfileBTLE.slice(1,currentCSVfileBTLE.length);
                            //Importam in baza de date PR
                            await Promise.all(currentCSVfileBTLE.map(async (item) => {
                            
                                try
                                {
                                    // insert BTLE STRUCTURE IN DATABASE
                                    //DateTime.Now.toString("yyyy-MM-dd HH:mm:ss")
                                    if (!insertRecordInDatabaseBulk(ipAddress, fileNameMission, fileNameLocation, fileNameAdapterCode, fileNameSystemCode, item["BTLE Station MAC"], "BTLE", item["BTLE First Time Seen"], item["BTLE Last Time Seen"], "", "", "", "", "", item["BTLE Power"], "", "", "", "", item["BTLE ESSID"].replace("\0", " ").replace("'", "''"), "", item["BTLE #Packets"], item["BTLE BSSID"], "", "", uniqueIDFile + item["BTLE First Time Seen"], item["BTLE Latitudine"], item["BTLE Longitudine"], "0"))
                                    {
                                        throw new Error("insertRecordInDatabase error! Return not empty");
                                    }
                                   
                                }
                                catch (error)
                                {
                                    logging(ipAddress, currentUserName, `Error importing BTLE file ${file.name} into DataBase. Detail error ${error}`, errorLogFile);
                                 forceContinueOnNextFile = true;
                                }
                            }));
                        
                            
                            insertBulkElastic(bulkInsertValues, ES.INDEX_WISE);
                    }
                            
                        // INSERT BTLE IN DATABASE
            
                        
                        if (!forceContinueOnNextFile && !notImportedLine){ 
// move file into destination directory 
                            if (moveFileTo(file, destDirImported + '\\' + file.name)){
                             logging(ipAddress, currentUserName, `CSV File ${file.name} moved to ${destDirImported + "\\" + file.name} .....done.`, logFile);
                            }
                             else{
                            logging(ipAddress, currentUserName, `Error moving CSV File ${file.name} to ${destDirImported + "\\" + file.name}`, errorLogFile);
                            forceContinueOnNextFile = true;
                            }
                    }
        

                    
                    if (forceContinueOnNextFile || notImportedLine)
                    {

                        logging(ipAddress, currentUserName, `Error detected (forceContinueNextFile=true or LineNotImported) for CSV File ${file.name}`, errorLogFile);
                        
                        if (moveFileTo(file, destDirFaulty + '\\' + file.name)){
                        logging(ipAddress, currentUserName, `CSV File ${file.name} moved to ${destDirFaulty + file.name} .....done.`, logFile);
                        }
                        else{
                            logging(ipAddress, currentUserName, `Error moving CSV File ${file.name} to ${destDirFaulty + file.name}`, errorLogFile);
                        }

                    }
                  
                    //application end?
                } catch (error) {
                    logging(ipAddress, currentUserName, `Error while reading lines`, errorLogFile);                                   
                  }
                }// Main try                   
            ));             
          
    }
    else
            {
                logging(ipAddress, currentUserName, "No CSV files on disk", logFile);
                logging(ipAddress, currentUserName, "Application End", logFile);
                return;
            }
      
      csvFunction(ipAddress);
                       
      //Close DB connection if open.
     
}


const extractData = (filePath) => {
    // Read the file
    const fileContent = fss.readFileSync(filePath, 'utf8');
  
    // Split the content into groups based on empty lines
    const groups = fileContent.split('\r\n\r\n');
  
    // Process each group and build the dictionary
    const dictionary = groups.reduce((dict, group) => {
      // Get the first line of the group
      const firstLine = group.trim().split('\r\n')[0];
  
      // Extract the OUI and PRODUCER information
      const match = firstLine.match(/([0-9A-Fa-f-]+)\s+\(hex\)\s+(.+)/);
      if (!match) return dict;
  
      const OUI = match[1];
      const PRODUCER = match[2].trim();
  
      // Add the entry to the dictionary
      dict[OUI] = PRODUCER;
      return dict;
    }, {});
  
    return dictionary;
  };


const assignProducerToOUI = (mac, ouiList) => {
   
    try {
        mac = mac.substring(0,8).replace(/:/g,"-");
      if (ouiList && ouiList[mac]) { 
        return ouiList[mac];

      }
      else
        return "Unknown";
    } catch (error) {
      return "Unknown";
    }
  };


const mainImporter = async (req, res, next) => {
    const ipAddress = getComputerIp();
    try {

        const filePath = sourceDirOUI;
        ouiList = extractData(filePath);

        logging(ipAddress, currentUserName, "Application Start", logFile);

        appendPathSeparatorIfMissing(sourceDirWifiCSV);
        
        //appendPathSeparatorIfMissing(sourceDirOUI);
        appendPathSeparatorIfMissing(destDirFaulty);
        appendPathSeparatorIfMissing(destDirImported);

        logAndStopIfNotExisting("sourceDirWifiCSV", sourceDirWifiCSV, ipAddress);
       
        //logAndStopIfNotExisting("sourceDirOUI", sourceDirOUI, ipAddress);

        createDirectoryIfNotExists(destDirFaulty);

        createDirectoryIfNotExists(destDirImported);

        logging(ipAddress, currentUserName, "Read configuration file.....done.", logFile);

        await csvFunction(ipAddress);

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

