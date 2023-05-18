//             VERSION
//version 1.0.0.7 dinamyc FileMask pattern from dataBase
//version 1.0.0.8 one open connection with database
//version 1.0.0.9 get all fils from all directory (Not only Top)
//version 1.0.1.0 Fix bug - key column value form csv files
//version 1.0.2.0 Fix bug - csv file abnormally terminated/Delete records from recordsTemp on error
//202006051500 - version 1.0.2.1 Fix bug - csv file abnormally terminated/Delete records from recordsTemp on error
//202009252000 - txt file processing
//202112081300 - version 1.0.2.3 kismet csv column optimize. Ex #beacons => # beacons
//202112081400 version 1.0.2.4 csvAPValues.Take
//202205101100 version 1.0.2.5 Bluetooth added
//202303021500 version 1.0.2.7 Adapt import for new Kismet2CSV version; reset columns from dictionary for every csv file
using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Windows.Forms;
using Npgsql;
using System.IO;
using System.Xml.Linq;
using System.Dynamic;
using System.Net;
using System.Globalization;
using System.Diagnostics;


namespace WifiDB
{
    public partial class Form1 : Form
    {
        
        public static string connectionString = string.Empty;
        public static string ipAdress = string.Empty;
        public static string currentUserName = "SRSIPDN\\user.sm";// string.Empty;//System.Security.Principal.WindowsIdentity.GetCurrent().Name;
        public static string appName = "WISE";
        //SOURCE DIRECTORY
        public static string sourceDirWifiCSV = string.Empty;       
        public static string sourceDirWigle = string.Empty;
        public static string sourceDirOUI = string.Empty;
        //DESTINATION DIRECTORY
        public static string destDirFaulty = string.Empty;
        public static string destDirImported = string.Empty;
        //OTHER
        public static int counter = 0;
        public static bool forceContinueOnNextFile = false;
        private List<LogInfo> logList;
        public static int xFileIndex = -1;
        /* �����������������������������������������������������������������������������������������������������������������������������������������������������������������������������������������������������������������������������������������
           | public static  string[] columnAPNames = new string[] { "BSSID", "First time seen", "Last time seen", "channel", "Speed", "Privacy", "Cipher", "Authentication", "Power", "# beacons", "# IV", "LAN IP", "ID-length", "ESSID", "Key"}; |
           |                                                                                                                                                             |
           | public static string[] columnPRNames = new string[] { "Station MAC", "First time seen", "Last time seen", "Power", "# packets", "BSSID"}; //Probed ESSIDs                                                                             |
           | public static  string[] columnBTLENames = new string[] { "BTLE Station MAC", "BTLE First time seen", "BTLE Last time seen", "BTLE Power", "BTLE # packets", "BTLE BSSID","BTLE ESSID", "BTLE Latitudine", "BTLE Longitudine" };       |
           | public static string[] columnBTLENamesOldVers = new string[] { "BTLE Station MAC", "BTLE First time seen", "BTLE Last time seen", "BTLE Power", "BTLE # packets", "BTLE BSSID", "BTLE Latitudine", "BTLE Longitudine" };              |
           ����������������������������������������������������������������������������������������������������������������������������������������������������������������������������������������������������������������������������������������� */
        public static string[] columnAPNames;
        public static string[] columnPRNames;
        public static string[] columnBTLENames;
        public static string[] columnBTLENamesOldVers;
        public Dictionary<string, string> pairAP=new Dictionary<string, string>();
        public Dictionary<string, string> pairPR=new Dictionary<string, string>() ;
        public Dictionary<string, string> pairBTLE = new Dictionary<string, string>();
        public static Boolean foundAP = false;
        public static Boolean foundPR = false;
        public static Boolean foundBTLE = false;
        private static Boolean tryForPR = false;
        public List<Dictionary<string,string>> currentCSVfileAP=new List<Dictionary<string,string>>();
        public List<Dictionary<string, string>> currentCSVfilePR = new List<Dictionary<string, string>>();
        public List<Dictionary<string, string>> currentCSVfileBTLE = new List<Dictionary<string, string>>();
        public Int64 uniqueIDFile;
        public string userApplication = "SRSIPDN\\user.sm"; //TODO trebuie adaugat userul wiseImport in tUserDescription
        public string applicationName="Wise";
        public string functionName="fImportFilesSet";


        //FileName parameters
        string fileNameLocation = string.Empty;
        string fileNameMission = string.Empty;
        string fileNameSystemCode = string.Empty;
        string fileNameAdapterCode = string.Empty;

        NpgsqlConnection connMain;


         string txtMisiune = "";
         string txtLocatie = "";
         string txtCodAdapter ="";

        //VERSION
        public string versionApp = "1.0.0.0";

        private void Form1_Load(object sender, EventArgs e)
        {
            #region Hide Window
            this.Visible = false;
            this.Opacity = 0;
            #endregion

            #region Get Computer IP
            if (Dns.GetHostAddresses(Dns.GetHostName()).Length > 0)
            {
                ipAdress = Convert.ToString(Dns.GetHostAddresses(Dns.GetHostName())[0]);
            }
            #endregion

            logList = new List<LogInfo>();
            logList.Add(new LogInfo("1", DateTime.Now.ToString("dd.MM.yyyy HH:mm:ss"), "1000", ipAdress, "1000", currentUserName, "Application Start", versionApp, "Info", string.Empty));

            #region Read configuration values and validate values
            connectionString = System.Configuration.ConfigurationManager.ConnectionStrings["WifiDB.Properties.Settings.dbwifiConnectionString"].ConnectionString;
            sourceDirWifiCSV = Properties.Settings.Default.sourceDirWifiCSV;

            if (!sourceDirWifiCSV.EndsWith("\\"))
            {
                sourceDirWifiCSV = string.Concat(sourceDirWifiCSV, "\\");
            }
            sourceDirWigle = Properties.Settings.Default.sourceDirWigle;
            if (!sourceDirWigle.EndsWith("\\"))
            {
                sourceDirWigle = string.Concat(sourceDirWigle, "\\");
            }
            sourceDirOUI = Properties.Settings.Default.sourceDirOUI;
            if (!sourceDirOUI.EndsWith("\\"))
            {
                sourceDirOUI = string.Concat(sourceDirOUI, "\\");
            }
            destDirFaulty = Properties.Settings.Default.destDirFaulty;
            if (!destDirFaulty.EndsWith("\\"))
            {
                destDirFaulty = string.Concat(destDirFaulty, "\\");
            }
            //MAIN DIRECTORY CSV WIFI
            if (!System.IO.Directory.Exists(sourceDirWifiCSV))
            {
                logList.Add(new LogInfo("1", DateTime.Now.ToString("dd.MM.yyyy HH:mm:ss"), "1000", ipAdress, "1000", currentUserName, string.Format("Error: sourceDirWifiCSV {0} does not exist on disk", new string[] { sourceDirWifiCSV }), "1", "Info", string.Empty));
                logList.Add(new LogInfo("1", DateTime.Now.ToString("dd.MM.yyyy HH:mm:ss"), "1000", ipAdress, "1000", currentUserName, "Application End", "1", "Info", string.Empty));
                if (logList.Count > 0)
                {
                    var csv = new CsvExport<LogInfo>(logList);
                    csv.ExportToFile();
                }
                Close();
                return;
            }
            //WIGLE
            if (!System.IO.Directory.Exists(sourceDirWigle))
            {
                logList.Add(new LogInfo("1", DateTime.Now.ToString("dd.MM.yyyy HH:mm:ss"), "1000", ipAdress, "1000", currentUserName, string.Format("Error: sourceDirWigle {0} does not exist on disk", new string[] { sourceDirWigle }), "1", "Info", string.Empty));
                logList.Add(new LogInfo("1", DateTime.Now.ToString("dd.MM.yyyy HH:mm:ss"), "1000", ipAdress, "1000", currentUserName, "Application End", "1", "Info", string.Empty));
                if (logList.Count > 0)
                {
                    var csv = new CsvExport<LogInfo>(logList);
                    csv.ExportToFile();
                }
                Close();
                
            }
            //OUI
            if (!System.IO.Directory.Exists(sourceDirOUI))
            {
                logList.Add(new LogInfo("1", DateTime.Now.ToString("dd.MM.yyyy HH:mm:ss"), "1000", ipAdress, "1000", currentUserName, string.Format("Error: sourceDirOUI {0} does not exist on disk", new string[] { sourceDirOUI }), "1", "Info", string.Empty));
                logList.Add(new LogInfo("1", DateTime.Now.ToString("dd.MM.yyyy HH:mm:ss"), "1000", ipAdress, "1000", currentUserName, "Application End", "1", "Info", string.Empty));
                if (logList.Count > 0)
                {
                    var csv = new CsvExport<LogInfo>(logList);
                    csv.ExportToFile();
                }
                Close();

            }

            if (!System.IO.Directory.Exists(destDirFaulty))
            {
                System.IO.Directory.CreateDirectory(destDirFaulty);
            }

            if (!System.IO.Directory.Exists(destDirFaulty + "CSV\\"))
            {
                System.IO.Directory.CreateDirectory(destDirFaulty + "CSV\\");
            }
            if (!System.IO.Directory.Exists(destDirFaulty + "Wigle\\"))
            {
                System.IO.Directory.CreateDirectory(destDirFaulty + "Wigle\\");
            }

            if (!System.IO.Directory.Exists(destDirFaulty + "OUI\\"))
            {
                System.IO.Directory.CreateDirectory(destDirFaulty + "OUI\\");
            }

            destDirImported = Properties.Settings.Default.destDirImported;
            if (!destDirImported.EndsWith("\\"))
            {
                destDirImported = string.Concat(destDirImported, "\\");
            }
            if (!System.IO.Directory.Exists(destDirImported))
            {
                System.IO.Directory.CreateDirectory(destDirImported);
            }
            if (!System.IO.Directory.Exists(destDirImported + "CSV\\"))
            {
                System.IO.Directory.CreateDirectory(destDirImported + "CSV\\");
            }
            if (!System.IO.Directory.Exists(destDirImported + "Wigle\\"))
            {
                System.IO.Directory.CreateDirectory(destDirImported + "Wigle\\");
            }
            if (!System.IO.Directory.Exists(destDirImported + "OUI\\"))
            {
                System.IO.Directory.CreateDirectory(destDirImported + "OUI\\");
            }
            #endregion

            logList.Add(new LogInfo("1", DateTime.Now.ToString("dd.MM.yyyy HH:mm:ss"), "1000", ipAdress, "1000", currentUserName, "Read configuration file.....done.", "1", "Info", string.Empty));

            #region ProcessTXT files
                ProcessTxtFiles();
            #endregion


            var csvFileList = new List<FileInfo>();
            csvFileList.AddRange(getFilesFromTopDirectoryOnly(sourceDirWifiCSV, "*.csv"));

            logList.Add(new LogInfo("1", DateTime.Now.ToString("dd.MM.yyyy HH:mm:ss"), "1000", ipAdress, "1000", currentUserName, "CSV files type", "1", "Info", string.Empty));

            logList.Add(new LogInfo("1", DateTime.Now.ToString("dd.MM.yyyy HH:mm:ss"), "1000", ipAdress, "1000", currentUserName, string.Format("Get all {0} CSV Files.....done.", new string[] { Convert.ToString(csvFileList.Count) }), "1", "Info", string.Empty));

            if (csvFileList.Count > 0)
            {
                //create main connection
                connMain = new NpgsqlConnection(connectionString);
                try
                {
                    connMain.Open();
                    //connMain.ConnectionTimeout = 3000;
                }
                catch (Exception)
                {
                    
                    throw;
                }
                foreach (FileInfo file in csvFileList)
            {
                //initialize variable foreach new csv file
                forceContinueOnNextFile = false;
                currentCSVfileAP.Clear();
                currentCSVfilePR.Clear();
                currentCSVfileBTLE.Clear();
                foundAP = false;
                foundPR = false;
                foundBTLE = false;
                    //********************************** RESET DICTIONARY VALUES****************************
                    columnAPNames = new string[] { "BSSID", "First time seen", "Last time seen", "channel", "Speed", "Privacy", "Cipher", "Authentication", "Power", "# beacons", "# IV", "LAN IP", "ID-length", "ESSID", "Key"};
                    columnPRNames = new string[] { "Station MAC", "First time seen", "Last time seen", "Power", "# packets", "BSSID"}; //Probed ESSIDs
                    columnBTLENames = new string[] { "BTLE Station MAC", "BTLE First time seen", "BTLE Last time seen", "BTLE Power", "BTLE # packets", "BTLE BSSID","BTLE ESSID", "BTLE Latitudine", "BTLE Longitudine" };
                    columnBTLENamesOldVers = new string[] { "BTLE Station MAC", "BTLE First time seen", "BTLE Last time seen", "BTLE Power", "BTLE # packets", "BTLE BSSID", "BTLE Latitudine", "BTLE Longitudine" };
                    //**************************************************************************************
                #region Get Values from fileName
                try
                {
                    string[] fileNameValues = file.Name.Split('_');
                    Dictionary<string,int> pairFileMask=checkForFileMask(logList);
                    fileNameLocation =pairFileMask["codeLocation"]==-1?"-":fileNameValues[pairFileMask["codeLocation"]];
                    fileNameMission = pairFileMask["codeMission"]==-1?"-":fileNameValues[pairFileMask["codeMission"]];
                    fileNameSystemCode = pairFileMask["codeSystem"]==-1?"-":fileNameValues[pairFileMask["codeSystem"]];
                    fileNameAdapterCode = pairFileMask["codeAdapter"]==-1?"-":fileNameValues[pairFileMask["codeAdapter"]];
                }
                catch (Exception ex)
                {
                    logList.Add(new LogInfo("1", DateTime.Now.ToString("dd.MM.yyyy HH:mm:ss"), "1000", ipAdress, "1000", currentUserName, string.Format("Critical error while getting values from CSV File name {0}. Error: {1}", new string[] { Convert.ToString(file.Name), ex.Message }), "1", "Error", string.Empty));
                    forceContinueOnNextFile = true;
                   // break; //Must exit from foreach. Is a critical error and it should not continue
                    continue; // next csv file 
                }
                #endregion
                //get an ID from dataBase to store session for each file. ID will be used to rollback data if error occur
                uniqueIDFile = getIDFromWiseDatabase(userApplication,ipAdress,applicationName,functionName,file.FullName,string.Empty,Convert.ToString(file.Length),logList);
                if (uniqueIDFile==-1){
                    logList.Add(new LogInfo("1", DateTime.Now.ToString("dd.MM.yyyy HH:mm:ss"), "1000", ipAdress, "1000", currentUserName, string.Format("Critical error while getting ID from Database for CSV File {0}.", new string[] { Convert.ToString(file.Name) }), "1", "Error", string.Empty));
                    forceContinueOnNextFile=true;
                    break; //Must exit from foreach. Is a critical error and it should not continue
                }                    
               
                try
                {
                    logList.Add(new LogInfo("1", DateTime.Now.ToString("dd.MM.yyyy HH:mm:ss"), "1000", ipAdress, "1000", currentUserName, string.Format("Read CSV File {0} .....done.", new string[] { Convert.ToString(file.Name) }), "1", "Info", string.Empty));

                    if (isFileLocked(file))
                    {
                        continue;
                    }
                    logList.Add(new LogInfo("1", DateTime.Now.ToString("dd.MM.yyyy HH:mm:ss"), "1000", ipAdress, "1000", currentUserName, string.Format("CSV File {0}  is not locked.", new string[] { Convert.ToString(file.Name) }), "1", "Info", string.Empty));

                    #region Open, Load and Parse CSV line by line
                    StreamReader sr = new StreamReader(file.FullName);

                    string currentLine;
                    //currentline will be null when StreamReader reaches the end of file
                    while ((currentLine = sr.ReadLine()) != null && !forceContinueOnNextFile)
                    {
                        if (!currentLine.Equals(string.Empty))
                        {
                            //search case insensitive, if the currentline contains searched keyword
                            #region Check file for AP import; Get values line by line and add to pair
                            //search if find column name for AP format, minim 3 column
                            if (currentLine.IndexOf(columnAPNames[0], StringComparison.CurrentCultureIgnoreCase) >= 0 && currentLine.IndexOf(columnAPNames[1], StringComparison.CurrentCultureIgnoreCase) >= 0 && currentLine.IndexOf(columnAPNames[2], StringComparison.CurrentCultureIgnoreCase) >= 0 && currentLine.IndexOf(columnAPNames[3], StringComparison.CurrentCultureIgnoreCase) >= 0 && currentLine.IndexOf(columnAPNames[4], StringComparison.CurrentCultureIgnoreCase) >= 0)
                            {
                                foundAP = true;
                                foundPR = false;
                                foundBTLE = false;
                                if (currentLine.Contains("Latitudine") && currentLine.Contains("Longitudine"))
                                {
                                    if (!columnPRNames.Contains("Latitudine")) {columnAPNames=columnAPNames.Concat(new string[] { "Latitudine" }).ToArray();}
                                    if (!columnPRNames.Contains("Longitudine")) { columnAPNames = columnAPNames.Concat(new string[] { "Longitudine" }).ToArray(); }
                                }
                                string[] columnAPNameCSV = currentLine.Split(',');
                                #region CHECK FOR INTEGRITY (lenght of column names and eqaulity)
                                
                                if (columnAPNameCSV.Length == columnAPNames.Length)
                                {
                                    //verific denumirea coloanelor
                                    for (int i = 0; i < columnAPNameCSV.Length; i++)
                                    {
                                        if (columnAPNameCSV[i].ToLower() == "#beacons")
                                        {
                                            columnAPNameCSV[i] = "# beacons";
                                        }
                                        if (columnAPNameCSV[i].ToLower() == "#iv")
                                        {
                                            columnAPNameCSV[i] = "# iv";
                                        }
                                        if (columnAPNames[i].ToLower().Trim() != columnAPNameCSV[i].ToLower().Trim())
                                        {
                                            logList.Add(new LogInfo("1", DateTime.Now.ToString("dd.MM.yyyy HH:mm:ss"), "1000", ipAdress, "1000", currentUserName, string.Format("Column name (AP) from CSV File {0} are not the same with what was declared", new string[] { Convert.ToString(file.Name) }), "1", "Error", string.Empty));
                                            forceContinueOnNextFile = true;
                                        }
                                    }
                                    continue;
                                }
                                else
                                {
                                    logList.Add(new LogInfo("1", DateTime.Now.ToString("dd.MM.yyyy HH:mm:ss"), "1000", ipAdress, "1000", currentUserName, string.Format("No of column (AP) from CSV File {0} {1} is not equal with what was declared {2}", new string[] { Convert.ToString(file.Name), Convert.ToString(columnAPNames.Length), Convert.ToString(columnAPNameCSV.Length) }), "1", "Error", string.Empty));
                                    forceContinueOnNextFile = true;
                                }
                                #endregion
                            }
                            #endregion
                            #region Check file for PROBE REQUEST import; Get values line by line and add to pair

                            //EXCEPTIE BUG Nu se creaza header pentru PR !!! 20221110 SM
                            if (tryForPR && currentCSVfileAP.Count > 1 && !currentLine.Contains("Station MAC") && !foundPR )
                            {
                                if (currentCSVfileAP[0].ContainsKey("Longitudine") && currentLine.Split(',').Count() == 9 && !foundPR)
                                {
                                    //currentLine = "Station MAC,First Time Seen,Last Time Seen,Power,#Packets,BSSID,Latitudine,Longitudine,Probed ESSID";
                                    if (!columnPRNames.Contains("Latitudine")) { columnPRNames = columnPRNames.Concat(new string[] { "Latitudine" }).ToArray(); }

                                    if (!columnPRNames.Contains("Longitudine")) { columnPRNames = columnPRNames.Concat(new string[] { "Longitudine" }).ToArray(); }
                                    if (!columnPRNames.Contains("Probed ESSIDs")) { columnPRNames = columnPRNames.Concat(new string[] { "Probed ESSIDs" }).ToArray(); }
                                    foundPR = true;
                                    foundAP = false;
                                    foundBTLE = false;
                                    tryForPR = false;
                                }
                                if (!currentCSVfileAP[0].ContainsKey("Longitudine") && currentLine.Split(',').Count() == 7 && !foundPR)
                                {
                                    if (!columnPRNames.Contains("Probed ESSIDs")) { columnPRNames = columnPRNames.Concat(new string[] { "Probed ESSIDs" }).ToArray(); }
                                    foundPR = true;
                                    foundAP = false;
                                    foundBTLE = false;
                                    tryForPR = false;
                                }
                            }
                            if (!currentLine.Contains("BTLE") && currentLine.IndexOf(columnPRNames[0], StringComparison.CurrentCultureIgnoreCase) >= 0 && currentLine.IndexOf(columnPRNames[1], StringComparison.CurrentCultureIgnoreCase) >= 0 && currentLine.IndexOf(columnPRNames[2], StringComparison.CurrentCultureIgnoreCase) >= 0) 
                            {
                                foundPR = true;
                                foundAP = false;
                                foundBTLE = false;
                                tryForPR = false;
                                if (currentLine.Contains("Latitudine") && currentLine.Contains("Longitudine"))
                                {
                                    if (!columnPRNames.Contains("Latitudine")) { columnPRNames = columnPRNames.Concat(new string[] { "Latitudine" }).ToArray(); }
                                    
                                    if (!columnPRNames.Contains("Longitudine")) {columnPRNames=columnPRNames.Concat(new string[] { "Longitudine" }).ToArray();}
                                    if (!columnPRNames.Contains("Probed ESSIDs")) {columnPRNames = columnPRNames.Concat(new string[] { "Probed ESSIDs" }).ToArray();}
                                }
                                else
                                {
                                    if (!columnPRNames.Contains("Probed ESSIDs")) {columnPRNames = columnPRNames.Concat(new string[] { "Probed ESSIDs" }).ToArray();}
                                }
                                string[] columnPRNameCSV = currentLine.Split(',').Take(columnPRNames.Length - 3).ToArray();// !!! Only First 6 columns  //currentLine.Split(',');
                                #region CHECK FOR INTEGRITY (lenght of column names and eqaulity) !!! BUT only for first 6 columns. Last one is ProbedESSIDs and it is splitted by coma
                                if (columnPRNameCSV.Length == columnPRNames.Length - 3)
                                {
                                    //verific denumirea coloanelor
                                    for (int i = 0; i < columnPRNameCSV.Length; i++)
                                    {
                                        if (columnPRNameCSV[i].ToLower()=="#packets")
                                        {
                                            columnPRNameCSV[i] = "# packets";
                                        }
                                        if (columnPRNames[i].ToLower() != columnPRNameCSV[i].ToLower().Trim())
                                        {
                                            logList.Add(new LogInfo("1", DateTime.Now.ToString("dd.MM.yyyy HH:mm:ss"), "1000", ipAdress, "1000", currentUserName, string.Format("Column name (PR) from CSV File {0} are not the same with what was declared", new string[] { Convert.ToString(file.Name) }), "1", "Error", string.Empty));
                                            forceContinueOnNextFile = true;
                                        }
                                    }
                                    continue;
                                }
                                else
                                {
                                    logList.Add(new LogInfo("1", DateTime.Now.ToString("dd.MM.yyyy HH:mm:ss"), "1000", ipAdress, "1000", currentUserName, string.Format("No of column (PR) from CSV File {0} {1} is not equal with what was declared {2}", new string[] { Convert.ToString(file.Name), Convert.ToString(columnPRNames.Length), Convert.ToString(columnPRNameCSV.Length) }), "1", "Error", string.Empty));
                                    forceContinueOnNextFile = true;
                                }
                                #endregion
                            }
                            #endregion


                            //*************
                            #region Check file for BTLE import; Get values line by line and add to pair
                            if (currentLine.Contains("BTLE") && currentLine.IndexOf(columnBTLENames[0], StringComparison.CurrentCultureIgnoreCase) >= 0 && currentLine.IndexOf(columnBTLENames[1], StringComparison.CurrentCultureIgnoreCase) >= 0 && currentLine.IndexOf(columnBTLENames[2], StringComparison.CurrentCultureIgnoreCase) >= 0)
                            {
                                if (!currentLine.Contains("BTLE ESSID"))
                                {
                                    columnBTLENames = columnBTLENamesOldVers;
                                }
                                foundPR = false;
                                foundAP = false;
                                foundBTLE = true;
                                string[] columnBTLENameCSV = currentLine.Split(',').Take(columnBTLENames.Length - 1).ToArray();// !!! Only First 6 columns  //currentLine.Split(',');
                                #region CHECK FOR INTEGRITY (lenght of column names and eqaulity) !!! BUT only for first 6 columns. Last one is BTLE and it is splitted by coma
                                if (columnBTLENameCSV.Length == columnBTLENames.Length - 1)
                                {
                                    //verific denumirea coloanelor
                                    for (int i = 0; i < columnBTLENameCSV.Length; i++)
                                    {
                                        if (columnBTLENameCSV[i].ToLower() == "btle #packets")
                                        {
                                            columnBTLENameCSV[i] = "BTLE # packets";
                                        }
                                        if (columnBTLENames[i].ToLower() != columnBTLENameCSV[i].ToLower().Trim())
                                        {
                                            logList.Add(new LogInfo("1", DateTime.Now.ToString("dd.MM.yyyy HH:mm:ss"), "1000", ipAdress, "1000", currentUserName, string.Format("Column name (BTLE) from CSV File {0} are not the same with what was declared", new string[] { Convert.ToString(file.Name) }), "1", "Error", string.Empty));
                                            forceContinueOnNextFile = true;
                                        }
                                    }
                                    continue;
                                }
                                else
                                {
                                    logList.Add(new LogInfo("1", DateTime.Now.ToString("dd.MM.yyyy HH:mm:ss"), "1000", ipAdress, "1000", currentUserName, string.Format("No of column (BTLE) from CSV File {0} {1} is not equal with what was declared {2}", new string[] { Convert.ToString(file.Name), Convert.ToString(columnBTLENames.Length), Convert.ToString(columnBTLENames.Length) }), "1", "Error", string.Empty));
                                    forceContinueOnNextFile = true;
                                }
                                #endregion
                            }
                            #endregion
                          
                            //****************

                            #region GET VALUES FROM CSV LINE BY LINE (AP STRUCTURE)
                            if (foundAP && !forceContinueOnNextFile)
                            {
                                if (currentLine.Contains("\\,")) {currentLine=currentLine.Replace("\\,", ""); }
                                string[] csvAPValues = currentLine.Split(',');
                                //Apar probleme cand in coloana Key exista valoare. Creste nr de csvAPValues cu 1. |Cand se creeaza fisierul nu pune automat , daca e empty
                                if (csvAPValues.Length == columnAPNames.Length + 1)
                                {
                                    //c# csvAPValues = csvAPValues.Where(w => w != csvAPValues[csvAPValues.Length - 1]).ToArray();
                                    //SM 20211208
                                    csvAPValues = csvAPValues.Take(csvAPValues.Length-1).ToArray();
                                }
                                if (csvAPValues.Length == columnAPNames.Length)
                                {

                                    for (int i = 0; i < csvAPValues.Length; i++)
                                    {
                                        //add a line with values in dictionary
                                        pairAP.Add(columnAPNames[i], csvAPValues[i].Trim());
                                    }
                                    //add line (dictionary) in csvList
                                    Dictionary<string, string> pairAPLine = new Dictionary<string, string>();
                                    pairAPLine.AddRange(pairAP);                                   
                                    currentCSVfileAP.Add(pairAPLine);
                                    pairAP.Clear();
                                    continue;
                                }
                                else
                                {
                                    logList.Add(new LogInfo("1", DateTime.Now.ToString("dd.MM.yyyy HH:mm:ss"), "1000", ipAdress, "1000", currentUserName, string.Format("No of values (AP) from CSV File {0}  is not equal with no of declared column.", new string[] { Convert.ToString(file.Name) }), "1", "Error", string.Empty));
                                   // if (currentCSVfileAP.Count==0)
                                   // {
                                    //forceContinueOnNextFile = true;
                                    tryForPR = true;
                                  // }
                                }


                            }
                            #endregion

                           
                            //search if find column name for PROBE REQUEST format, minim 3 column
                            
                            #region GET VALUES FROM CSV LINE BY LINE (PR STRUCTURE)
                            if (foundPR && !forceContinueOnNextFile)
                            {
                                string[] csvPRValues = currentLine.Split(',').Take(columnPRNames.Length-1).ToArray();
                                string[] probedESSIDS = currentLine.Split(',').Skip(columnPRNames.Length-1).ToArray();
                                string ESSIDS = string.Join(",", probedESSIDS);
                                if (csvPRValues.Length == columnPRNames.Length-1)
                                {

                                    for (int i = 0; i < csvPRValues.Length; i++)
                                    {
                                        //add a line with values in dictionary
                                        pairPR.Add(columnPRNames[i], csvPRValues[i].Trim());
                                    }
                                    //add last line with probed ESSIDS
                                    pairPR.Add("Probed ESSIDs", ESSIDS);
                                    //add line (dictionary) in csvList
                                    Dictionary<string, string> pairRPLine = new Dictionary<string, string>();
                                    pairRPLine.AddRange(pairPR);
                                    currentCSVfilePR.Add(pairRPLine);
                                    pairPR.Clear();
                                    continue;
                                }
                                else
                                {
                                    if (currentLine[0] == '\0') { continue; } //Daca s-a terminat brusc fisierul
                                    logList.Add(new LogInfo("1", DateTime.Now.ToString("dd.MM.yyyy HH:mm:ss"), "1000", ipAdress, "1000", currentUserName, string.Format("No of values (PR) from CSV File {0}  is not equal with no of declared column.", new string[] { Convert.ToString(file.Name) }), "1", "Error", string.Empty));
                                    forceContinueOnNextFile = true;
                                }
                            }
                            #endregion

                            #region GET VALUES FROM CSV LINE BY LINE (BTLE STRUCTURE)
                            if (foundBTLE && !forceContinueOnNextFile)
                            {
                                string[] csvBTLEValues = currentLine.Split(',').Take(columnBTLENames.Length).ToArray();
                               // string[] probedESSIDS = currentLine.Split(',').Skip(columnPRNames.Length - 1).ToArray();
                               // string ESSIDS = string.Join(",", probedESSIDS);
                                if (csvBTLEValues.Length == columnBTLENames.Length)
                                {

                                    for (int i = 0; i < csvBTLEValues.Length; i++)
                                    {
                                        //add a line with values in dictionary
                                        pairBTLE.Add(columnBTLENames[i], csvBTLEValues[i].Trim());
                                    }
                                    //add last line with probed ESSIDS                                    
                                    //pairBTLE.Add("Probed ESSIDs", csvBTLEValues["BTLE ESSID"]);
                                    //add line (dictionary) in csvList
                                    if (!pairBTLE.ContainsKey("BTLE ESSID")) //for backword compatibility SM 20230213
                                    {
                                        pairBTLE.Add("BTLE ESSID", string.Empty);
                                    }
                                    Dictionary<string, string> pairBTLELine = new Dictionary<string, string>();
                                    pairBTLELine.AddRange(pairBTLE);
                                    currentCSVfileBTLE.Add(pairBTLELine);
                                    pairBTLE.Clear();
                                    continue;
                                }
                                else
                                {
                                    if (currentLine[0] == '\0') { continue; } //Daca s-a terminat brusc fisierul
                                    logList.Add(new LogInfo("1", DateTime.Now.ToString("dd.MM.yyyy HH:mm:ss"), "1000", ipAdress, "1000", currentUserName, string.Format("No of values (BTLE) from CSV File {0}  is not equal with no of declared column.", new string[] { Convert.ToString(file.Name) }), "1", "Error", string.Empty));
                                    forceContinueOnNextFile = true;
                                }
                            }
                            #endregion
                        }
                     } //while
                    sr.Close();

                    //finished to parse CSV file. Next check for error and if zero errors import in database

                    if (currentCSVfileAP.Count > 0 && !forceContinueOnNextFile)
                    {
                        //Importam in baza de date AP
                        foreach (Dictionary<string,string> item in currentCSVfileAP)
                        {
                            try
                            {
#region insert AP STRUCTURE IN DATABASE
                                if (!insertRecordInDatabaseBulk(DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss"), currentUserName, ipAdress, appName, "InsertAPStructure", fileNameMission, fileNameLocation, fileNameAdapterCode, fileNameSystemCode, Convert.ToString(item["BSSID"]), "AP", Convert.ToString(item["First time seen"]), Convert.ToString(item["Last time seen"]), Convert.ToString(item["channel"]), Convert.ToString(item["Speed"]), Convert.ToString(item["Privacy"]), Convert.ToString(item["Cipher"]), Convert.ToString(item["Authentication"]), Convert.ToString(item["Power"]), Convert.ToString(item["# beacons"]), Convert.ToString(item["# IV"]), Convert.ToString(item["LAN IP"]), Convert.ToString(item["ID-length"]), Convert.ToString(item["ESSID"]).Replace("\0", ""), Convert.ToString(item["Key"]), "NULL", Convert.ToString(item["BSSID"]), "", "", Convert.ToString(uniqueIDFile), item.ContainsKey("Longitudine") ? Convert.ToString(item["Longitudine"]) : "NULL", item.ContainsKey("Latitudine") ? Convert.ToString(item["Latitudine"]) : "NULL", "0", logList))
                                {
                                    throw new Exception("insertRecordInDatabase error! Return not empty");
                                }
#endregion
                            }
                            catch (Exception ex)
                            {
                                logList.Add(new LogInfo("1", DateTime.Now.ToString("dd.MM.yyyy HH:mm:ss"), "1000", ipAdress, "1000", currentUserName, string.Format("Error importing AP structure from CSV File {0}. AP will not be imported in database! Detail error {1}", new string[] { Convert.ToString(file.Name), ex.Message }), "1", "Error", string.Empty));
                                deleteRecordsFromDatabase(Convert.ToString(uniqueIDFile), logList);                                
                                forceContinueOnNextFile = true;
                            } 
                        }
                    }
                    else
                    {
                        logList.Add(new LogInfo("1", DateTime.Now.ToString("dd.MM.yyyy HH:mm:ss"), "1000", ipAdress, "1000", currentUserName, string.Format("Error found in CSV File {0}. AP rows count={1}. AP will not be imported in database. Move on to PR", new string[] { Convert.ToString(file.Name), Convert.ToString(currentCSVfileAP.Count) }), "1", "Info", string.Empty));
                       // forceContinueOnNextFile = true; Must move on to PR rows
                    }


                    if (currentCSVfilePR.Count > 0 && !forceContinueOnNextFile)
                    {
                        //Importam in baza de date PR
                        foreach (Dictionary<string, string> item in currentCSVfilePR)
                        {
                            try
                            {
#region insert PROBE REQUEST STRUCTURE IN DATABASE
                                if (!insertRecordInDatabaseBulk(DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss"), currentUserName, ipAdress, appName, "InsertAPStructure", fileNameMission, fileNameLocation, fileNameAdapterCode, fileNameSystemCode, Convert.ToString(item["Station MAC"]), "PR", Convert.ToString(item["First time seen"]), Convert.ToString(item["Last time seen"]), "NULL", "NULL", "NULL", "NULL", "NULL", Convert.ToString(item["Power"]), "NULL", "NULL", "NULL", "NULL", Convert.ToString(item["Probed ESSIDs"]).Replace("\0", ""), "NULL", Convert.ToString(item["# packets"]), Convert.ToString(item["BSSID"]), "", "", Convert.ToString(uniqueIDFile), item.ContainsKey("Longitudine") ? Convert.ToString(item["Longitudine"]) : "NULL", item.ContainsKey("Latitudine") ? Convert.ToString(item["Latitudine"]) : "NULL", "0", logList))
                                {
                                    throw new Exception("insertRecordInDatabase error! Return not empty");
                                }
#endregion
                            }
                            catch (Exception ex)
                            {
                                logList.Add(new LogInfo("1", DateTime.Now.ToString("dd.MM.yyyy HH:mm:ss"), "1000", ipAdress, "1000", currentUserName, string.Format("Error found in CSV File {0}. ProbeRequest will not be imported in database. Detail error {1}", new string[] { Convert.ToString(file.Name), ex.Message }), "1", "Error", string.Empty));
                                deleteRecordsFromDatabase(Convert.ToString(uniqueIDFile),logList);                                
                                forceContinueOnNextFile = true;
                            }
                        }
                        #region INSERT BTLE IN DATABASE
                        if (currentCSVfileBTLE.Count > 0 && !forceContinueOnNextFile)
                        {
                            //Importam in baza de date PR
                            foreach (Dictionary<string, string> item in currentCSVfileBTLE)
                            {
                                try
                                {
                                    #region insert BTLE STRUCTURE IN DATABASE
                                    if (!insertRecordInDatabaseBulk(DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss"), currentUserName, ipAdress, appName, "InsertAPStructure", fileNameMission, fileNameLocation, fileNameAdapterCode, fileNameSystemCode, Convert.ToString(item["BTLE Station MAC"]), "BTLE", Convert.ToString(item["BTLE First time seen"]), Convert.ToString(item["BTLE Last time seen"]), "NULL", "NULL", "NULL", "NULL", "NULL", Convert.ToString(item["BTLE Power"]), "NULL", "NULL", "NULL", "NULL", Convert.ToString(item["BTLE ESSID"]).Replace("\0", ""), "NULL", Convert.ToString(item["BTLE # packets"]), Convert.ToString(item["BTLE BSSID"]), "", "", Convert.ToString(uniqueIDFile), Convert.ToString(item["BTLE Latitudine"]), Convert.ToString(item["BTLE Longitudine"]), "0", logList))
                                    {
                                        throw new Exception("insertRecordInDatabase error! Return not empty");
                                    }
                                    #endregion
                                }
                                catch (Exception ex)
                                {
                                    logList.Add(new LogInfo("1", DateTime.Now.ToString("dd.MM.yyyy HH:mm:ss"), "1000", ipAdress, "1000", currentUserName, string.Format("Error found in CSV File {0}. BTLE will not be imported in database. Detail error {1}", new string[] { Convert.ToString(file.Name), ex.Message }), "1", "Error", string.Empty));
                                    deleteRecordsFromDatabase(Convert.ToString(uniqueIDFile), logList);
                                    forceContinueOnNextFile = true;
                                }
                            }

                            if (!forceContinueOnNextFile)
                            {
                                #region move file into destination directory
                                if (moveFileTo(file, Path.Combine(destDirImported + ("CSV\\"), file.Name.ToLower().Replace(".csv", string.Format("_{0}.{1}", new string[] { Convert.ToString(uniqueIDFile), ".csv" })))))
                                {
                                    logList.Add(new LogInfo("1", DateTime.Now.ToString("dd.MM.yyyy HH:mm:ss"), "1000", ipAdress, "1000", currentUserName, string.Format("CSV File {0} moved to {1} .....done.", new string[] { Convert.ToString(file.Name) }, Path.Combine(destDirImported + ("CSV\\"), file.Name.ToLower().Replace(".csv", string.Format("_{0}.{1}", new string[] { Convert.ToString(uniqueIDFile), ".csv" })))), "1", "Info", string.Empty));
                                }
                                else
                                {
                                    logList.Add(new LogInfo("1", DateTime.Now.ToString("dd.MM.yyyy HH:mm:ss"), "1000", ipAdress, "1000", currentUserName, string.Format("Error moving CSV File {0} to {1}", new string[] { Convert.ToString(file.Name) }, Path.Combine(destDirImported + ("CSV\\"), file.Name.ToLower().Replace(".csv", string.Format("_{0}.{1}", new string[] { Convert.ToString(uniqueIDFile), ".csv" })))), "1", "Error", string.Empty));
                                }
                                #endregion
                            }
                        }
                        else
                        {
                            if (currentCSVfileBTLE.Count > 0)
                            {
                                logList.Add(new LogInfo("1", DateTime.Now.ToString("dd.MM.yyyy HH:mm:ss"), "1000", ipAdress, "1000", currentUserName, string.Format("Error found in CSV File {0}.BTLE rows={1}. BTLE will not be imported in database", new string[] { Convert.ToString(file.Name), Convert.ToString(currentCSVfilePR.Count) }), "1", "Error", string.Empty));
                                forceContinueOnNextFile = true;
                            }
                            
                        }
                        #endregion
                        
                        if (!forceContinueOnNextFile){ 
#region move file into destination directory 
                            if (moveFileTo(file, Path.Combine(destDirImported + ("CSV\\"), file.Name.ToLower().Replace(".csv", string.Format("_{0}.{1}",new string[]{Convert.ToString(uniqueIDFile),".csv"}))))){
                             logList.Add(new LogInfo("1", DateTime.Now.ToString("dd.MM.yyyy HH:mm:ss"), "1000", ipAdress, "1000", currentUserName, string.Format("CSV File {0} moved to {1} .....done.", new string[] { Convert.ToString(file.Name) }, Path.Combine(destDirImported + ("CSV\\"), file.Name.ToLower().Replace(".csv", string.Format("_{0}.{1}",new string[]{Convert.ToString(uniqueIDFile),".csv"})))), "1", "Info", string.Empty));
                        }
                        else{
                            logList.Add(new LogInfo("1", DateTime.Now.ToString("dd.MM.yyyy HH:mm:ss"), "1000", ipAdress, "1000", currentUserName, string.Format("Error moving CSV File {0} to {1}", new string[] { Convert.ToString(file.Name) }, Path.Combine(destDirImported + ("CSV\\"), file.Name.ToLower().Replace(".csv", string.Format("_{0}.{1}",new string[]{Convert.ToString(uniqueIDFile),".csv"})))), "1", "Error", string.Empty));
                        }
#endregion
                        }
                    }
                    else
                    {
                        logList.Add(new LogInfo("1", DateTime.Now.ToString("dd.MM.yyyy HH:mm:ss"), "1000", ipAdress, "1000", currentUserName, string.Format("Error found in CSV File {0}.ProbeRequest rows={1}. ProbeRequest will not be imported in database", new string[] { Convert.ToString(file.Name), Convert.ToString(currentCSVfilePR.Count) }), "1", "Error", string.Empty));
                        forceContinueOnNextFile = true;
                    }


                  


                    if (forceContinueOnNextFile)
                    {
#region move csv file to faulty
                        //move csv file to faulty
                        //log errors
                        logList.Add(new LogInfo("1", DateTime.Now.ToString("dd.MM.yyyy HH:mm:ss"), "1000", ipAdress, "1000", currentUserName, string.Format("Error detected (forceContinueNextFile=true) for CSV File {0}", new string[] { Convert.ToString(file.Name) }), "1", "Error", string.Empty));
                        if (moveFileTo(file, Path.Combine(destDirFaulty + (""), file.Name.ToLower().Replace(".csv", string.Format("_{0}.{1}",new string[]{Convert.ToString(uniqueIDFile),".csv"}))))){
                        logList.Add(new LogInfo("1", DateTime.Now.ToString("dd.MM.yyyy HH:mm:ss"), "1000", ipAdress, "1000", currentUserName, string.Format("CSV File {0} moved to {1} .....done.", new string[] { Convert.ToString(file.Name) }, Path.Combine(destDirFaulty + (""), file.Name.ToLower().Replace(".csv", ".csv"))), "1", "Info", string.Empty));
                        }
                        else{
                            logList.Add(new LogInfo("1", DateTime.Now.ToString("dd.MM.yyyy HH:mm:ss"), "1000", ipAdress, "1000", currentUserName, string.Format("Error moving CSV File {0} to {1}", new string[] { Convert.ToString(file.Name) }, Path.Combine(destDirFaulty + (""), file.Name.ToLower().Replace(".csv", ".csv"))), "1", "Error", string.Empty));
                        }
#endregion
                    }

                    //application end?
                    #endregion
                }// Main try                   

                catch (Exception ex)
                {
                    logList.Add(new LogInfo("1", DateTime.Now.ToString("dd.MM.yyyy HH:mm:ss"), "1000", ipAdress, "1000", currentUserName, Convert.ToString(ex.Message), "1", "Info", string.Empty));
                }
                
            }
            }
            else
            {
                logList.Add(new LogInfo("1", DateTime.Now.ToString("dd.MM.yyyy HH:mm:ss"), "1000", ipAdress, "1000", currentUserName, "No CSV files on disk", "1", "Info", string.Empty));
            }
          

            logList.Add(new LogInfo("1", DateTime.Now.ToString("dd.MM.yyyy HH:mm:ss"), "1000", ipAdress, "1000", currentUserName, "Application End", "1", "Info", string.Empty));

            #region Export Log to disk
            if (logList.Count > 0)
            {
                var csv = new CsvExport<LogInfo>(logList);
                csv.ExportToFile();
            }
            #endregion
            try
            {
                if (connMain.State == ConnectionState.Open)
                {
                    connMain.Close();
                }
            }
            catch (Exception)
            {
                Close(); 
                
            }           
           
            Close();
        }


        public Form1()
        {
            InitializeComponent();
        } //constructor


        /// <summary>
        /// 
        /// </summary>
        /// <param name="path"></param>
        /// <param name="pattern"></param>
        /// <returns></returns>
        private List<FileInfo> getFilesFromTopDirectoryOnly(string path, string pattern)
        {
            var files = new List<FileInfo>();
            var directory = new DirectoryInfo(path);
            try
            {
                //files.AddRange(directory.GetFiles(pattern, SearchOption.TopDirectoryOnly));
                files.AddRange(directory.GetFiles(pattern, SearchOption.AllDirectories));
            }
            catch (UnauthorizedAccessException)
            {
            }
            return files;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="file"></param>
        /// <returns></returns>
        protected virtual bool isFileLocked(FileInfo file)
        {
            FileStream stream = null;
            try
            {
                stream = file.Open(FileMode.Open, FileAccess.Write, FileShare.None);
            }
            catch (IOException)
            {
                return true;
            }
            finally
            {
                if (stream != null)
                {
                    stream.Close();
                }
            }

            return false;
        }
        /// <summary>
        /// 
        /// </summary>
        /// <param name="file"></param>
        /// <param name="destinationPath"></param>
        /// <returns></returns>
        protected virtual bool moveXMLTo(FileInfo file, string destinationPath)
        {
            try
            {
                file.MoveTo(destinationPath);
            }
            catch (Exception ex)
            {
                return false;
            }
            return true;
        }

        protected virtual bool moveFileTo(FileInfo file, string destinationPath)
        {
            try
            {
                file.MoveTo(destinationPath);
            }
            catch (Exception ex)
            {
                return false;
            }
            return true;
        }

        //DATABASE FUNCTIONS

        private long getIDFromWiseDatabase(string xUserApplication, string xIP, string xApplicationName, string xFunctionName,string xFileName,string xDescriptionFile, string xSizeFile,List<LogInfo> logList)
        {
            var conn = new NpgsqlConnection(connectionString);
            long result = -1;
            try
            {
                conn.Open();
                //select swise.fImportFilesSet('2016-01-01 01:01:01','EVALP\Sorin','::1' ,'Wise','fImportFilesSet','c:\xxx.csv','test deascription','11111','::1')
                var cmd = new NpgsqlCommand(string.Format("SELECT swise.fImportFilesSet('{0}','{1}','{2}','{3}','{4}','{5}','{6}','{7}','{8}')", new string[] { DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss"),xUserApplication,xIP,xApplicationName,xFunctionName,xFileName,xDescriptionFile,xSizeFile,xIP }), conn);
                result=Convert.ToInt64(cmd.ExecuteScalar());                
                return result;
            }
            catch (Exception ex)
            {
                logList.Add(new LogInfo("1", DateTime.Now.ToString("dd.MM.yyyy HH:mm:ss"), "1000", ipAdress, "1000", currentUserName, string.Format("Function getIDFromWiseDatabase: Error: {0}", new string[] { Convert.ToString(ex.Message) }), "1", "Error", string.Empty));
                return result;
            }
            finally
            {
                if (conn.State == ConnectionState.Open)
                {
                    conn.Close();
                }
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="idExt"></param>
        /// <param name="plResXmlFileName"></param>
        /// <param name="logList"></param>
        /// <returns></returns>
        private string checkForTicketInDatabase(string idExt, string plResXmlFileName, List<LogInfo> logList)
        {
            var rezultat = "null" + "|" + "null";


            var conn = new NpgsqlConnection(connectionString);
            try
            {
                conn.Open();
                var cmd = new NpgsqlCommand(string.Format("SELECT \"IdExt\", \"XmlFileNamePlRes\" FROM \"sMain\".\"tTicket\" WHERE \"IdExt\"={0}", new string[] { idExt}), conn);

                var dr = cmd.ExecuteReader();
                while (dr.Read())
                {
                    if (Convert.ToString(dr[0]) == string.Empty)
                    {
                        rezultat = "null";
                    }
                    else
                    {
                        rezultat = Convert.ToString(dr[0]);
                    }
                    rezultat = rezultat + "|";
                    if (Convert.ToString(dr[1]) == string.Empty)
                    {
                        rezultat = rezultat + "null";
                    }
                    else
                    {
                        rezultat = rezultat + Convert.ToString(dr[0]);
                    }
                }

                logList.Add(new LogInfo("1", DateTime.Now.ToString("dd.MM.yyyy HH:mm:ss"), "1000", ipAdress, "1000", currentUserName, string.Format("Function checkForTicketInDatabase: checking done."), "1", "Info", string.Empty));
            }
            catch (Exception ex)
            {
                logList.Add(new LogInfo("1", DateTime.Now.ToString("dd.MM.yyyy HH:mm:ss"), "1000", ipAdress, "1000", currentUserName, string.Format("Function checkForTicketInDatabase: Error: {0}", new string[] { Convert.ToString(ex.Message) }), "1", "Error", string.Empty));
            }
            finally
            {
                if (conn.State == ConnectionState.Open)
                {
                    conn.Close();
                }
            }
            return rezultat;
        }



        private string insertAndValidateREQFromPLInDatabase(string idExt, string plResXmlFileName, string frequency, string bandwith, string time, string transmissionType, List<LogInfo> logList)
        {
            var rezultat = "null";


            var conn = new NpgsqlConnection(connectionString);
            try
            {
                conn.Open();
                //select "sMain"."fTicketPlReq"( 123, 12110000, 500, '2016-06-27 09:10:00', 'MORSE', 'FileName', 'XML Content');
                var cmd = new NpgsqlCommand(string.Format("SELECT FROM \"sMain\".\"fTicketPlReq\"({0},{1},{2},'{3}','{4}','{5}','{6}');", new string[] { idExt, frequency, bandwith, DateTime.ParseExact(time, "dd.MM.yyyy HH:mm:ss", null).ToString("yyyy-MM-dd HH:mm:ss"),transmissionType,plResXmlFileName,""}), conn);

                var dr = cmd.ExecuteScalar();            

                logList.Add(new LogInfo("1", DateTime.Now.ToString("dd.MM.yyyy HH:mm:ss"), "1000", ipAdress, "1000", currentUserName, string.Format("Function insertAndValidateREQFromPLInDatabase: checking done."), "1", "Info", string.Empty));
            }
            catch (Exception ex)
            {
                rezultat = ex.Message;
                logList.Add(new LogInfo("1", DateTime.Now.ToString("dd.MM.yyyy HH:mm:ss"), "1000", ipAdress, "1000", currentUserName, string.Format("Function insertAndValidateREQFromPLInDatabase: Error: {0}", new string[] { Convert.ToString(ex.Message) }), "1", "Error", string.Empty));
            }
            finally
            {
                if (conn.State == ConnectionState.Open)
                {
                    conn.Close();
                }
            }
            return rezultat;
        }
        
        
        /// <summary>
        /// 
        /// </summary>
        /// <param name="IdDf"></param>
        /// <param name="iDExt"></param>
        /// <param name="fileNameXml"></param>
        /// <param name="logList"></param>
        private bool updateTiketsAndTraksInDatabase(string angle,string IdDf, string iDExt, string fileNameXml, List<LogInfo> logList)
        {
            var conn = new NpgsqlConnection(connectionString);
            try
            {
                conn.Open();
                var cmd = new NpgsqlCommand("", conn);
                var idDfAcronym = (string)cmd.ExecuteScalar();

                
                return true;
            }
            catch (Exception ex)
            {
                logList.Add(new LogInfo("1", DateTime.Now.ToString("dd.MM.yyyy HH:mm:ss"), "1000", ipAdress, "1000", currentUserName, string.Format("Function updateTiketsAndTraksInDatabase: Error: {0}", new string[] { Convert.ToString(ex.Message) }), "1", "Error", string.Empty));
                return false;
            }
            finally
            {
                if (conn.State == ConnectionState.Open)
                {
                    conn.Close();
                }
            }
        }
        /// <summary>
        /// 
        /// </summary>
        /// <param name="iDDf"></param>
        /// <returns></returns>
        protected bool checkForDfIDInDatabase(string iDDf)
        {
            var conn = new NpgsqlConnection(connectionString);
            try
            {
                conn.Open();
                var cmd = new NpgsqlCommand(string.Format("SELECT  Count(*) FROM \"sMain\".\"tGonioLocations\" WHERE \"ExternalID\"='{0}'", new string[] { iDDf }), conn);
                var idExtCount = (Int64)cmd.ExecuteScalar();
                if (idExtCount > 0)
                {
                    return true;
                }
                else
                {
                    return false;
                }
            }
            catch (Exception ex)
            {
                return false;
            }
            finally
            {
                if (conn.State == ConnectionState.Open)
                {
                    conn.Close();
                }
            }
        }

        //***********************REQ**************************************
        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        private DataTable getReqFromDatabase()
        {
            NpgsqlDataReader drNull = null;
            DataTable dt = null;
            var conn = new NpgsqlConnection(connectionString);
            try
            {
                conn.Open();
                var cmd = new NpgsqlCommand(string.Format("SELECT \"ID\", \"XmlContentRoReq\" FROM \"sMain\".\"tTicket\" WHERE \"XmlFileNameRoReq\" is null {0}", new string[] { string.Empty }), conn);
                NpgsqlDataReader dr = cmd.ExecuteReader();
                dt = new DataTable();
                dt.Load(dr);                
            }
            catch (Exception ex)
            {
               
            }
            finally
            {
                if (conn.State == ConnectionState.Open)
                {
                    conn.Close();
                }
            }

            return dt;
        }


        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        private DataTable getResFromDatabase()
        {
            NpgsqlDataReader drNull = null;
            DataTable dt = null;
            var conn = new NpgsqlConnection(connectionString);
            try
            {
                conn.Open();
                var cmd = new NpgsqlCommand(string.Format("SELECT \"ID\", \"XmlContentRoRes\" FROM \"sMain\".\"tTicketFromPL\" WHERE \"XmlFileNameRoRes\" is null and \"XmlContentRoRes\" is not null {0}", new string[] { string.Empty }), conn);
                NpgsqlDataReader dr = cmd.ExecuteReader();
                dt = new DataTable();
                dt.Load(dr);
            }
            catch (Exception ex)
            {

            }
            finally
            {
                if (conn.State == ConnectionState.Open)
                {
                    conn.Close();
                }
            }

            return dt;
        }
        /// <summary>
        /// 
        /// </summary>
        /// <param name="id"></param>
        /// <param name="fileNameXml"></param>
        /// <param name="logList"></param>
        private void updateTicketREQ(string id,string fileNameXml, List<LogInfo> logList)
        {
            var conn = new NpgsqlConnection(connectionString);
            try
            {
                conn.Open();
                var cmd = new NpgsqlCommand(string.Format("UPDATE \"sMain\".\"tTicket\" SET \"XmlFileNameRoReq\"='{0}', \"TimeRoReq\"='{1}'  WHERE \"ID\"={2}", new string[] { fileNameXml, DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss"), id }), conn);
                cmd.ExecuteNonQuery();
            }
            catch (Exception ex)
            {
                logList.Add(new LogInfo("1", DateTime.Now.ToString("dd.MM.yyyy HH:mm:ss"), "1000", ipAdress, "1000", currentUserName, string.Format("Function updateTicketREQ: Error: {0}", new string[] { Convert.ToString(ex.Message) }), "1", "Error", string.Empty));
            }
            finally
            {
                if (conn.State == ConnectionState.Open)
                {
                    conn.Close();
                }
            }
        }

        private void updateTicketRES(string id, string fileNameXml, List<LogInfo> logList)
        {
            var conn = new NpgsqlConnection(connectionString);
            try
            {
                conn.Open();
                var cmd = new NpgsqlCommand(string.Format("UPDATE \"sMain\".\"tTicketFromPL\" SET \"XmlFileNameRoRes\"='{0}', \"TimeRoRes\"='{1}'  WHERE \"ID\"={2}", new string[] { fileNameXml, DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss"), id }), conn);
                cmd.ExecuteNonQuery();
            }
            catch (Exception ex)
            {
                logList.Add(new LogInfo("1", DateTime.Now.ToString("dd.MM.yyyy HH:mm:ss"), "1000", ipAdress, "1000", currentUserName, string.Format("Function updateTicketREQ: Error: {0}", new string[] { Convert.ToString(ex.Message) }), "1", "Error", string.Empty));
            }
            finally
            {
                if (conn.State == ConnectionState.Open)
                {
                    conn.Close();
                }
            }
        }
        private bool updateErrorDFInDatabase(string idExt, string fileNameXml, List<LogInfo> logList)
        {
            var conn = new NpgsqlConnection(connectionString);
            try
            {
                conn.Open();
               var  cmd = new NpgsqlCommand(string.Format("UPDATE \"sMain\".\"tTicket\" SET \"StatusPlRes\"=-1 WHERE \"IdExt\"={0}", new string[] { idExt }), conn);
                cmd.ExecuteNonQuery();
                cmd = new NpgsqlCommand(string.Format("UPDATE \"sMain\".\"tTicket\" SET \"XmlFileNamePlRes\"='{0}', \"TimePlRes\"='{1}'  WHERE \"IdExt\"={2}", new string[] { fileNameXml, DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss"), idExt }), conn);
                cmd.ExecuteNonQuery();
                return true;
            }
            catch (Exception ex)
            {
                logList.Add(new LogInfo("1", DateTime.Now.ToString("dd.MM.yyyy HH:mm:ss"), "1000", ipAdress, "1000", currentUserName, string.Format("Function updateErrorDFInDatabase: Error: {0}", new string[] { Convert.ToString(ex.Message) }), "1", "Error", string.Empty));
                return false;
            }
            finally
            {
                if (conn.State == ConnectionState.Open)
                {
                    conn.Close();
                }
            }
        }

        private bool insertRecordInDatabase(string xUserTime,string xUserx,string  xUserIP,string xUserApplication,string xUserSender,string  xCodeMission,string xLocation,string xCodeAdapter,string xCodeSystem,string xMacMain,string xRecordType,string xFirstTimeSeen,string xLastTimeSeen,string xChannel,string xSpeed,string xPrivacy,string xCipher,string xAuthentication,string xPower,string xBeacons,string xIv,string xLanIP,string xIdLength,string xSSID,string xKey,string xPackets,string xMacAP,string xMessage,string xObservations,string xIDFileName,string xLatitude,string xLongitude,string xImportFlag, List<LogInfo> logList)
        {
            //var conn = new NpgsqlConnection(connectionString);
            try
            {
                xSSID = xSSID.Replace("'", "''");
               // conn.Open();
                var cmd = new NpgsqlCommand(string.Format("SELECT swise.fRecordSet_New('{0}','{1}','{2}','{3}','{4}','{5}','{6}','{7}','{8}','{9}','{10}','{11}','{12}','{13}','{14}','{15}','{16}','{17}','{18}','{19}','{20}','{21}','{22}','{23}','{24}','{25}','{26}','{27}','{28}','{29}','{30}','{31}','{32}')", new string[] { xUserTime, xUserx, xUserIP, xUserApplication, xUserSender, xCodeMission, xLocation, xCodeAdapter, xCodeSystem, xMacMain, xRecordType, xFirstTimeSeen, xLastTimeSeen, xChannel, xSpeed,xPrivacy,xCipher, xAuthentication, xPower, xBeacons, xIv, xLanIP, xIdLength, xSSID, xKey, xPackets, xMacAP, xMessage, xObservations, xIDFileName, xLatitude, xLongitude, xImportFlag }), connMain);
                var idDfAcronym = (string)cmd.ExecuteScalar();
                

                return idDfAcronym.Equals(string.Empty);
            }
            catch (Exception ex)
            {
                logList.Add(new LogInfo("1", DateTime.Now.ToString("dd.MM.yyyy HH:mm:ss"), "1000", ipAdress, "1000", currentUserName, string.Format("Function fRecordSet_New: Error: {0}", new string[] { Convert.ToString(ex.Message) }), "1", "Error", string.Empty));
                return false;
            }
            finally
            {
                //if (conn.State == ConnectionState.Open)
                //{
                //    conn.Close();
                //}
            }
        }


        private bool deleteRecordsFromDatabase(string xIDFileName, List<LogInfo> logList)
        {
            var conn = new NpgsqlConnection(connectionString);
            try
            {
                conn.Open();
                var cmd = new NpgsqlCommand(string.Format("DELETE FROM swise.tRecordsTemp WHERE cIDFileName={0})", new string[] {xIDFileName }), conn);
                Int32 rowsDeleted = cmd.ExecuteNonQuery();

                logList.Add(new LogInfo("1", DateTime.Now.ToString("dd.MM.yyyy HH:mm:ss"), "1000", ipAdress, "1000", currentUserName, string.Format("Rows deleted from database: {0}", new string[] { Convert.ToString(rowsDeleted) }), "1", "Info", string.Empty));
                return true;
            }
            catch (Exception ex)
            {
                logList.Add(new LogInfo("1", DateTime.Now.ToString("dd.MM.yyyy HH:mm:ss"), "1000", ipAdress, "1000", currentUserName, string.Format("Function fRecordSet_New: Error: {0}", new string[] { Convert.ToString(ex.Message) }), "1", "Error", string.Empty));
                return false;
            }
            finally
            {
                if (conn.State == ConnectionState.Open)
                {
                    conn.Close();
                }
            }
        }

        private Dictionary<string,int> checkForFileMask(List<LogInfo> logList)
        {
            Dictionary<string,int> pairMaskFile=new Dictionary<string,int>();
            var conn = new NpgsqlConnection(connectionString);
            try
            {
                conn.Open();
                var cmd = new NpgsqlCommand(string.Format("select d.cname,d.cValueInteger from swise.tWiseParameterDescription d where EXISTS (SELECT 1 FROM swise.tState t Where d.cType=t.cID AND t.cCategory ilike '{0}' and t.cState ilike '{1}')", new string[] { "SystemParameter","FileMask" }), conn);

                var dr = cmd.ExecuteReader();
                while (dr.Read())
                {
                    pairMaskFile.Add(Convert.ToString(dr[0]), Convert.ToInt32(dr[1]));                  
                    
                }

                logList.Add(new LogInfo("1", DateTime.Now.ToString("dd.MM.yyyy HH:mm:ss"), "1000", ipAdress, "1000", currentUserName, string.Format("Function checkForFileMask: checking done."), "1", "Info", string.Empty));
            }
            catch (Exception ex)
            {
                logList.Add(new LogInfo("1", DateTime.Now.ToString("dd.MM.yyyy HH:mm:ss"), "1000", ipAdress, "1000", currentUserName, string.Format("Function checkForFileMask: Error: {0}", new string[] { Convert.ToString(ex.Message) }), "1", "Error", string.Empty));
            }
            finally
            {
                if (conn.State == ConnectionState.Open)
                {
                    conn.Close();
                }
            }
            return pairMaskFile;
        }


        private bool insertRecordInDatabaseBulk(string xUserTime, string xUserx, string xUserIP, string xUserApplication, string xUserSender, string xCodeMission, string xLocation, string xCodeAdapter, string xCodeSystem, string xMacMain, string xRecordType, string xFirstTimeSeen, string xLastTimeSeen, string xChannel, string xSpeed, string xPrivacy, string xCipher, string xAuthentication, string xPower, string xBeacons, string xIv, string xLanIP, string xIdLength, string xSSID, string xKey, string xPackets, string xMacAP, string xMessage, string xObservations, string xIDFileName, string xLatitude, string xLongitude, string xImportFlag, List<LogInfo> logList)
        {
            //var conn = new NpgsqlConnection(connectionString);
            try
            {
                if (xLongitude == "0" || xLongitude == "")
                {
                    xLongitude = "NULL";
                }
                if (xLatitude == "0" || xLatitude == "")
                {
                    xLatitude = "NULL";
                }
                xSSID = xSSID.Replace("'", "''");

                using (NpgsqlBinaryImporter importer = connMain.BeginBinaryImport("COPY sWise.tRecordsTemp (cUserTime,cUser,cUserIP,cUserApplication,cUserSender,ccodemission,clocation,cCodeAdapter,cCodeSystem,cmacmain,crecordtype,cfirsttimeseen,clasttimeseen,cchannel,cspeed,cprivacy,ccipher,cauthentication,cpower,cbeacons,civ,clanip,cidlength,cessid,ckey,cpackets,cmacap,cmessage,cobservations,cidfilename,clatitude,clongitude,cimportflag) FROM STDIN (FORMAT BINARY)")) 
                {
                    importer.StartRow();
                    importer.Write(xUserTime);
                    importer.Write(xUserx);
                    importer.Write(xUserIP);
                    importer.Write(xUserApplication);
                    importer.Write(xUserSender);
                    importer.Write(xCodeMission);
                    importer.Write(xLocation);
                    importer.Write(xCodeAdapter);
                    importer.Write(xCodeSystem);
                    importer.Write(xMacMain);
                    importer.Write(xRecordType);
                    importer.Write(xFirstTimeSeen);
                    importer.Write(xLastTimeSeen);
                    importer.Write(xChannel);
                    importer.Write(xSpeed);
                    importer.Write(xPrivacy);
                    importer.Write(xCipher);
                    importer.Write(xAuthentication);

                    importer.Write(xPower);
                    importer.Write(xBeacons);
                    importer.Write(xIv);
                    importer.Write(xLanIP);
                    importer.Write(xIdLength);
                    importer.Write(xSSID);
                    importer.Write(xKey);

                    importer.Write(xPackets);
                    importer.Write(xMacAP);
                    importer.Write(xMessage);
                    importer.Write(xObservations);
                    importer.Write(xIDFileName);
                    importer.Write(xLatitude);

                    importer.Write(xLongitude);
                    importer.Write(xImportFlag);
                    //importer.Close();
                }
                return true;

               
            }
            catch (Exception ex)
            {
                logList.Add(new LogInfo("1", DateTime.Now.ToString("dd.MM.yyyy HH:mm:ss"), "1000", ipAdress, "1000", currentUserName, string.Format("Function fRecordSet_New: Error: {0}", new string[] { Convert.ToString(ex.Message) }), "1", "Error", string.Empty));
                if (ex.Message.Contains("Unable to read data from the transport"))
                {
                    #region Export Log to disk
                    if (logList.Count > 0)
                    {
                        var csv = new CsvExport<LogInfo>(logList);
                        csv.ExportToFile();
                    }
              
                    #endregion
                    Environment.Exit(0);
                }
                return false;
            }
            finally
            {
               
            }
        }

        private void ProcessTxtFiles()
        {
            var txtFileList = new List<FileInfo>();
            txtFileList.AddRange(getFilesFromTopDirectoryOnly(sourceDirWifiCSV, "*.txt"));

            if (txtFileList.Count > 0)
            {
                logList.Add(new LogInfo("1", DateTime.Now.ToString("dd.MM.yyyy HH:mm:ss"), "1000", ipAdress, "1000", currentUserName, string.Format("{0} file type...", new string[]{"Txt"}), "1", "Info", string.Empty));                
                logList.Add(new LogInfo("1", DateTime.Now.ToString("dd.MM.yyyy HH:mm:ss"), "1000", ipAdress, "1000", currentUserName, string.Format("Get all {0} TXT Files.....done.", new string[] { Convert.ToString(txtFileList.Count) }), "1", "Info", string.Empty));

                //create main connection
                Dictionary<string, int> pairFileMask = checkForFileMask(logList);               

                foreach (FileInfo file in txtFileList)
                {
                    #region Get Values from fileName
                    try
                    {
                        string[] fileNameValues = file.Name.Split('_');

                        if (fileNameValues.Length > 3)
                        {
                            txtMisiune = fileNameValues[0];
                            txtLocatie = fileNameValues[1];
                            txtCodAdapter = fileNameValues[2];
                        }
                        else
                        {
                            continue;
                        }
                    }
                    catch (Exception ex)
                    {                        
                        logList.Add(new LogInfo("1", DateTime.Now.ToString("dd.MM.yyyy HH:mm:ss"), "1000", ipAdress, "1000", currentUserName, string.Format("Critical error while getting values from TXT File name {0}. Error: {1}", new string[] { Convert.ToString(file.Name), ex.Message }), "1", "Error", string.Empty));
                        // forceContinueOnNextFile = true;
                        //break; //Must exit from foreach. Is a critical error and it should not continue
                    }
                    #endregion

                    try
                    {
                        logList.Add(new LogInfo("1", DateTime.Now.ToString("dd.MM.yyyy HH:mm:ss"), "1000", ipAdress, "1000", currentUserName, string.Format("Read TXT File {0} .....done.", new string[] { Convert.ToString(file.Name) }), "1", "Info", string.Empty));
                        if (isFileLocked(file))
                        {
                            continue;
                        }
                        logList.Add(new LogInfo("1", DateTime.Now.ToString("dd.MM.yyyy HH:mm:ss"), "1000", ipAdress, "1000", currentUserName, string.Format("TXT File {0}  is not locked.", new string[] { Convert.ToString(file.Name) }), "1", "Info", string.Empty));


                        StreamReader sr = new StreamReader(file.FullName);

                        string currentLine;
                        Boolean fisOk = true;
                        //currentline will be null when StreamReader reaches the end of file
                        while ((currentLine = sr.ReadLine()) != null && !forceContinueOnNextFile)
                        {
                            if (!currentLine.Equals(string.Empty))
                            {
                                string[] coords = currentLine.Split(' ');
                                if (coords.Length >= 2)
                                {
                                    double latit = Convert.ToDouble(coords[0]);
                                    double longit = Convert.ToDouble(coords[1]);

                                    var conn = new NpgsqlConnection(connectionString);
                                    try
                                    {
                                        conn.Open();
                                        logList.Add(new LogInfo("1", DateTime.Now.ToString("dd.MM.yyyy HH:mm:ss"), "1000", ipAdress, "1000", currentUserName, string.Format("Conexiune la baza de date realizata cu succes"), "1", "Info", string.Empty));

                                    }
                                    catch (Exception e)
                                    {
                                        logList.Add(new LogInfo("1", DateTime.Now.ToString("dd.MM.yyyy HH:mm:ss"), "1000", ipAdress, "1000", currentUserName, string.Format("Eroare la deschiderea conexiunii catre BD: {0}",new string[]{e.Message}), "1", "Error", string.Empty));
                                        continue;
                                    }

                                    #region Apel swise.flocationcenterset
                                    NpgsqlCommand cmd = new NpgsqlCommand(string.Format("select swise.flocationcenterset('{0}','{1}','{2}','{3}','{4}','{5}','{6}','{7}','{8}','{9}')", new string[] { DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss"), userApplication, ipAdress, applicationName, functionName, txtLocatie, longit + " " + latit, txtLocatie, "", "" }), conn);
                                    try
                                    {
                                        cmd.ExecuteScalar();
                                    }
                                    catch (Exception e)
                                    {
                                        logList.Add(new LogInfo("1", DateTime.Now.ToString("dd.MM.yyyy HH:mm:ss"), "1000", ipAdress, "1000", currentUserName, string.Format("Eroare la inserarea coordonatelor unei locatii noi: {0}", new string[] { e.Message }), "1", "Error", string.Empty));
                                    }
                                    #endregion


                                    #region apel swise.fequipemnetset_new
                                    NpgsqlCommand cmdEq = new NpgsqlCommand(string.Format("select swise.fequipmentset_new('{0}','{1}','{2}','{3}','{4}','{5}','{6}','{7}','{8}','{9}','{10}','{11}','{12}','{13}')", new string[] { DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss"), userApplication, ipAdress, applicationName, functionName, "", txtCodAdapter, "", txtCodAdapter, "", "1", txtLocatie, txtMisiune, "Enabled" }), conn);
                                    try
                                    {
                                        cmdEq.ExecuteScalar();
                                    }
                                    catch (Exception e)
                                    {
                                        //logList.Add(new LogInfo(DateTime.Now.ToString("dd.MM.yyyy HH:mm:ss"), ipAdress, currentUserName, string.Format("Eroare la inserarea coordonatelor unui echipament nou: {0}", e.Message), LoggingType.Error));
                                        logList.Add(new LogInfo("1", DateTime.Now.ToString("dd.MM.yyyy HH:mm:ss"), "1000", ipAdress, "1000", currentUserName, string.Format("Eroare la inserarea coordonatelor unui echipament nou: {0}", new string[] { e.Message }), "1", "Error", string.Empty));         
                                    }
                                    #endregion
                                }
                                else
                                {
                                    fisOk = false;
                                }

                            }
                        } //while
                        sr.Close();

                        if (fisOk == true)
                        {
                            //moveFileTo(file, "prelucrate");
                            if (moveFileTo(file, Path.Combine(destDirImported + (""), file.Name.ToLower().Replace(".txt", string.Format("_{0}{1}", new string[] { Convert.ToString(DateTime.Now.ToString("yyyyMMddHHmmss")), ".txt" })))))
                            {
                                logList.Add(new LogInfo("1", DateTime.Now.ToString("dd.MM.yyyy HH:mm:ss"), "1000", ipAdress, "1000", currentUserName, string.Format("TXT File {0} moved to {1} .....done.", new string[] { Convert.ToString(file.Name) }, Path.Combine(destDirImported + (""), file.Name.ToLower().Replace(".txt", ".txt"))), "1", "Info", string.Empty));                         
                            }
                            else
                            {
                                logList.Add(new LogInfo("1", DateTime.Now.ToString("dd.MM.yyyy HH:mm:ss"), "1000", ipAdress, "1000", currentUserName, string.Format("Error moving TXT File {0} to {1}", new string[] { Convert.ToString(file.Name) }, Path.Combine(destDirImported + (""), file.Name.ToLower().Replace(".txt", ".txt"))), "1", "Error", string.Empty));              
                            }
                        }
                        else
                        {
                            //moveFileTo(file, "necofnorme");
                            if (moveFileTo(file, Path.Combine(destDirFaulty + (""), file.Name.ToLower().Replace(".txt", string.Format("_{0}{1}", new string[] { Convert.ToString(DateTime.Now.ToString("yyyyMMddHHmmss")), ".txt" })))))
                            {
                                //logList.Add(new LogInfo(DateTime.Now.ToString("dd.MM.yyyy HH:mm:ss"), ipAdress, currentUserName, string.Format("TXT File {0} moved to {1} .....done.", new string[] { Convert.ToString(file.Name) }, Path.Combine(destDirFaulty + (""), file.Name.ToLower().Replace(".txt", ".txt"))), LoggingType.Info));
                                logList.Add(new LogInfo("1", DateTime.Now.ToString("dd.MM.yyyy HH:mm:ss"), "1000", ipAdress, "1000", currentUserName, string.Format("TXT File {0} moved to {1} .....done.", new string[] { Convert.ToString(file.Name) }, Path.Combine(destDirFaulty + (""), file.Name.ToLower().Replace(".txt", ".txt"))), "1", "Info", string.Empty));              
  
                            }
                            else
                            {
                                //logList.Add(new LogInfo(DateTime.Now.ToString("dd.MM.yyyy HH:mm:ss"), ipAdress, currentUserName, string.Format("Error moving TXT File {0} to {1}", new string[] { Convert.ToString(file.Name) }, Path.Combine(destDirFaulty + (""), file.Name.ToLower().Replace(".txt", ".txt"))), LoggingType.Error));
                                logList.Add(new LogInfo("1", DateTime.Now.ToString("dd.MM.yyyy HH:mm:ss"), "1000", ipAdress, "1000", currentUserName, string.Format("Error moving TXT File {0} to {1}", new string[] { Convert.ToString(file.Name) }, Path.Combine(destDirFaulty + (""), file.Name.ToLower().Replace(".txt", ".txt"))), "1", "Error", string.Empty));              
                            }
                        }

                        //finished to parse CSV file. Next check for error and if zero errors import in database
                    }
                    catch (Exception ex)
                    {
                        //logList.Add(new LogInfo(DateTime.Now.ToString("dd.MM.yyyy HH:mm:ss"), ipAdress, currentUserName, Convert.ToString(ex.Message), LoggingType.Info));
                        logList.Add(new LogInfo("1", DateTime.Now.ToString("dd.MM.yyyy HH:mm:ss"), "1000", ipAdress, "1000", currentUserName, string.Format("Error {0}", new string[] { ex.Message }), "1", "Info", string.Empty));
                    }
                }//foreach fisiere
            }
            else
            {
                logList.Add(new LogInfo("1", DateTime.Now.ToString("dd.MM.yyyy HH:mm:ss"), "1000", ipAdress, "1000", currentUserName, string.Format("{0} file type not found!", new string[] { "Txt" }), "1", "Info", string.Empty));                
            }

            #region Export Log to disk
            //if (logList.Count > 0)
            //{
            //    var csv = new CsvExport<LogInfo>(logList);
            //    csv.ExportToFile();
            //}
            #endregion
            //try
            //{
            //    if (connMain.State == ConnectionState.Open)
            //    {
            //        connMain.Close();
            //    }
            //}
            //catch (Exception)
            //{
            //    Close();
            //}
            //finally
            //{
            //    //SetTempWritingFlag(0);
            //}
        }

    }
}
