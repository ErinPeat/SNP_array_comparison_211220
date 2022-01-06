"""
This script has been written to export legacy CNV data from moka 
to be uploaded on to UCSC using pyodbc

"""
import os 
import pandas as pd
from ConfigParser import ConfigParser 
import pyodbc 

# Read config file(must be called config.ini and stored in the same directory as script)
config_parser = ConfigParser()
print_config = config_parser.read(os.path.join(os.path.dirname(os.path.realpath(__file__)), "config.ini"))

# Create pyodbc connection to moka 
cnxn = pyodbc.connect('DRIVER={{SQL Server}}; SERVER={server}; DATABASE={database};'.format(
         server=config_parser.get("MOKA", "SERVER"),
         database=config_parser.get("MOKA", "DATABASE")))
         

'''=================================================== SCRIPT =================================================== ''' 
# Query to pull data from moka 
export_patient_data_SQL = ("SELECT DISTINCT 'chr' + [Chr] AS Chromo, ArrayOligoPreliminaryResults.Start19," 
    "ArrayOligoPreliminaryResults.Stop19, Patients.PatientID, ArrayOligoPreliminaryResults.Pathogenic, "
    "Status.Status, [dbo].[gwv-patientlinked].[Gender], ArrayOligoPreliminaryResults.CNVTypeID, "
   " 'Patient result for' + ' ' + [Patients].[PatientID] AS PatientResult, "
    "[band19] + '(' + CONVERT(varchar(20), start19) + '-' + CONVERT(varchar(20), stop19) + ')' + [Change] AS [DESCtwo], " # Convert integers to make a string
    "Arrays.DateRecieved, [Patients].[PatientID] + [AltForBED] AS PatientIDInheritance, "
    "Phenotype, ArrayOligoPreliminaryResults.Copies, Change.Change "
    "FROM (((((((((Patients INNER JOIN ResultCode ON Patients.OverallResultCodeID = ResultCode.ResultCodeID) "
    "INNER JOIN ArrayOligoPreliminaryResults ON Patients.InternalPatientID = ArrayOligoPreliminaryResults.InternalPatientID) "
    "INNER JOIN Chromosome ON ArrayOligoPreliminaryResults.ChrID19 = Chromosome.ChrID) " 
    "INNER JOIN ArrayLabelling ON ArrayOligoPreliminaryResults.DNALabellingID = ArrayLabelling.DNALabellingID) "
    "INNER JOIN Arrays ON ArrayLabelling.ArrayID = Arrays.ArrayID) " 
    "INNER JOIN Status ON ArrayOligoPreliminaryResults.Pathogenic = Status.StatusID) "
    "INNER JOIN [dbo].[gwv-patientlinked] ON [dbo].[gwv-patientlinked].[PatientTrustID] = Patients.PatientID) " # Some patients aren't in GW?!
    "INNER JOIN Change ON Change.ChangeID = ArrayOligoPreliminaryResults.Copies) " # one patient has no copy information
    "LEFT JOIN Phenotype ON Phenotype.InternalPatientID = ArrayOligoPreliminaryResults.InternalPatientID) " # Some patients have no phenotype information
    "LEFT JOIN Inheritance ON Inheritance.InheritanceID = ArrayOligoPreliminaryResults.InheritanceID " # Some patients have no inheritance reported
    "WHERE (((ArrayOligoPreliminaryResults.WholeChromosome) Is Null Or (ArrayOligoPreliminaryResults.WholeChromosome)=-1128799521) " 
    "AND ((ArrayOligoPreliminaryResults.CNVTypeID)=1190384922 Or (ArrayOligoPreliminaryResults.CNVTypeID)=1190384964) " # Only 'confirmed' or 'reported' CNVs
    "AND ((Patients.OverallResultCodeID)<>1 And (Patients.OverallResultCodeID)<>1189679593)  " # Results code is not 'normal' or 'not reported'
    "AND ((ArrayOligoPreliminaryResults.Start19) Is Not Null) AND ((ArrayOligoPreliminaryResults.Stop19) Is Not Null) " 
    "AND ((ArrayOligoPreliminaryResults.Pathogenic)=1202218781 Or " # Class 3 variants
    "(ArrayOligoPreliminaryResults.Pathogenic)=1202218783 Or (ArrayOligoPreliminaryResults.Pathogenic)=1202218788) " # Class 4 and Class 5 variants
    "AND ((Arrays.DateRecieved) BETWEEN '2016-01-01 00:00:00' AND '2021-12-31 00:00:00'))") # For current array plaform

# Run SQL query and store results in a pandas dataframe 
export_patient_data_df = pd.read_sql(export_patient_data_SQL, cnxn)
print("Data pulled from moka")

# Save as a tsv file, some characters are not ascii so encoding in utf-8
export_patient_data_df.to_csv("moka_array_export_220106.tsv",sep = "\t", encoding='utf-8')
