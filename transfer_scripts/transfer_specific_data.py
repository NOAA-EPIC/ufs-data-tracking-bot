from get_timestamp_data import GetTimestampData
from progress_bar import ProgressPercentage
from upload_data import UploadData
import sys

class TransferSpecificData():
    """
    Obtain directories for the datasets requested by the user.
    
    """
    
    def __init__(self, input_ts, bl_ts, ww3_input_ts, bmic_ts, linked_home_dir, data_dir): #platform="orion"):
        """
        Args: 
             linked_home_dir (str): User directory linked to the RDHPCS' root
                                    data directory.
             data_dir (str): Driectory of where the datasets will be sourced on RCHPCS.
        """
        # Establish locality of where the datasets will be sourced.
        if linked_home_dir == None:
            self.linked_home_dir = ""
        else:
            self.linked_home_dir = linked_home_dir
            
        # Data directory on RDHPCS.
        self.data_dir = data_dir
        
        ## If dev team decides to go by platform names with fixed paths, then ...
        #if platform == "orion":
        #    self.orion_rt_data_dir = self.linked_home_dir + "/work/noaa/nems/emc.nemspara/RT/NEMSfv3gfs/"
        #elif platform == "hera":
        #    self.hera_rt_data_dir = "/scratch1/NCEPDEV/nems/emc.nemspara/RT/NEMSfv3gfs/"
        #else:
        #    print("Select a different platform.")
    
        # Select timestamp dataset to transfer from RDHPCS on-disk to cloud.
        self.input_ts, self.bl_ts, self.ww3_input_ts, self.bmic_ts = input_ts, bl_ts, ww3_input_ts, bmic_ts
        self.filter2specific_ts_datasets = GetTimestampData(self.linked_home_dir + self.data_dir, None).get_specific_ts_files(input_ts, bl_ts, ww3_input_ts, bmic_ts)
        
        # Detect if data of interest was not given read permission by the UFS-WM code manager.
        if self.filter2specific_ts_datasets == {}:
            print("\n*NOTE: At least one of the data parent directory that was requested from on-prem was not set with readable permissions. Prevents full data migration to cloud. Contact the appropriate UFS-WM code manager to resolve this issue.\n")
            
        # Upload datasets requested by user. 
        UploadData(self.linked_home_dir + self.data_dir, self.filter2specific_ts_datasets, use_bucket='rt').upload_files2cloud()
        
        ## If dev team decides to go by platform names with fixed paths, then ...
        #if platform == "orion":
        #    UploadData(self.orion_rt_data_dir, self.filter2specific_ts_datasets, use_bucket='rt').upload_files2cloud()
        #if platform == "hera":
        #    UploadData(self.hera_rt_data_dir, self.filter2specific_ts_datasets, use_bucket='rt').upload_files2cloud()
    
        
if __name__ == '__main__': 
    
    # Obtain directories for the datasets requested by the user.
    input_ts, bl_ts, ww3_input_ts, bmic_ts = sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[4]
    linked_home_dir = sys.argv[5]
    data_dir = sys.argv[6]
    TransferSpecificData(input_ts, bl_ts, ww3_input_ts, bmic_ts, linked_home_dir, data_dir)
