from get_timestamp_data import GetTimestampData
from progress_bar import ProgressPercentage
from upload_data import UploadData


class TransferSpecificData():
    """
    Obtain directories for the datasets requested by the user.
    
    """
    
    def __init__(self, input_ts, bl_ts, ww3_input_ts, bmic_ts, linked_home_dir, platform="orion"):
        """
        Args: 
             linked_home_dir (str): User directory linked to the RDHPCS' root
                                    data directory.
             platform (str): RDHPCS of where the datasets will be sourced.
        """
    
        # Establish locality of where the datasets will be sourced.
        self.linked_home_dir =  linked_home_dir

        if platform == "orion":
            self.orion_rt_data_dir = self.linked_home_dir + "/work/noaa/nems/emc.nemspara/RT/NEMSfv3gfs/"
        else:
            print("Select a different platform.")
    
        # Select timestamp dataset to transfer from RDHPCS on-disk to cloud
        self.input_ts, self.bl_ts, self.ww3_input_ts, self.bmic_ts = input_ts, bl_ts, ww3_input_ts, bmic_ts
        self.filter2specific_ts_datasets = GetTimestampData(self.orion_rt_data_dir, None).get_specific_ts_files(input_ts, bl_ts, ww3_input_ts, bmic_ts)

        # Upload datasets requested by user.
        UploadData(self.orion_rt_data_dir, self.filter2specific_ts_datasets, use_bucket='rt').upload_files2cloud()
        #print("Reach to upload")        
        #print(self.filter2specific_ts_datasets.keys())
        #print(self.filter2specific_ts_datasets)
    
        
if __name__ == '__main__': 
    
    # Obtain directories for the datasets requested by the user.
    input_ts, bl_ts, ww3_input_ts, bmic_ts = [], ['develop-20220304'], [], []
    TransferSpecificData(input_ts, bl_ts, ww3_input_ts, bmic_ts, linked_home_dir="/home/schin", platform="orion")
