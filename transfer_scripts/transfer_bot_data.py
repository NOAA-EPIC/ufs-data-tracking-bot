from get_timestamp_data import GetTimestampData
from progress_bar import ProgressPercentage
from upload_data import UploadData
import sys

class TransferBotData():
    """
    Obtain directories for the datasets tracked by the data tracker bot.
    
    """
    def __init__(self, linked_home_dir, data_dir): #platform="orion"):
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
        #     self.hera_rt_data_dir = "/scratch1/NCEPDEV/nems/emc.nemspara/RT/NEMSfv3gfs/"
        #else:
        #    print("Select a different platform.")
    
            
        # Filter to data tracker bot's timestamps & extract their corresponding UFS data file directories.
        self.filter2tracker_ts_datasets = GetTimestampData(self.linked_home_dir + self.data_dir, None).get_tracker_ts_files()
        print(f"\nLatest Retrieved Datasets' File Directories:\n{self.filter2tracker_ts_datasets}")
        
        # Upload timestamped data which do not exist in cloud and pre-existing ts data on-prem.
        self.upload_dne_data()
        
        
    def upload_dne_data(self):
        """
        Upload timestamped data which do not exist in cloud and pre-existing ts data on-prem.
        
        Args:
            None
            
        Return: None
        
        """
        upload_wrapper = UploadData(self.linked_home_dir + self.data_dir, self.filter2tracker_ts_datasets, use_bucket='rt')
        
        # Determine all s3 object keys.
        all_bucket_objects = upload_wrapper.get_all_s3_keys()
        
        # Check if key is in cloud and if not, then add the key's values to Cloud
        # IF k2 does not exist as key in Cloud, then upload the directories of k2 (filter2tracker_ts_datasets[k2]):
        dne_data = []
        pre_exist_data = []
        for k, v in self.filter2tracker_ts_datasets.items():
            for ts_tracked, ts_files in v.items():
                
                # When a PR occurs regarding the ts datasets, must transfer all 
                # ts data to cloud. CM's way of notifying developers of updates to 
                # pre-existing ts data on-prem.
                if any(ts_tracked in s for s in all_bucket_objects):
                    print(f"\n{ts_tracked} Data Exist In Cloud.\n")
                    print(f'Syncing Pre-Existing {ts_tracked} Data to Cloud ...\n')
                    
                    # Remove pre-existing dataset residing in cloud by its key prefix 
                    # prior to transferring pre-existing dataset's new updated data.
                    # Reason: CMs may alter the data structure for a given ts dataset on-prem.
                    upload_wrapper.purge_by_keyprefix(ts_tracked)
                    
                    # Transfer new pre-existing dataset to cloud.
                    for file_dir in ts_files:
                        upload_wrapper.upload_single_file(file_dir)
                    print(f"*** Pre-Existing {ts_tracked} Synced to Cloud: Complete! ***") 
                    pre_exist_data.append(ts_tracked)
                    
                else:
                    
                    # Upload latest retrieval date's recorded timestamped datasets tracked by data tracker bot.
                    print(f'Currently, {ts_tracked} Data Does Not Exist In Cloud.\n')                     
                    print(f'Uploading {ts_tracked} Data to Cloud ...\n')
                    for file_dir in ts_files:
                        upload_wrapper.upload_single_file(file_dir)
                    print(f"{ts_tracked} Upload to Cloud: Complete!") 
                    dne_data.append(ts_tracked)
                    
        print(f'\nAdded New Timestamp Datasets Transferred to Cloud:\n{dne_data}')
        print(f'\nSynced Pre-Existing Timestamp Datasets within Cloud:\n{pre_exist_data}')
        
        return

if __name__ == '__main__': 
    
    # Obtain directories for the datasets requested by the bot.
    """
    Your Home Directory on RDHPCS: linked_home_dir = "/home/[USERNAME ON RDHPCS]"
    UFS-WM RT Data Directory on Orion: data_dir = "/work/noaa/nems/emc.nemspara/RT/NEMSfv3gfs/"
    UFS-WM RT Data Directory on Hera: data_dir = "/scratch1/NCEPDEV/nems/emc.nemspara/RT/NEMSfv3gfs/"
    """
    linked_home_dir = sys.argv[1]
    data_dir = sys.argv[2]
    TransferBotData(linked_home_dir, data_dir)
