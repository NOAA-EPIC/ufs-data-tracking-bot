import os 
import pickle
from collections import defaultdict


class GetTimestampData():
    """
    Extract locality of the UFS datasets of interest & generate a dictionary which will
    map the UFS dataset files into the following dataset types:
    Input data, WW3 input data, Baseline data, and BMIC data. 
    
    """
    
    def __init__(self, hpc_dir, avoid_fldrs):
        """
        Args: 
            hpc_dir (str): Root directory path of where all the UFS timestamp datasets reside.
            avoid_fldrs (str): Foldername to ignore within main directory of interest on-prem.
                               Note: Some data folders were found w/ people's names within
                               them -- to be ignored.
            tracker_log_file (str): The folder directory containing the return of the UFS data 
                                    tracker bot.
        """
        
        # Datasets' main directory of interest. 
        self.hpc_dir = hpc_dir
        
        # Extract all data directories residing w/in datasets' main hpc directory.
        # Remove file directories comprise of a folder name.
        self.avoid_fldrs = avoid_fldrs
        self.file_dirs = self.get_data_dirs()
        
        # List of all data file directories w/in the UFS datasets.
        self.partition_datasets = self.get_input_bl_data()
        
        # List of all data folders/files in datasets' main directory of interest.
        self.rt_root_list = os.listdir(self.hpc_dir)
        print("\033[1m" +\
              f"\nAll Primary Dataset Folders & Files In Main Directory ({self.hpc_dir}):" +\
              f"\n\033[0m{self.rt_root_list}")

        # Recall latest updated file population recorded by the UFS tracker bot.
        self.tracker_log_file = "../tracker_scripts/track_ts/latest_rt.sh.pk" #<=== References log file generated by Bot (Prior to Upload)
        with open(self.tracker_log_file, 'rb') as log_file:      
            self.data_log_dict = pickle.load(log_file)  
            
#         # FOR TESTING PURPOSES [REMOVE AFTER TESTING]    
#         self.data_log_dict = {'06-30-2022': {'BL_DATE': ['20220623', '20220629'], 'INPUTDATA_ROOT': ['20220414'], 'INPUTDATA_ROOT_WW3': ['20220418'], 'INPUTDATA_ROOT_BMIC': ['20220207']}, '07-05-2022': {'BL_DATE': ['20220701'], 'INPUTDATA_ROOT': ['20220414'], 'INPUTDATA_ROOT_WW3': ['20220418'], 'INPUTDATA_ROOT_BMIC': ['20220207']}, '07-07-2022': {'BL_DATE': ['20220706'], 'INPUTDATA_ROOT': ['20220414'], 'INPUTDATA_ROOT_WW3': ['20220624'], 'INPUTDATA_ROOT_BMIC': ['20220207']}, '07-13-2022': {'BL_DATE': ['20220707', '20220713', '20220316'], 'INPUTDATA_ROOT': ['20220414'], 'INPUTDATA_ROOT_WW3': ['20220624'], 'INPUTDATA_ROOT_BMIC': ['20220207', '20210717']}}

        print('\033[1m' +\
              f"\nData Tracker's Latest Set of Timestamped Datasets Retrieved was on {max(self.data_log_dict)}:" +\
              '\033[0m' +\
              f"\n{self.data_log_dict[max(self.data_log_dict)]}")
        print('\033[1m' +\
              f"\nData Tracker's Retrieval Dates:\n" + '\033[0m' +\
              f"{self.data_log_dict.keys()}")
        print('\033[1m' +\
              f"\nData Tracker's Log of Timestamped Datasets (By Retrieval Date):\n" +'\033[0m' + f"{self.data_log_dict}")   

        # Filter data directory paths to timestamps recorded by the UFS data tracker bot.
        # For bot, refer to https://github.com/NOAA-EPIC/ufs-dev_data_timestamps.
        #self.filter2tracker_ts_datasets = self.get_tracker_ts_files()
        
        # Data files pertaining to specific timestamps of interest.
        # Select timestamp dataset(s) to transfer from RDHPCS on-disk to cloud
        #self.filter2specific_ts_datasets = self.get_specific_ts_files()
        
    def get_data_dirs(self):
        """
        Extract list of all file directories in datasets' main directory.
        
        Args:
            None
            
        Return (list): List of all file directories in datasets' main directory
        of interest.
        
        """
        
        # Generate list of all file directories residing w/in datasets' 
        # main directory of interest. 
        file_dirs = []
        file_size =[]
        #for root_dir, subfolders, filenames in os.walk(self.hpc_dir):
        for root_dir, subfolders, filenames in os.walk(self.hpc_dir, followlinks=True):
            for file in filenames:
                file_dirs.append(os.path.join(root_dir, file))
        
        # Removal of personal names.
        if self.avoid_fldrs != None:
            file_dirs = [x for x in file_dirs if any(x for name in self.avoid_fldrs if name not in x)]
        
        return file_dirs

    def get_input_bl_data(self):
        """
        Extract list of all input file & baseline file directories.

        Args: 
            None
            
        Return (dict): Dictionary partitioning the file directories into the
        dataset types.
        
        *Note: Will keep 'INPUTDATA_ROOT_WW3' as a key within the mapped dictionary
        -- in case, the NOAA development team decides to migrate WW3_input_data_YYYYMMDD
        out of the input-data-YYYYMMDD folder then, we will need to track the 
        'INPUTDATA_ROOT_WW3' related data files.
        
        # ********************************** REMARK (as of 07/14/22) ************************************                   
        # Filter out the prefix underscore. 
        # REMARK (as of 07/14/22): The EMC protocol of prefix underscore to datasets must be removed in near future !!!
        # It is not an ideal protocol and should not be used! WILL REMOVE CONDITION below WHEN CM (POC: Jong)
        # REMOVES THIS EMC PROTOCOL
        # ***********************************************************************************************   

        """
        
        # Extract list of all input file & baseline file directories.
        partition_datasets = defaultdict(list) 
        for file_dir in self.file_dirs:

           # ********************************** REMARK (as of 07/14/22) ************************************                   
           # Filter out the prefix underscore. 
           # REMARK (as of 07/14/22): The EMC protocol of prefix underscore to datasets must be removed in near future !!!
           # It is not an ideal protocol and should not be used! WILL REMOVE CONDITION below WHEN CM (POC: Jong)
           # REMOVES THIS EMC PROTOCOL
           # ***********************************************************************************************               
            if self.hpc_dir + '_' not in file_dir:
                
                # Input data files w/ root directory truncated.
                if any(subfolder in file_dir for subfolder in ['input-data', 'INPUT-DATA']):
                    partition_datasets['INPUTDATA_ROOT'].append(file_dir.replace(self.hpc_dir, ""))

                # Baseline data files w/ root directory truncated.
                if any(subfolder in file_dir for subfolder in ['develop', 'ufs-public-release', 'DEVELOP', 'UFS-PUBLIC-RELEASE']):
                    partition_datasets['BL_DATE'].append(file_dir.replace(self.hpc_dir, ""))

                # WW3 input data files w/ root directory truncated.
                if any(subfolder in file_dir for subfolder in ['WW3_input_data', 'ww3_input_data', 'WW3_INPUT_DATA']):
                    partition_datasets['INPUTDATA_ROOT_WW3'].append(file_dir.replace(self.hpc_dir, ""))

                # BM IC input data files w/ root directory truncated.
                if any(subfolder in file_dir for subfolder in ['BM_IC', 'bm_ic']):
                    partition_datasets['INPUTDATA_ROOT_BMIC'].append(file_dir.replace(self.hpc_dir, ""))        

        return partition_datasets    
    
    def get_tracker_ts_files(self):
        """
        Filters file directory paths related to timestamps obtained from UFS data tracker bot for latest retrival date.
        
        Args: 
            None

        Return (dict): Dictionary partitioning file directories into the
        timestamps of interest obtained from UFS data tracker bot.
        
        """
        
        # Map dictionary keys to the established ts names as shown on data folders on-prem.
        data_fldrs_dict = {}
        for retrieval_date, ts_dict in self.data_log_dict.items():

            # Initialize the list per retrieval day
            input_ts, bl_ts, ww3_input_ts, bmic_ts = [], [], [], []
            for ts_type, ts_day in ts_dict.items():
                for ts in ts_day:
                    if ts_type == 'BL_DATE' and f'develop-{ts}' not in bl_ts:
                        bl_ts.append(f'develop-{ts}')
                    elif ts_type == 'INPUTDATA_ROOT' and f'input-data-{ts}' not in input_ts:
                        input_ts.append(f'input-data-{ts}')
                    elif ts_type == 'INPUTDATA_ROOT_BMIC' and f'BM_IC-{ts}' not in bmic_ts:
                        bmic_ts.append(f'BM_IC-{ts}')
                    elif ts_type == 'INPUTDATA_ROOT_WW3' and f'WW3_input_data_{ts}' not in ww3_input_ts:
                         ww3_input_ts.append(f'WW3_input_data_{ts}')
            data_fldrs_dict[retrieval_date] = input_ts, bl_ts, ww3_input_ts, bmic_ts
            
        # Extract latest retrival date's recorded timestamped datasets. 
        input_ts, bl_ts, ww3_input_ts, bmic_ts = data_fldrs_dict[max(data_fldrs_dict)]
        print('\033[1m' + f"\nLatest Datasets Retrieved on {max(data_fldrs_dict)}:\n" +\
              '\033[0m' + f"{data_fldrs_dict}")

        # Create dictionary mapping data tracker's latest timestamps.
        tracker_ts_dict = defaultdict(list)
        tracker_ts_dict['INPUTDATA_ROOT'] = input_ts
        tracker_ts_dict['BL_DATE'] = bl_ts
        tracker_ts_dict['INPUTDATA_ROOT_WW3'] = ww3_input_ts
        tracker_ts_dict['INPUTDATA_ROOT_BMIC'] = bmic_ts
        
        # Reference timestamps captured from data tracker.
        filter2tracker_ts_datasets_primary = defaultdict(dict)
        
        # Reference timestamp by key be able to filter by timestamp.
        for dataset_type, timestamps in tracker_ts_dict.items():
            for ts in timestamps:
            
                # Extracts datafiles within the timestamps captured from data tracker.
                if dataset_type == 'INPUTDATA_ROOT':
                    input_dirs = []
                    for subfolder in self.partition_datasets[dataset_type]:
                        if ts in subfolder:
                            input_dirs.append(subfolder)
                            filter2tracker_ts_datasets_primary[dataset_type][ts] = input_dirs

                if dataset_type == 'BL_DATE':
                    bl_dirs = []
                    for subfolder in self.partition_datasets[dataset_type]:
                        if ts in subfolder:                        
                            bl_dirs.append(subfolder)
                            filter2tracker_ts_datasets_primary[dataset_type][ts] = bl_dirs

                if dataset_type == 'INPUTDATA_ROOT_WW3':
                    ww3_dirs = []
                    for subfolder in self.partition_datasets[dataset_type]:
                        if ts in subfolder:
                            ww3_dirs.append(subfolder)
                            filter2tracker_ts_datasets_primary[dataset_type][ts] = ww3_dirs

                if dataset_type == 'INPUTDATA_ROOT_BMIC':
                    bmic_dirs = []
                    for subfolder in self.partition_datasets[dataset_type]:
                        if ts in subfolder:
                            bmic_dirs.append(subfolder)
                            filter2tracker_ts_datasets_primary[dataset_type][ts] = bmic_dirs
                        
        return filter2tracker_ts_datasets_primary
    
    def get_specific_ts_files(self, input_ts, bl_ts, ww3_input_ts, bmic_ts):
        """
        Filters directory paths to timestamps of interest.
        
        Args: 
            input_ts (list): List of input timestamps to upload to cloud.
            bl_ts (list): List of baseline timestamps to upload to cloud.
            ww3_input_ts (list): List of WW3 input timestamps to upload to cloud.
            bmic_ts (list): List of BMIC timestamps to upload to cloud.
                                  
        Return (dict): Dictionary partitioning the file directories into the
        timestamps of interest specified by user.
        
        """
        
        # Create dictionary mapping the user's request of timestamps.
        specific_ts_dict = defaultdict(list)
        specific_ts_dict['INPUTDATA_ROOT'] = input_ts
        specific_ts_dict['BL_DATE'] = bl_ts
        specific_ts_dict['INPUTDATA_ROOT_WW3'] = ww3_input_ts
        specific_ts_dict['INPUTDATA_ROOT_BMIC'] = bmic_ts
        
        # Filter to directory paths of the timestamps specified by user.
        filter2specific_ts_datasets = defaultdict(list) 
        for dataset_type, timestamps in specific_ts_dict.items():
            
            # Extracts data files within the timestamps captured from data tracker.
            if dataset_type == 'INPUTDATA_ROOT':
                for subfolder in self.partition_datasets[dataset_type]:
                    if any(ts in subfolder for ts in timestamps):
                        filter2specific_ts_datasets[dataset_type].append(subfolder)

            if dataset_type == 'BL_DATE':
                for subfolder in self.partition_datasets[dataset_type]:
                    if any(ts in subfolder for ts in timestamps):
                        filter2specific_ts_datasets[dataset_type].append(subfolder)

            if dataset_type == 'INPUTDATA_ROOT_WW3':
                for subfolder in self.partition_datasets[dataset_type]:
                    if any(ts in subfolder for ts in timestamps):
                        filter2specific_ts_datasets[dataset_type].append(subfolder)

            if dataset_type == 'INPUTDATA_ROOT_BMIC':
                for subfolder in self.partition_datasets[dataset_type]:
                    if any(ts in subfolder for ts in timestamps):
                        filter2specific_ts_datasets[dataset_type].append(subfolder)
                        
        return filter2specific_ts_datasets    
    
