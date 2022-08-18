import sys
sys.path.append( '../transfer_scripts' )
sys.path.append('../tracker_scripts/track_ts')
from datetime import datetime, timedelta
import pickle
import itertools
from get_timestamp_data import GetTimestampData
from progress_bar import ProgressPercentage
from upload_data import UploadData


class RtTrackerFilter():
    
    """
    Define window to filter cloud datasets by retrieval date In this scenario, capturing latest 60 days of data.
    
    """
    
    def __init__(self, linked_home_dir, platform="orion"):

        # Recall data tracker bot's latest log/dictionary file featuring timestamped data residing in cloud.
        with open("../tracker_scripts/track_ts/latest_rt.sh.pk", 'rb') as handle:
            self.data_log_dict = pickle.load(handle)  
       
        # Establish locality of where the datasets will be sourced.
        self.linked_home_dir =  linked_home_dir

        if platform == "orion":
            self.orion_rt_data_dir = self.linked_home_dir + "/work/noaa/nems/emc.nemspara/RT/NEMSfv3gfs/"
        else:
            print("Select a different platform.")
    
            
        # Filter to data tracker bot's timestamps & extract their corresponding UFS data file directories.
        self.filter2tracker_ts_datasets = GetTimestampData(self.orion_rt_data_dir, None).get_tracker_ts_files()
        print(f"\nLatest Retrieved Datasets' File Directories:\n{self.filter2tracker_ts_datasets}")
        
        # Instantiate wrapper for data purging.
        self.upload_wrapper = UploadData(self.orion_rt_data_dir, self.filter2tracker_ts_datasets, use_bucket='rt')
            

    def maintenance_window(self, thresh = 60):
        """
        Maintains the latest N days of retrieved datasets w/in the UFS-WM S3 cloud bucket.
        
        Preserves current data management process. Foldernames on-prem are formatted as follows:
        - 'BM_IC' # IC folder's prefix
        - 'develop' # Baseline folder's prefix
        - 'input-data' # Input folder's prefix
        - 'WW3_input_data' # WW3 Input folder's prefix

        Args: 
            thresh (int): Number of days to go back relative to the day at
                          which method is executed. Defines the window of
                          UFS-WM datasets to retain in cloud. Default: Set to 60
                          days to retain the past 60 days of retrieved data
                          from on-prem to cloud.
        Return: None
        
        """
        
        # First date within the range of interest.
        min_date = (datetime.today() - timedelta(days=thresh)).strftime('%m-%d-%Y')
        print(f"\nStart Date of Window of Interest: {min_date}")
        
        # Extract dates tracked by bot's log/dictionary of timestamped datasets within window of interest.
        dates_window = []
        for date, val in self.data_log_dict.items():
            if (datetime.strptime(date, '%m-%d-%Y') >= datetime.strptime(min_date, '%m-%d-%Y')) and datetime.strptime(date, '%m-%d-%Y') <= datetime.today():
                dates_window.append(date)
        
        # Filtered dates w/in bot's log/dictionary.
        self.maintenance_window_dict = {window_val: self.data_log_dict[window_val] for window_val in dates_window}
        
        # Map dictionary secondary keys to timestamps' nomenclature as set on on-prem data folders.
        data_fldrs_dict = {}
        for retrieval_date, ts_dict in self.maintenance_window_dict.items():
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
            data_fldrs_dict[retrieval_date] = [input_ts, bl_ts, ww3_input_ts, bmic_ts]

        # Unique timestamps within the latest N (thresh) days window.
        window_list_2d = list(itertools.chain.from_iterable(data_fldrs_dict.values()))
        self.n_days_ts_list = set(list(itertools.chain.from_iterable(window_list_2d)))
        
        # Determine all s3 object keys.
        all_bucket_objects = self.upload_wrapper.get_all_s3_keys()

        # Remove UFS-WM cloud bucket timestamps, which are outside of the N (thresh) window.
        out_of_window_ts = []
        for data_obj in all_bucket_objects:
            if (data_obj.split('/')[0] not in self.n_days_ts_list) and data_obj!='index.html':
                self.upload_wrapper.purge_by_keyprefix(data_obj.split('/')[0])
                print(f'{data_obj} Deleted')
                out_of_window_ts.append(data_obj)
            elif ('input-data' in data_obj.split('/')[0] and 'WW3' in data_obj.split('/')[1] and data_obj.split('/')[1] not in self.n_days_ts_list and data_obj!='index.html'):
                self.upload_wrapper.purge_by_keyprefix(data_obj.split('/')[0] + '/' + data_obj.split('/')[1])
                print(f'{data_obj} Deleted')
                out_of_window_ts.append(data_obj)
            else:
                continue
        if out_of_window_ts != []:
            print(f'\n ** UFS-WM RT Cloud Data w/ Start Retrieval Dates Out of Window Deleted: **\n{out_of_window_ts}') 
        else:
            print(f'\n ** All UFS-WM RT Cloud Data are w/in Requested Window (Latest {thresh} Days)! **\n') 
        
        return
    
if __name__ == '__main__': 
    
    # Maintain cloud datasets by retrieval date & retain data w/in latest 60 days of revisions.
    RtTrackerFilter(linked_home_dir="", platform="orion").maintenance_window(60)
