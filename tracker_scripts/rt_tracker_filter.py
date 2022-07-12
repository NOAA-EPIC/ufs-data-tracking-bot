from datetime import datetime, timedelta
import pickle

class RtTrackerFilter():
    
    """
    Track & extracts revisions made to UFS weather model's development branch data timestamps.
    
    """
    
    def __init__(self):

        # Recall latest updated file comprised of 
        # dictionary as of day at which class is executed.
        with open("./track_ts/latest_rt.sh.pk", 'rb') as handle:
            self.data_log_dict = pickle.load(handle)  
        
        # Troubleshooting Purposes: Remove only for troubleshooting purposes
        self.data_log_dict['07-01-2022'] = {'BL_DATE': ['20221122', '20221111'],
                                      'INPUTDATA_ROOT': ['20211112'],
                                      'INPUTDATA_ROOT_WW3': ['20211113'],
                                      'INPUTDATA_ROOT_BMIC': ['20220214']}

        self.data_log_dict['06-01-2022'] = {'BL_DATE': ['20221118', '20221117'],
                                       'INPUTDATA_ROOT': ['20211118'],
                                       'INPUTDATA_ROOT_WW3': ['20211113'],
                                       'INPUTDATA_ROOT_BMIC': ['20220214']}
        self.data_log_dict['05-01-2022'] = {'BL_DATE': ['20221101', '20221101'],
                               'INPUTDATA_ROOT': ['20211119'],
                               'INPUTDATA_ROOT_WW3': ['20211113'],
                               'INPUTDATA_ROOT_BMIC': ['20220214']}
        self.data_log_dict['04-29-2022'] = {'BL_DATE': ['20221118', '20221119'],
                               'INPUTDATA_ROOT': ['20211120'],
                               'INPUTDATA_ROOT_WW3': ['20211113'],
                               'INPUTDATA_ROOT_BMIC': ['20220214']}

    def maintenance_window(self, thresh = 60):
        """
        Args: 
            thresh (int): Number of days to go back relative to the day at
                          which class is executed.
        """
        # First date within the range of interest.
        min_date = (datetime.today() - timedelta(days=thresh)).strftime('%m-%d-%Y')
        print(f"First date within the range of interest: {min_date}")
        
        # Extract dates in dictionary within window of interest.
        dates_window = []
        for date, val in self.data_log_dict.items():
            if (datetime.strptime(date, '%m-%d-%Y') >= datetime.strptime(min_date, '%m-%d-%Y')) and datetime.strptime(date, '%m-%d-%Y') <= datetime.today():
                dates_window.append(date)
        
        # Filtered dates of dictionary.
        self.maintenance_window_dict = {window_val: self.data_log_dict[window_val] for window_val in dates_window}
        
        return