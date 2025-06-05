



import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import os
import sys 

try: # for running in terminal
    f = os.path.dirname( 
                    os.path.dirname(os.path.abspath(__file__))
                    )
                
    sys.path.append(f)
    os.chdir(f)
except:
    pass


from annual_wildfire_susceptibility.partial_plots.partial_plots import PartialPlotByGiorgio
from risico_operational.settings import DATAPATH

# import json
# trhesolds = json.load(open(f'{BASEP}/susc_thresholds.json'))
# tr1, tr2 = trhesolds['lv1'], trhesolds['lv2']


# partial plot 
working_dir = f'{DATAPATH}/model/v1'


model_file = f'{working_dir}/RF_bil_100t_15d_15samples.sav'
ml_dataset_file =  f'{working_dir}/X_train.npy'

features = pd.read_csv(f'{working_dir}/X_merged_no_coords.csv', index_col=0).columns

training_npy = np.load(ml_dataset_file)
training_df = pd.DataFrame(training_npy, columns = features)

# get valeus
pp = PartialPlotByGiorgio(working_dir)
monthly_variable_names = [ 'SPI_1m', 'SPI_3m', 'SPI_6m',
                            'SPEI_1m', 'SPEI_3m', 'SPEI_6m',
                            'P_1m', 'P_3m', 'P_6m',
                            'Tanomaly_1m', 'Tanomaly_3m', 'Tanomaly_6m',
                            'T_1m', 'T_3m', 'T_6m',            
                            ]
    
feature_l = [f'{i}_m' for i in monthly_variable_names] + ['dem', 'perc_13', 'perc_14']

for feature in feature_l:
    print(f'doing {feature}')
    extreme_values = None

    values, average_predictions = pp.compute_values(model_file, training_df, feature, value_range = 20, add_extreme_values = extreme_values)

    # plot it
    fig, ax = pp.pplot(values, average_predictions, feature, extreme_values = None)

    # add some styling before saving

    # ax.axhline(tr2, color = 'pink', linestyle = '--', label = 'threshold for high susc')
    # ax.axhline(tr1, color = 'green', linestyle = '--', label = 'threshold for low susc')
    ax.legend(frameon = 'False')
    ax.set_ylim(0, max(average_predictions)+0.2)
    ax.set_xlim(xmin=training_df[feature].min())
    ax.set_xlabel(feature)
    ax.set_ylabel('average predictions')
    fig

    os.makedirs(f'{working_dir}/partial_plots', exist_ok = True)
    fig.savefig(f'{working_dir}/partial_plots/{feature}.png')

#%% make prediction of extreme

# new_values = {'CDD_y': 100, 'CWD_y': 1, 'T_average_y': 40,
#        'Tmax_avg_daily_y': 50, 'Rain_cum_y': 50, 'Wind_average_y': 10, 'RH_average_y': 20,

# }

# prediction = pp.make_avg_pred_custom_feture_vals(model_file, training_df, new_values) 

# myclass = 'high' if prediction > tr2 else 'medium' if prediction > tr1 and prediction <= tr2 else 'low'
# print(myclass)