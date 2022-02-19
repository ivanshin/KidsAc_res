from sklearn.linear_model import LogisticRegression
from sklearn.ensemble import RandomForestClassifier, GradientBoostingClassifier
from pandas import DataFrame, read_csv
import joblib
import os
#from Data_preproc import main_preproc_pipeline


class Models_ensemble:
    
    models: list
    preds: DataFrame


    def __init__(self) -> None:

        """ Init models and random state """

        self.preds = DataFrame()
        self.r_state = 14
        self.models = [0] * 3 
        #self.models = [LogisticRegression(random_state= self.r_state), 
        #RandomForestClassifier(random_state= self.r_state), 
        #GradientBoostingClassifier(random_state= self.r_state)]

        self.models[0] = joblib.load(os.path.join('pretrained_models', 'model_lr.joblib'))
        self.models[1] = joblib.load(os.path.join('pretrained_models', 'model_rf.joblib'))
        self.models[2] = joblib.load(os.path.join('pretrained_models', 'model_gb.joblib'))

        
        
    async def make_predictions(self, df):

        """ Make prediction fro all models """

        self.preds['user_id'] = df['user_id']

        df = df.drop(columns= ['user_id'])


        for i in range(0, len(self.models)):
            self.preds[f'{self.models[i]}'] = self.models[i].predict(df.to_numpy())


        
    async def save_predictions_to_excel(self, out_filename):

        """ Save results to csv """


        # Update preds to all models == True
        #self.preds = self.preds[(self.preds[f'{self.models[0]}'] == True) | (self.preds[f'{self.models[1]}'] == True) | (self.preds[f'{self.models[2]}'] == True)]
        
        
        self.preds.to_excel(out_filename, header= True, index= False)




async def make_predicts_pipeline(path_to_preproccesed_table):

    """ Main predictions pipeline """
    
    ens = Models_ensemble()
    await ens.make_predictions(read_csv(os.path.join(path_to_preproccesed_table,'fs_fca_faue_active_usrs.csv')))
    return ens.preds.to_dict()

    

#res = make_preds_pipe()
#print('1')




#input_filename = 'fs_fca_faue_active_usrs.csv'
#
#ens = Models_ensemble()
#ens.make_predictions(read_csv(input_filename))
#ens.save_predictions_to_excel('output.xlsx')

