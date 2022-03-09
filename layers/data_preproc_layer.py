
import numpy as np
import pandas as pd
from datetime import datetime
import gc
import os


def csv_reader(filename):

    """ Read single table from .csv """

    #try:
    #    df = pd.read_csv(filename)
    #except:
    #    return
        
    df = pd.read_csv(filename)

    return df


def csv_writer(preproc_df, out_filename):

    """ Write preprocessed DataFrame to .csv """

    try:
        preproc_df.to_csv(out_filename, index= False)
    except:
        return
    return


def fs_table_preproc(df_fs):

    """ "fact_subscriptions" table preprocessing """

    uni_uid = np.sort(df_fs['user_id'].unique())

    df_gb_uid = df_fs.groupby('user_id')

    # Count of used coupons 
    tmp = df_gb_uid.coupon.unique()
    coupons_used = []
    for val in tmp:
        coupons_used.append((len(val) - 1))

    # User had trial (bool)
    tmp = df_gb_uid.user_had_trial.sum()
    count_of_trials = []
    for uid_w in tmp:
        count_of_trials.append(uid_w >= 1)

    # Resulting DataFrame with enginered features
    df_imp = pd.DataFrame( )
    df_imp['user_id'] = uni_uid

    df_imp['periods_count'] = df_gb_uid.start_date.count().values

    df_imp['first_plan_id'] = df_gb_uid.sub_plan_id.first().values
    df_imp['last_plan_id'] = df_gb_uid.sub_plan_id.last().values
    df_imp['bool_changed_plan'] = (df_imp.last_plan_id != df_imp.last_plan_id)

    df_imp['first_app_id'] = df_gb_uid.app_id.first().values
    df_imp['last_app_id'] = df_gb_uid.app_id.last().values

    df_imp['first_platform_id'] = df_gb_uid.platform_id.first().values
    df_imp['last_platform_id'] = df_gb_uid.platform_id.last().values

    df_imp['last_subs_status'] = df_gb_uid.sub_status_id.last().values

    df_imp['mean_paid_period_days'] = df_gb_uid.paid_period_days.mean().values

    df_imp['first_merchant'] = df_gb_uid.merchant_id.first().values
    df_imp['last_merchant'] = df_gb_uid.merchant_id.last().values
    df_imp['bool_changed_merchant'] = (df_imp.first_merchant != df_imp.last_merchant)

    df_imp['used_coupons_count'] = coupons_used

    df_imp['user_had_trial'] = count_of_trials


    df_imp['total_profit'] = df_gb_uid.profit.sum().values

    df_imp['bool_is_cancelled'] = (1 - df_gb_uid.cancelled_at.last().isna()).values
    df_imp['bool_autorenew_off'] = (1 - df_gb_uid.autorenew_off_at.last().isna()).values

    df_imp['still_active'] = (df_gb_uid.end_date.last() > str(datetime.now())).values
    df_imp.still_active.fillna(0)

    return df_imp


def fca_table_preproc(df_fca):

    """ "fact_courses_activities" table preprocessing """

    # Drop useless samples
    df_fca = df_fca.drop(df_fca[df_fca.auth_user_type == 'school_admin'].index)
    df_fca = df_fca.drop(df_fca[df_fca.auth_user_type == 'teacher'].index)

    uni_uid = np.sort(df_fca.auth_user_id.unique())

    df_gr_uid = df_fca.groupby('auth_user_id')

    df_fca_res = pd.DataFrame()
    df_fca_res['user_id'] = uni_uid

    df_fca_res['count_course_activity_id'] = df_gr_uid.course_activity_id.count().values
    df_fca_res['median_cource_grade_id'] = df_gr_uid.course_grade_id.median().values

    df_fca_res['first_client_version_id'] = df_gr_uid.client_version_id.first().values
    df_fca_res['last_client_version_id'] = df_gr_uid.client_version_id.last().values

    df_fca_res['last_geo_id'] = df_gr_uid.geo_id.last().values

    df_fca_res['count_content_id'] = df_gr_uid.content_id.count().values

    return df_fca_res


def faue_table_preproc(df_faue):

    """ "fact_app_usage_event" preprocessing """

    df_grby_uid = df_faue.groupby('auth_user_id')

    # Bool "is member of any campaign?"
    b_memb_of_cmp = []
    for arr in df_grby_uid.campaign.unique().values:
        if 'UNATTRIBUTED' in arr:
            b_memb_of_cmp.append(0)
            continue
        if pd.isna(arr[-1]):
            b_memb_of_cmp.append(np.nan)
        else:
            b_memb_of_cmp.append(1)
    
    # Bool "is school?"
    b_is_school = []
    for arr in df_grby_uid.school_id.unique().values:
        if pd.isna(arr[-1]):
            b_is_school.append(0)
        else:
            b_is_school.append(1)

    df_faue_res = pd.DataFrame()
    df_faue_res['user_id'] = np.sort(df_faue.auth_user_id.unique())
    df_faue_res['kids_amount'] = df_grby_uid.kids_amount.median().values
    df_faue_res['total_usage_steps'] = df_grby_uid.usage_step_id.nunique().values
    df_faue_res['bool_isMember_of_campaign'] = b_memb_of_cmp
    df_faue_res['bool_is_school'] = b_is_school

    return df_faue_res


def join_result_tables():

    """ Join (left) preprocessed tables into one """

    f_fs = pd.read_csv('pp_fs.csv')
    f_fca = pd.read_csv('pp_fca.csv')
    f_faue = pd.read_csv('pp_faue.csv')

    merged_res = f_fs.merge(f_fca, how= 'left', validate='one_to_one', on= 'user_id')
    print(merged_res.shape)
    merged_res = merged_res.merge(f_faue, how= 'left', validate='one_to_one', on= 'user_id')
    print(merged_res.shape)
    
    merged_res = merged_res.drop(merged_res[merged_res['still_active'] == False].index)

    return merged_res
    

def final_data_cleaning(df):
    
    """ Final prerocessing step """

    df.first_app_id.fillna(df.first_app_id.mode().item(), inplace= True)
    df.last_app_id.fillna(df.last_app_id.mode().item(), inplace= True)

    df.count_course_activity_id.fillna(-1, inplace= True)
    df.median_cource_grade_id.fillna(-1, inplace= True)
    df.drop(columns= ['first_client_version_id', 'last_client_version_id', 'last_geo_id'], inplace= True)
    df.count_content_id.fillna(-1, inplace= True)
    df.kids_amount.fillna(-1, inplace= True)
    df.total_usage_steps.fillna(-1, inplace= True)
    df.bool_isMember_of_campaign.fillna(-1, inplace= True)
    df.bool_is_school.fillna(-1, inplace= True)

    
    active_users = df[df['still_active'] == True]
    
    active_users = active_users.drop(columns=
    
                  ['still_active', 'bool_is_cancelled', 'mean_paid_period_days'])
    
    
    
    return active_users
    
    #return df


def main_preproc_pipeline(path_to_tables):
    
    """ Main pipeline """

    input_filenames = [os.path.join(path_to_tables,"db_fs.csv"), os.path.join(path_to_tables,"db_fca.csv"), os.path.join(path_to_tables,"db_faue.csv")]
    output_filenames = [f"pp_fs.csv", f"pp_fca.csv", f"pp_faue.csv"]
    
    for i in range(0,3):
    
        df = csv_reader(input_filenames[i])
    
        if i == 0:
            result = fs_table_preproc(df)
    
        elif i == 1:
            result = fca_table_preproc(df)
    
        elif i == 2:
            result = faue_table_preproc(df)
    
        if i == 2:
            result = faue_table_preproc(df)
    
        csv_writer(result, output_filenames[i])
    
        del df
        del result
        gc.collect()

    result = join_result_tables()
    result = final_data_cleaning(result)


    csv_writer(result, "fs_fca_faue_active_usrs.csv")


#main_preproc_pipeline('tmp_data')
