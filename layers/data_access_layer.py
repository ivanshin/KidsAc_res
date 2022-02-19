import gc
from sqlalchemy import create_engine, select
from sqlalchemy.orm import create_session, sessionmaker
from pandas import DataFrame


class DataLayer:
    status: int


    def setConnetionString(self, conn_string_):

        """ Connect to DB using connection string """

        # Connect to DB
        #engine = create_engine(f'{db_type}://{usr}:{paswd}@{addr}:{port}/{db_name}')
        engine = create_engine(conn_string_, echo= True, connect_args={'options': '-csearch_path={}'.format('analytics_v3')})
        
        # Create and open session
        Session = sessionmaker()
        Session.configure(bind=engine)
        session = Session()

        return session


    def getDataFromDB(self, session):  

        """ Get and save tables to .csv from DB """

        tables_list = ["analytics_v3.fact_subscriptions", "analytics_v3.fact_courses_activities", "analytics_v3.fact_app_usage_event"]
        result_file_names_list = ["db_fs.csv", "db_fca.csv", "db_fau.csv"] 

        for i in range(0,3):

            # Get 2,3-d tables from DB
            if i != 0:
                statement = f"SELECT * FROM {tables_list[i]} tb WHERE tb.auth_user_id in (SELECT DISTINCT user_id FROM {tables_list[0]})"
                result = session.execute(statement)
            
            # Get 1-st table from DB
            else:
                statement = f"SELECT * FROM {tables_list[i]}"
                result = session.execute(statement)


            # Save result to csv
            buffer = DataFrame(result, columns= result._metadata.keys)
            buffer.to_csv(f"{result_file_names_list[i]}", index=False, header= True)

            # Free mem
            del result
            del buffer
            gc.collect()

        session.close()
    
        #print(engine.table_names())



dl = DataLayer()
conn_string_ = "postgres://postgres:postgres@192.168.56.105:5432/KidsAnalyticsV3"

ses = dl.setConnetionString(conn_string_= conn_string_)
dl.getDataFromDB(session = ses)
