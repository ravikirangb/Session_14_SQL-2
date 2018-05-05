import pandas as pd

import sqlalchemy
from sqlalchemy import create_engine
engine = create_engine('sqlite:///:memory:', echo=True)
conn = engine.connect()

from sqlalchemy import Column, Integer, String

from sqlalchemy.ext.declarative import declarative_base
Base = declarative_base()



#1 Read the following data set: https://archive.ics.uci.edu/ml/machine-learning-databases/adult/adult.data

df = pd.read_csv('D:\\Backup_INLT0488\\MyDoc_DownloadsFromCDrive\\Desktop\\Downloads\\SQL-2\\Assignment\\audultData.csv', sep=',', header=None, names=['age','workclass','fnlwgt','education','education_num','marital_status','occupation','relationship','race','sex','capital_gain','capital_loss','hours_per_week','native_country','class'])
print (df)
#
df_obj = df.select_dtypes(['object'])
df[df_obj.columns] = df_obj.apply(lambda x: x.str.strip())
df = df.rename(columns={'class': 'Income'})
print (df)


class Adults(Base):
    __tablename__ = 'SocialData'

    Id = Column(Integer, primary_key = True)
    age = Column(Integer)
    workclass = Column(String)
    fnlwgt = Column(Integer)
    education = Column(String)
    education_num = Column(Integer)
    marital_status = Column(String)
    occupation = Column(String)
    relationship = Column(String)
    race = Column(String)
    sex = Column(String)
    capital_gain = Column(Integer)
    capital_loss = Column(Integer)
    hours_per_week = Column(Integer)
    native_country = Column(String)
    Income = Column(String)

    def __repr__(self):
        return """<User(age='%d', workclass='%s', 
                        fnlwgt='%d', education='%s', 
                        education_num='%d', marital_status='%s', 
                        occupation='%s', relationship='%s', 
                        race='%s', sex='%s', capital_gain='%d',
                        capital_loss='%d', hours_per_week='%d', 
                        native_country='%s', Income='%s')>""" % (
                        self.age, self.workclass, self.fnlwgt,
                        self.education, self.education_num, self.marital_status,
                        self.occupation, self.relationship, self.race,
                        self.sex, self.capital_gain, self.capital_loss,
                        self.hours_per_week, self.native_country, self.Income)



Adults.__table__


Base.metadata.create_all(engine)

from sqlalchemy.orm import sessionmaker

TargetTable = 'SocialData'

#create Index column to use as primary key
df.reset_index(inplace=True)
df.rename(columns={'index':'Id'}, inplace =True)

list_to_write = df.to_dict(orient='records')

print (list_to_write)

metadata = sqlalchemy.schema.MetaData(bind=engine)
table = sqlalchemy.Table(TargetTable, metadata, autoload=True)

# Open session
Session = sessionmaker(bind=engine)
session = Session()

# Insert the dataframe
conn.execute(table.insert(), list_to_write)

# Commit the changes
session.commit()

# close the session
session.close()


from sqlalchemy import and_, or_, not_

#####################################################
# Write two basic update queries
######################################################
session.query(Adults).filter( or_(
        Adults.education == '10th'
    )).update({"education": 'Junior High School'})
  
session.commit()

session.query(Adults).filter( or_(
        Adults.education == '1st-4th',
    )).update({"education": 'Primary School'})
  
session.commit()


###
# Write two delete queries
# ### 
session.query(Adults).filter(Adults.workclass == '?').delete()

session.query(Adults).filter(Adults.occupation == '?').delete()


### FILTER QUERY - count adults with a masters degrees and a private job ##
session.query(Adults).filter( and_(
        Adults.workclass=='Private',
        Adults.education=='Masters'
    )).count()



### 
# Write two filter queries
###

session.query(Adults).filter( and_(
        Adults.marital_status.like('%Married%'),
        Adults.sex == 'Male'
    )).count()

print("Filter Query::: \n\n\n", session.query(Adults).filter( and_( Adults.marital_status.like('%Married%'),Adults.sex == 'Male')).count())

### 
# 
# Write two function queries
# 
#  ###

from sqlalchemy import func

session.query(func.count('*')).select_from(Adults).scalar()
session.query(Adults.workclass, func.count(Adults.Id)).group_by(Adults.workclass).all()
print ("\n\n\n function:: output :::: \n\n\n", session.query(Adults.workclass, func.count(Adults.Id)).group_by(Adults.workclass).all())

