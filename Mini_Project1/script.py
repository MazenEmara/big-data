from pymongo import MongoClient
from datetime import datetime
import numpy as np

cluster = MongoClient("mongodb+srv://WorldCup:Bola123@cluster0.0ma82ju.mongodb.net/test")
db=cluster["Trips"]
collection=db["cabs"]

import pandas as pd



#df = pd.read_csv('taxi_trip_data.csv',encoding='unicode_escape')

#df = df.iloc[:15000]

#df = df.drop(columns=['store_and_fwd_flag', 'rate_code', 'total_amount'])

#df.to_csv('taxitripdatanew.csv', index=False)

#df2 = pd.read_csv('taxitripdatanew.csv',encoding='unicode_escape')

#essentials= ['fare_amount', 'extra', 'mta_tax', 'tip_amount', 'tolls_amount', 'imp_surcharge', 'payment_type', 'tip_amount'  ,'pickup_datetime', 'dropoff_datetime']
#df2 = df2.dropna(subset=essentials)

#print(df.shape)
#print(df2.shape)

#pickup_datetime = pd.to_datetime(df2['pickup_datetime'])
#dropoff_datetime = pd.to_datetime(df2['dropoff_datetime'])
#duration = dropoff_datetime - pickup_datetime
#df2['duration'] = duration

#collection.insert_many(df2.to_dict('records'))


#for trip in collection.find():
        #pickup_datetime = datetime.strptime(trip['pickup_datetime'], '%Y-%m-%d %H:%M:%S')
        #dropoff_datetime = datetime.strptime(trip['dropoff_datetime'], '%Y-%m-%d %H:%M:%S')
        #duration = (dropoff_datetime - pickup_datetime).total_seconds()

        #collection.update_one(
                #{'_id': trip['_id']},
                #{'$set': {'duration': duration}}
#)

#for trip in collection.find():
        #total_cost = sum(trip[field] for field in ['fare_amount', 'extra', 'mta_tax', 'tip_amount', 'tolls_amount', 'imp_surcharge'])
        #collection.update_one({'_id': trip['_id']}, {'$set': {'total_cost': total_cost}})


pipeline = [
        {'$group': {'_id': '$passenger_count', 'average': {'$avg': '$tip_amount'}}}
]
#for result in list(collection.aggregate(pipeline)):
        #print(f"count: {result['_id']}, Average : {result['average']}")

morning = datetime.strptime('06:00:00', '%H:%M:%S').time().strftime('%H:%M:%S')
afternoon = datetime.strptime('12:00:00', '%H:%M:%S').time().strftime('%H:%M:%S')
evening = datetime.strptime('18:00:00', '%H:%M:%S').time().strftime('%H:%M:%S')

# group the documents by payment type and time of day
pipeline = [
{
        '$project': {
                'payment_type': 1,
                'pickup_time': {'$hour': {'$toDate': '$pickup_datetime'}},
                'dropoff_time': {'$hour': {'$toDate': '$dropoff_datetime'}}
        }
},
{
        '$addFields': {
                'pickup_time_of_day': {
                '$switch': {
                        'branches': [
                                {'case': {'$lt': ['$pickup_time', datetime.strptime(morning, '%H:%M:%S').hour]}, 'then': 'night'},
                                {'case': {'$lt': ['$pickup_time', datetime.strptime(afternoon, '%H:%M:%S').hour]}, 'then': 'morning'},
                                {'case': {'$lt': ['$pickup_time', datetime.strptime(evening, '%H:%M:%S').hour]}, 'then': 'afternoon'},
                                {'case': {'$gte': ['$pickup_time', datetime.strptime(evening, '%H:%M:%S').hour]}, 'then': 'evening'}
                        ],
                        'default': 'unknown'
                }
                },
                'dropoff_time_of_day': {
                '$switch': {
                        'branches': [
                                {'case': {'$lt': ['$dropoff_time', datetime.strptime(morning, '%H:%M:%S').hour]}, 'then': 'night'},
                                {'case': {'$lt': ['$dropoff_time', datetime.strptime(afternoon, '%H:%M:%S').hour]}, 'then': 'morning'},
                                {'case': {'$lt': ['$dropoff_time', datetime.strptime(evening, '%H:%M:%S').hour]}, 'then': 'afternoon'},
                                {'case': {'$gte': ['$dropoff_time', datetime.strptime(evening, '%H:%M:%S').hour]}, 'then': 'evening'}
                        ],
                        'default': 'unknown'
                }
                }
        }
        },
        {
        '$group': {
                '_id': {'time_of_day': '$pickup_time_of_day', 'payment_type': '$payment_type'},
                'count': {'$sum': 1}
        }
        },
        {
        '$sort': {'_id': 1}
        }
]

groupby = collection.aggregate(pipeline)

#for t in groupby:
        #print(t)
#it looks like that the payment type 1 is the most common in all of them

j=0
afternoon_arr=[0,0,0,0]
evening_arr=[0,0,0,0]
morning_arr=[0,0,0,0]
night_arr=[0,0,0,0]
for i in groupby:
        if(j==4):
                j=0
        if(i['_id']['time_of_day']=='afternoon'):
                afternoon_arr[j]=i['count']
        if(i['_id']['time_of_day']=='evening'):
                evening_arr[j]=i['count']
        if(i['_id']['time_of_day']=='morning'):
                morning_arr[j]=i['count']
        if(i['_id']['time_of_day']=='night'):
                night_arr[j]=i['count']
        j=j+1
#here is the count of payment 1 in each day time
print('afternoon payment type 1 :',np.max(afternoon_arr))
print('evening payment type 1 :',np.max(evening_arr))
print('morning payment type 1 :',np.max(morning_arr))
print('night payment type 1 :',np.max(night_arr))



query2 = "UPDATE Trips SET total_cost = fare_amount + extra + mta_tax + tip_amount + tolls_amount + imp_surcharge"
session.execute(query2)


query3 = "SELECT pickup_datetime, dropoff_datetime FROM Trip"

datetimes = session.execute(query3)
for row in datetimes:
        pickup_datetime = datetime.strptime(row.pickup_datetime, '%Y-%m-%d %H:%M:%S')
        dropoff_datetime = datetime.strptime(row.dropoff_datetime, '%Y-%m-%d %H:%M:%S')
        duration = (dropoff_datetime - pickup_datetime).total_seconds()
        
        # Define the CQL query to update the table with the trip duration
        update_query = "UPDATE Trip SET trip_duration = %s WHERE pickup_datetime = %s AND dropoff_datetime = %s"
        session.execute(update_query, (duration, row.pickup_datetime, row.dropoff_datetime))



query = "SELECT * FROM Trip"


df = pd.DataFrame(list(session.execute(query)))

df['total_cost'] = df['fare_amount'] + df['extra'] + df['mta_tax'] + df['tip_amount'] + df['tolls_amount'] + df['imp_surcharge']

query = "UPDATE Trip SET total_cost = %s WHERE vendor_id = %s AND pickup_datetime = %s AND dropoff_datetime = %s"

for row in df.itertuples(index=False):
        session.execute(query, (row.total_cost, row.vendor_id, row.pickup_datetime, row.dropoff_datetime))


query = "SELECT passenger_count, tip_amount FROM Trip"
rows=session.execute(query)
tip_amounts = {}
for row in rows:
        if row.passenger_count not in tip_amounts:
                tip_amounts[row.passenger_count] = [row.tip_amount]
        else:
                tip_amounts[row.passenger_count].append(row.tip_amount)

avg_tip_amounts = {}
for i, j in tip_amounts.items():
        avg_tip_amounts[i] = sum(j) / len(j)

print(avg_tip_amounts)


