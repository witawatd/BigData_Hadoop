#!/usr/bin/env python
import sys

# input comes from STDIN (standard input)
for line in sys.stdin:
    rowlst = line.split(',')
    #print (len(rowlst),rowlst[0])
    #ori=rowlst[11]
    cancel=rowlst[43]
    year=rowlst[0]
    value = 1
    if cancel=='1.00':
        print( "%s\t%d" % (year, value) )
    else:
	continue

CREATE TABLE 46on (
Year INT,
Quarter INT,
Month INT,
DayofMonth INT,
DayOfWeek INT,
FlightDate String,
UniqueCarrier String,
AirlineID String,
Carrier String,
TailNum String,
FlightNum String,
Origin String,
OriginCityName String,
OriginState_1 String,
OriginState String,
OriginStateFips String,
OriginStateName String,
OriginWac String,
Dest String,
DestCityName String,
DestState_1 String,
DestState String,
DestStateFips String,
DestStateName String,
DestWac String,
CRSDepTime String,
DepTime String,
DepDelay String,
DepDelayMinutes String,
DepDel15 String,
DepartureDelayGroups String,
DepTimeBlk String,
TaxiOut String,
WheelsOff String,
WheelsOn String,
TaxiIn String,
CRSArrTime String,
ArrTime String,
ArrDelay String,
ArrDelayMinutes String,
ArrDel15 String,
ArrivalDelayGroups String,
ArrTimeBlk String,
Cancelled String,
CancellationCode String,
Diverted String,
CRSElapsedTime String,
ActualElapsedTime String,
AirTime String,
Flights String,
Distance String,
DistanceGroup String,
CarrierDelay String,
WeatherDelay String,
NASDelay String,
SecurityDelay String,
LateAircraftDelay String) 

ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE;
'''

SELECT Month, Origin, Count(*)
FROM 46on
WHERE Origin='"ORD"' and Cancelled= 1.00
GROUP BY Month, Origin
Order by Month;

ORD cancel mont
time $HADOOP_HOME/bin/hadoop jar hadoop-streaming-0.20.205.0.jar \
-file ORDcancel_month_map.py    -mapper ORDcancel_month_map.py \
-file reducer.py   -reducer reducer.py \
-input /user/ec2-user/final/456ontime.csv -output /user/ec2-user/final/ord/cancelbymonth
$HADOOP_HOME/bin/hadoop fs -cat /user/ec2-user/final/ord/cancelbymonth/part-00000

'''
ORD dear late year
time $HADOOP_HOME/bin/hadoop jar hadoop-streaming-0.20.205.0.jar \
-file departORDmap.py   -mapper departORDmap.py \
-file reducer.py   -reducer reducer.py \
-input /user/ec2-user/final/456ontime.csv -output /user/ec2-user/final/ord/deplateyear

$HADOOP_HOME/bin/hadoop fs -cat /user/ec2-user/final/ord/deplateyear/part-00000

SELECT Year, Origin, Count(*)
FROM 46on
WHERE Origin='"ORD"' and DepDel15= 1.00
GROUP BY Year, Origin;

SELECT Year, Origin, Count(*)
FROM 46on
WHERE Origin='"ORD"' and Cancelled= 1.00
GROUP BY Year, Origin
Order by Year;

time $HADOOP_HOME/bin/hadoop jar hadoop-streaming-0.20.205.0.jar \
-file year_depdelaymap.py   -mapper year_depdelaymap.py \
-file reducer.py   -reducer reducer.py \
-input /user/ec2-user/final/456ontime.csv -output /user/ec2-user/final/depdel_all_year

$HADOOP_HOME/bin/hadoop fs -cat /user/ec2-user/final/depdel_all_year/part-00000


count allyears
time $HADOOP_HOME/bin/hadoop jar hadoop-streaming-0.20.205.0.jar \
-file countyear.py   -mapper countyear.py \
-file reducer.py   -reducer reducer.py \
-input /user/ec2-user/final/456ontime.csv -output /user/ec2-user/final/allyearcount

$HADOOP_HOME/bin/hadoop fs -cat /user/ec2-user/final/allyearcount/part-00000


count allyears
time $HADOOP_HOME/bin/hadoop jar hadoop-streaming-0.20.205.0.jar \
-file cancel_map.py   -mapper cancel_map.py\
-file reducer.py   -reducer reducer.py \
-input /user/ec2-user/final/456ontime.csv -output /user/ec2-user/final/allyearcancel1

$HADOOP_HOME/bin/hadoop fs -cat /user/ec2-user/final/allyearcancel1/part-00000
