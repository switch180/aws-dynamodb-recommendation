import pandas as pd
import awswrangler as wr
from datetime import datetime, timedelta, date

def maxA(i, j):
    if i > j:
        return i
    else:
        return j


def minA(i, j):
    if i > j:
        return j
    else:
        return i


def decrease60(L):
    return any(x > y for x, y in zip(L, L[1:]))
def decrease15(L):
    return any(x > y for x, y in zip(L, L[1:]))





def estimateUnits(read, write,readutilization,writeutilization):
    
    #columns [metric_name,accountid,region,timestamp,name,units,unitps,estunit]
    finalreadcu = []
    count = 0
    finalwritecu = []
    prevread = read[0]
    prevwrite = write[0]
    finalwritecu += [prevwrite]
    finalreadcu += [prevread]
    prevread[7] = (prevread[6] / readutilization) * 100
    prevwrite[7] = (prevwrite[6] / writeutilization) * 100
    for i in range(1, len(read)):
        currentread = read[i]
        currentwrite = write[i]
        
        date_time_obj = currentread[3].to_pydatetime()
        midnight = date_time_obj.replace(hour=0, minute=0, second=0)
        if date_time_obj == midnight:
            count = 0
            
            
        # compare with prev val

        if i <= 2:
            currentread[7] = prevread[7]
            currentwrite[7] = prevwrite[7]
            #prevwrite = currentwrite
           # adding 1 - 2 records to final list
            finalreadcu += [currentread]
            finalwritecu += [currentwrite]
            continue
        # creating a list with last 2 records.
        last2read = [v[6] for v in list(read[i - 2: i])]
        last2write = [v[6] for v in list(write[i - 2: i])]

        last2maxread = max(last2read)
        last2maxwrite = max(last2write)
        last2minread = min(last2read)
        last2minwrite = min(last2write)
        maxVread = maxA((last2minread / readutilization) * 100, prevread[6])
       
        maxVwrite = maxA((last2minwrite/ writeutilization) * 100, prevwrite[6])
        # scale out based on last 2 min Units.


        if currentread[0] == 'ConsumedReadCapacityUnits':
            if maxVread == (last2minread / readutilization) * 100:

                currentread[7] = (last2maxread / readutilization) * 100
                

            else:

                currentread[7] = maxVread

        if currentwrite[0] == 'ConsumedWriteCapacityUnits':
            if maxVwrite == (last2minwrite / writeutilization) * 100:

                currentwrite[7] = (last2maxwrite / writeutilization) * 100
            else:

                currentwrite[7] = maxVwrite


        if i <= 14:
            prevread = currentread
            # print(i, current)
            finalreadcu += [currentread]
            prevwrite = currentwrite
            # print(i, current)
            finalwritecu += [currentwrite]
            continue
        # Create list from last 15 Consumed Units
        last15read = [v[6] for v in list(read[i - 15: i])]
        last15read2 = [v[7] for v in list(read[i - 15: i])]
        # print(last15)
        last15Maxread = max(last15read)
        # Create list from last 15 Consumed Units
        last15write = [v[6] for v in list(write[i - 15: i])]
        last15write2 = [v[7] for v in list(write[i - 15: i])]
        # print(last15)
        last15Maxwrite = max(last15write)
        if count < 4:
            if not decrease15(last15read2):
                currentread[7] = minA((last15Maxread / readutilization) * 100, currentread[6])
                if prevread[7] > currentread[7]:

                    count += 1                   

            if not decrease15(last15write2):
                currentwrite[7] = minA((last15Maxwrite / writeutilization) * 100, currentwrite[6])
                if prevwrite[7] > currentwrite[7]:
                    count += 1
                   
                    
        else:
            if i >= 60:
                # Create list from last 60 Consumed Units
                last60read = [v[7] for v in list(read[i - 60: i])]
                last60write = [v[7] for v in list(write[i - 60: i])]
                # if Table has not scale in in past 60 minutes then scale in
                if not decrease60(last60read) and not decrease60(last60write) :
                    currentread[7] = minA((last15Maxread / readutilization) * 100, currentread[6])
                    
                       
                if not decrease60(last60write) and  not decrease60(last60read):
                    currentwrite[7] = minA((last15Maxwrite / writeutilization) * 100, currentwrite[6])
                    
                    if prevread[7] > currentread[7] or prevwrite[7] > currentwrite[7] :

                            count += 1
                            

        prevread = currentread
        prevwrite = currentwrite
        # adding current row to the result list
        finalreadcu += [currentread]
        finalwritecu += [currentwrite]
    #print(finalreadcu)
    finalist = finalwritecu + finalreadcu
    return finalist



def estimate(df,readutilization,writeutilization):
    
    
    df['unitps'] = df['unit'] / 60
    df['estunit'] = 5

    name = df['name'].unique()
    finalcu = []
    for table_name in name:
        
        rcu = df.query("metric_name == 'ConsumedReadCapacityUnits' and name == @table_name")
        wcu = df.query("metric_name == 'ConsumedWriteCapacityUnits' and name == @table_name")
        rcu = ((rcu.sort_values(by='timestamp', ascending=True)).reset_index(drop=True)).values.tolist()
        wcu = ((wcu.sort_values(by='timestamp', ascending=True)).reset_index(drop=True)).values.tolist()
        if len(rcu) > 0 and len(wcu) > 0:
            print('estimating provisioned units for: ' + table_name)
            finalcu += estimateUnits(rcu,wcu, readutilization,writeutilization)
    if len(finalcu) > 0:
        finaldf = pd.DataFrame(finalcu)
        finaldf.columns = ['metric_name', 'accountid', 'region','timestamp',
                          'name', 'unit', 'unitps', 'estunit']
        return finaldf
    else:
        return None

