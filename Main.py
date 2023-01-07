import pandas as pd
import os
import codecs
import re

workingDir = r'D:\Siadatian log cleaner'
os.chdir(workingDir)

from funcs import *
logsPath = r'D:\Siadatians Logs'

files = os.listdir(logsPath)
for file in files:
    with codecs.open(logsPath + '/' + file, 'r', 'UTF-8') as text:
        results = text.readlines()
        requests = [result.split('|') for result in results if re.search(pattern = '\[Information\] Request', string = result)]
        requestsDF = pd.DataFrame(requests, columns=['AppName', 'Method', 'Path', 'QueryString','UserName', 'Payload',  'CorrelationID', 'IPAddress', 'Token', 'RequestedTime'])
        requestsDF.AppName = requestsDF.AppName.str.split(':').str.get(4).str.strip()
        
        for col in requestsDF:
            requestsDF[col] = requestsDF[col].str.strip()
        
        
        responses = [result for result in results if re.search(pattern = '\[Information\] Response', string = result)]
        # responses = [result.split('|') for result in results if re.search(pattern = '\[Information\] Response.*\n?', string = result)]
        
        twoLinedResponseIndex = []
        for ind in range(len(results)):
            if results[ind] == '[...]\r\n':
                twoLinedResponseIndex.append(ind)

        twoLinedResponses = []
        firsLines = []
        for ind in twoLinedResponseIndex:
            firsLines.append(results[ind-1])
            twoLinedResponses.append(results[ind-1].strip() + results[ind+1])
            
        oneLinedResponses = [response for response in responses if response not in firsLines]
        finalResponses = oneLinedResponses + twoLinedResponses
        finalResponsesSplitted = [response.split('|') for response in finalResponses]
        
        problematicResponsesIDs = [ind for ind in range(len(finalResponsesSplitted)) if len(finalResponsesSplitted[ind]) > 9]

        for ID in problematicResponsesIDs:
            # print(finalResponsesSplitted[ID])
            start = [ind for ind in range(len(finalResponsesSplitted[ID])) if re.search('^/.*', finalResponsesSplitted[ID][ind].strip())][0]
            # print(start)
            end = [ind for ind in range(len(finalResponsesSplitted[ID])) if re.search('^\d+$', finalResponsesSplitted[ID][ind].strip())][0]
            # print(end)
            part = ''.join(finalResponsesSplitted[ID][(start+1):end])
            # print(''.join(finalResponsesSplitted[ID][(start+1):end]))
            finalResponsesSplitted[ID] = finalResponsesSplitted[ID][:(start+1)] + [part] + finalResponsesSplitted[ID][end:]
        
        responsesDF = pd.DataFrame(finalResponsesSplitted, columns=['AppName', 'IsSuccessStatusCode', 'Path', 'Response','ResponseCode', 'UserName',  'CorrelationID', 'ResponseTime', 'ElapsedTime'])
        responsesDF.AppName = responsesDF.AppName.str.split(':').str.get(4).str.strip()
        
        for col in responsesDF:
            responsesDF[col] = responsesDF[col].str.strip()
        
        responsesDF['Count'] = [int(re.findall('"totalItemCount":(\d+)', response)[0]) if re.search('"totalItemCount":\d+', response) else 1 for response in responsesDF.Response]
