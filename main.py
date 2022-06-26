import requests
import pandas as pd
import numpy as np
import json
from openpyxl.workbook import Workbook


# function to use requests.post to make an API call to the subgraph url
def run_query(query, network):

    # endpoint where you are making the request
    request = requests.post('https://api.thegraph.com/subgraphs/name/divaprotocol/diva-{}'''.format(network),
                            json={'query': query})
    if request.status_code == 200:
        return request.json()
    else:
        raise Exception('Query failed. return code is {}.      {}'.format(request.status_code, query))


def extend_DataFrame(df, resp):
    df = pd.concat([df, pd.json_normalize(resp, ["data", "testnetUsers"])], ignore_index=True)
    return df


network = "ropsten"

def query(lastId):
    return """
    {
      testnetUsers(first: 1000, where: {id_gt: %s}) {
      id
      binaryPoolCreated
      linearPoolCreated
      convexPoolCreated
      concavePoolCreated
      liquidityAdded
      liquidityRemoved
      buyLimitOrderCreatedAndFilled
      sellLimitOrderCreatedAndFilled
      buyLimitOrderFilled
      sellLimitOrderFilled
      finalValueReported
      reportedValueChallenged
      positionTokenRedeemed
      feesClaimed
      feeClaimsTransferred
      }
    }""" % lastId


resp = run_query(query(0), network)
df = pd.json_normalize(resp, ["data", "testnetUsers"])
numberUsers = 0

while True:
    lastId = json.dumps(df.id.iloc[-1])
    if numberUsers == df.shape[0]:
        break
    numberUsers = df.shape[0]
    resp = run_query(query(lastId), network)
    df = extend_DataFrame(df, resp)


df["Points"] = df.apply(lambda x: np.count_nonzero(x[1:]) * 200, axis = 1)
df["Points"] = df["Points"].apply(lambda x: x * 1.5 if x == 3000 else x)


df.to_csv('TestnetPoints.csv', index=False)
df.to_excel('TestnetPoints.xlsx', index=False)

print("Total Users:", numberUsers)
print("Total Points Achieved:", int(df["Points"].sum()))
print("Total Users Achieving 4500 Points: ", df["Points"].value_counts()[4500])
