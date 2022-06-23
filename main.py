import requests
import pandas as pd
import numpy as np



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
i=0
query = """
{
  testnetUsers(first: 1000) {
    id
    binaryPoolCreated
    linearPoolCreated
    convexPoolCreated
    concavePoolCreated
    liquidityAdded
    liquidityRemoved
    finalValueReported
    reportedValueChallenged
    positionTokenRedeemed
    feesClaimed
    feeClaimsTransferred
  }
}"""

resp = run_query(query, network)
df = pd.json_normalize(resp, ["data", "testnetUsers"])
numberUsers = 0

while True:
    if numberUsers == df.shape[0]:
        break
    numberUsers = df.shape[0]
    i=i+1
    query = """
    {
      testnetUsers(first: 1000, skip: %s) {
        id
        binaryPoolCreated
        linearPoolCreated
        convexPoolCreated
        concavePoolCreated
        liquidityAdded
        liquidityRemoved
        finalValueReported
        reportedValueChallenged
        positionTokenRedeemed
        feesClaimed
        feeClaimsTransferred
      }
    }"""% (i*1000)
    resp = run_query(query, network)
    df = extend_DataFrame(df,resp)


df["Points"] = df.apply(lambda x: np.count_nonzero(x[1:]) * 200, axis = 1)



print("Total Users:", numberUsers)
print("Total Points Achieved:", df["Points"].sum())
