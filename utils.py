import pandas as pd
from tqdm import tqdm

from kusto_cli import Client


def cal_tp_fp_distribution(client: Client):
    step = 10
    FP_Prefix = 'FP'
    TP_Prefix = 'TP'
    query = f"""
            let ids=(cluster('https://slivdevoutput00usw3.westus3.kusto.windows.net/').database('outputs').SLISustainedDuration_v2
            |where sparse_rate >={0} and sparse_rate < {1})
            | join kind=leftouter (cluster('https://slivdevoutput00usw3.westus3.kusto.windows.net/').database('outputs').BrainAllIncident
            |where DetectionClassification startswith \\'{2}\\') on $left.AnomalyIncidentId == $right.IncidentId
            |summarize count()
          """
    df = pd.DataFrame(columns=['low_sparse_rate', 'high_sparse_rate', 'fp_count', 'tp_count'])
    i = 0
    for low_sparse_rate in tqdm(range(0, 100, step)):
        fp_query = query.format(low_sparse_rate, low_sparse_rate + step, FP_Prefix)
        tp_query = query.format(low_sparse_rate, low_sparse_rate + step, TP_Prefix)
        fp_num = client.execute_query_df(fp_query).loc[0, 'count_']
        tp_num = client.execute_query_df(tp_query).loc[0, 'count_']
        df.loc[i, 'low_sparse_rate'] = low_sparse_rate
        df.loc[i, 'high_sparse_rate'] = low_sparse_rate + step
        df.loc[i, 'fp_count'] = fp_num
        df.loc[i, 'tp_count'] = tp_num
        i = i + 1
    df.to_csv('re_sparse.csv', header=True, index=True)


if __name__ == '__main__':
    client = Client('https://slivdevoutput00usw3.westus3.kusto.windows.net/', 'outputs')
    cal_tp_fp_distribution(client)
