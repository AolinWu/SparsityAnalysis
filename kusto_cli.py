from azure.kusto.data import KustoClient, KustoConnectionStringBuilder
from azure.kusto.data._models import KustoResultTable
from azure.kusto.data.response import KustoResponseDataSet
from azure.kusto.data._models import KustoResultColumn, KustoResultRow
import pandas as pd
from typing import List, Any, Dict, Tuple

import datetime


class DBIdentifier:
    def __init__(self, cluster: str, db: str):
        self.cluster = cluster
        self.db = db

    def set_db(self, db: str):
        self.db = db

    def set_cluster(self, cluster: str):
        self.cluster = cluster

    def __eq__(self, other):
        return self.cluster == other.cluster and self.db == other.db

    def __hash__(self):
        return hash(self.cluster + self.db)

    def __str__(self):
        return f'{self.cluster}:{self.db}'


class Cluster:
    def __init__(self, dbs: List[Tuple[str]]):
        self.clients: Dict[DBIdentifier, Client] = dict()
        for db in dbs:
            cluster_name, db_name = db[0], db[1]
            self.clients[DBIdentifier(cluster_name, db_name)] = Client(cluster_name, db_name)

    def add_client(self, cluster: str, db: str):
        self.clients[DBIdentifier(cluster, db)] = Client(cluster, db)

    def get_client(self, cluster: str, db: str):
        identifier = DBIdentifier(cluster, db)
        return self.get_client_by_identifier(identifier)

    def get_client_by_identifier(self, identifier: DBIdentifier):
        return self.clients.get(identifier)

    def execute_query_df(self, db_identifier: DBIdentifier, query: str):
        assert self.get_client_by_identifier(db_identifier) is not None,f'{db_identifier} does not exists'
        self.get_client_by_identifier(db_identifier).execute_query_df(query)


class Client:
    def __init__(self, cluster: str, db: str):
        self.cluster = cluster
        self.db = db
        self.kcsb=KustoConnectionStringBuilder.with_aad_device_authentication(self.cluster)

    def get_cluster(self):
        return self.cluster

    def get_db(self):
        return self.db

    def set_cluster(self, new_cluster: str):
        self.cluster = new_cluster

    def set_db(self, new_db: str):
        self.db = new_db

    def execute_query(self, query: str) -> KustoResponseDataSet:
        with KustoClient(self.kcsb) as client:
            response = client.execute(self.db, query)
        return response

    def execute_query_df(self, query: str) -> pd.DataFrame:
        return self.table_to_df(
            self.execute_query(query).primary_results[0]
        )

    @staticmethod
    def table_to_df(table: KustoResultTable) -> pd.DataFrame:
        rows: List[KustoResultRow] = table.rows
        data: Dict[str, List[Any]] = {}
        for j in range(table.columns_count):
            column: KustoResultColumn = table.columns[j]
            column_values: List[Any] = list(map(lambda row: row._value_by_name[column.column_name], rows))
            data[column.column_name] = column_values
        return pd.DataFrame(data=data)


def get_service_name(formatted_signal_name: str):
    path = 'ServiceId_ServiceName.csv'
    df = pd.read_csv(path, sep=',', header=0, index_col=None)
    ans = df.loc[df['SliId'] == formatted_signal_name, "ServiceName"]
    assert len(ans.values) != 0, 'do not found corresponding service name'
    return ans.values[0]


def loading_raw_data(start_time: str, end_time: str, location_id: str,
                     slo_group_name: str, sli_signal_name: str, service_id: str) -> pd.DataFrame:
    formatted_signal_name = '.'.join([slo_group_name, sli_signal_name, 'Signal'])
    raw_data_query = f"""
       let StartTime_Param = datetime({start_time});
       let EndTime_Param = datetime({end_time});
       let LocationId_Param = '{location_id}';
       let ServiceName_Param = '{get_service_name(formatted_signal_name)}';
       let SliName_Param = '{formatted_signal_name}-{service_id}';
       let SliMetadataTableName = cluster('https://sloslimedatafollower.westus3.kusto.windows.net/').database('slislometadata').GetCombinedSLIMetadataFromV2AndV3(EndTime_Param)
            | extend SliName = strcat(SliId, "-", ServiceTreeId)
            | where ServiceName == ServiceName_Param and SliName == SliName_Param
            | project tableName = substring(NrtKustoTableName, 1, strlen(NrtKustoTableName)-2);
       let query = strcat(toscalar(SliMetadataTableName
              | project Ref = strcat("table(", tableName,")",
            "| extend LocationId = tolower(replace_string(LocationId, ' ', ''))",
            "| where EndTimeUtc >= datetime(",StartTime_Param,") and EndTimeUtc < datetime(",EndTime_Param,") and tolower(replace_string(LocationId, ' ', '')) == '", LocationId_Param, 
            "'","|sort by StartTimeUtc","|take 20000"
            )));
       let resultQuery = evaluate execute_query(".", query);
       resultQuery
    """
    cluster = 'https://slidataaiopshmppeuswc.westcentralus.kusto.windows.net/'
    db = 'slidata'
    client = Client(cluster, db)
    return client.execute_query_df(raw_data_query)


if __name__ == '__main__':
    # start_time = str(datetime.datetime.now() + datetime.timedelta(days=-10))
    # end_time = str(datetime.datetime.now())
    # location_id = 'eastus'
    # slo_group_name = 'Azure Kubernetes Service'
    # sli_signal_name = 'Etcd Availability'
    # service_id = 'f1d1800e-d38e-41f2-b63c-72d59ecaf9c0'
    # print(loading_raw_data(start_time, end_time, location_id, slo_group_name, sli_signal_name, service_id)['Value'])
    print(hash('Hello') == hash('Hello'))
