from dask_yarn import YarnCluster
from dask.distributed import Client
import dask.dataframe as dd
import gcsfs
# Create a cluster where each worker has two cores and eight GB of memory
cluster = YarnCluster(environment='environnement.tar.gz',
                      worker_vcores=2,
                      worker_memory="3GB")
# Scale out to ten such workers
cluster.scale(3)
# Connect to the cluster
client = Client(cluster)


def compute_departure_timestamp(df):
    hours = df.CRSDepTime // 100
    hours_timedelta = pd.to_timedelta(hours, unit='h')

    minutes = df.CRSDepTime % 100
    minutes_timedelta = pd.to_timedelta(minutes, unit='m')

    return df.Date + hours_timedelta + minutes_timedelta



df = dd.read_csv('gs:///formation-clients/sample.csv.zip')
df = df.persist()
progress(df)



# gcloud compute ssh hadoop-formation	-m --project=after-yesterday-217007 -- -L 1080:hadoop-formation	-m:8088 -N -n
