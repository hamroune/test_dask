from dask_yarn import YarnCluster
from dask.distributed import Client
import dask.dataframe as dd
import gcsfs
# Create a cluster where each worker has two cores and eight GB of memory
cluster = YarnCluster(environment='environnement.tar.gz',
                      worker_vcores=2,
                      worker_memory="3GB")
# Scale out to ten such workers
cluster.scale(2)
# Connect to the cluster
client = Client(cluster)

df = dd.read_parquet('gs:///formation-clients/sample.parquet', engine="pyarrow")
df = df.persist()
print(df)



# gcloud compute ssh hadoop-formation	-m --project=after-yesterday-217007 -- -L 1080:hadoop-formation	-m:8088 -N -n
