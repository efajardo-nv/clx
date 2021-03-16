# Copyright (c) 2020, NVIDIA CORPORATION.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import time
import dask
import cudf
import cugraph
from clx.heuristics import ports
import clx.parsers.zeek as zeek
import clx.ip
import pandas as pd

from clx_streamz_tools import utils
from clx_streamz_tools import streamz_workflow

edge_defs = [
  {
    "dataSource": "winevt",
    "timeCol":"time",
    "edges": [
      {
        "filters": ["eventcode==5156"],
        "srcCol": "network_information_source_address",
        "srcNodeType": "address",
        "dstCol": "network_information_destination_address",
        "dstNodeType": "address",
        "relationship": "connect",
        "srcPortCol": "network_information_source_port",
        "dstPortCol": "network_information_destination_port"
      }
    ]
  }
]

class NetworkMappingWorkflow(streamz_workflow.StreamzWorkflow):
    def inference(self, messages_df):
        batch_start_time = int(round(time.time()))
        result_size = messages_df.shape[0]
        print("Processing batch size: " + str(result_size))

        edges_gdf = self.build_edges(messages_df)
        edges_pd = edges_gdf.to_pandas()

        edgelist_pd = pd.DataFrame({"edges": [edges_pd]})

        return (edgelist_pd, batch_start_time, result_size)

    
    def worker_init(self):
        # Initialization for each dask worker
        worker = dask.distributed.get_worker()
        print(
            "Initializing Dask worker: "
            + str(worker)
        )
        # this dict can be used for adding more objects to distributed dask worker
        obj_dict = {}
        worker = utils.init_dask_workers(worker, self.config, obj_dict)

    
    def build_edges(self, wel_df):
        edges_gdf = None

        for log in edge_defs:

            for e in log["edges"]:
                
                eventsDF = wel_df
                
                # if 'filters' in e:
                #     for f in e['filters']:
                #         eventsDF = eventsDF.query(f)
                                
                evt_edges_gdf = cudf.DataFrame()
                evt_edges_gdf['time'] = eventsDF[log["timeCol"]]
                evt_edges_gdf['src'] = eventsDF[e["srcCol"]]
                evt_edges_gdf['dst'] = eventsDF[e["dstCol"]]
                evt_edges_gdf['src_port'] = eventsDF[e["srcPortCol"]]
                evt_edges_gdf['dst_port'] = eventsDF[e["dstPortCol"]]
                evt_edges_gdf['src_node_type'] = e["srcNodeType"]
                evt_edges_gdf['dst_node_type'] = e["dstNodeType"]
                evt_edges_gdf['relationship'] = e["relationship"]
                evt_edges_gdf['data_source'] = log["dataSource"]
                
                if edges_gdf is None:
                    edges_gdf = evt_edges_gdf
                else:
                    edges_gdf = cudf.concat([edges_gdf, evt_edges_gdf])
        
        return edges_gdf

if __name__ == "__main__":
    network_mapping = NetworkMappingWorkflow()
    network_mapping.start()
