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
        # Messages will be received and run through DGA inferencing
        batch_start_time = int(round(time.time()))
        result_size = messages_df.shape[0]
        print("Processing batch size: " + str(result_size))

        edges_gdf = self.build_edges(messages_df)

        print(edges_gdf.head())

        # get major ports for each node
        src_ports = ports.major_ports(edges_gdf["src"], edges_gdf["src_port"])
        dst_ports = ports.major_ports(edges_gdf["dst"], edges_gdf["dst_port"])

        # calculate top 5 source ports
        src_ports_pd = src_ports.to_pandas()
        src_ports_pd = src_ports_pd.groupby("addr").apply(self.top_n).reset_index()
        src_ports_pd = src_ports_pd.rename(columns={"ports":"src_ports", "services":"src_services", "conns":"src_conns"})

        # calculate top 5 dest ports
        dst_ports_pd = dst_ports.to_pandas()
        dst_ports_pd = dst_ports_pd.groupby("addr").apply(self.top_n).reset_index()
        dst_ports_pd = dst_ports_pd.rename(columns={"ports":"dst_ports", "services":"dst_services", "conns":"dst_conns"})

        # create single node list
        nodes_pd = src_ports_pd.merge(dst_ports_pd, on=['addr'], how='outer')
        
        # build nodes dataframe for cugraph results
        cg_nodes_gdf = cudf.DataFrame()
        addr = edges_gdf["src"].append(edges_gdf["dst"]).drop_duplicates()
        addr = addr[clx.ip.is_ip(addr)]
        cg_nodes_gdf["addr"] = addr
        cg_nodes_gdf["id"] = clx.ip.ip_to_int(addr)

        # build edges dataframe for cuGraph graph
        cg_edges_gdf = cudf.DataFrame()
        cg_edges_gdf['src'] = clx.ip.ip_to_int(edges_gdf['src'])
        cg_edges_gdf['dst'] = clx.ip.ip_to_int(edges_gdf['dst'])
        cg_edges_gdf["data"] = 1.0

        # pagerank
        G = cugraph.Graph()
        G.from_cudf_edgelist(cg_edges_gdf, source="src", destination="dst", renumber=True)
        pr = cugraph.pagerank(G, alpha=0.85, max_iter=500, tol=1.0e-05)
        pr['id'] = pr['vertex']
        cg_nodes_gdf = cg_nodes_gdf.merge(pr, on=['id'], how='left')
        cg_nodes_gdf = cg_nodes_gdf.drop(['vertex'], axis=1)

        # lovain
        G = cugraph.Graph()
        G.from_cudf_edgelist(cg_edges_gdf, source="src", destination="dst", renumber=True)
        lv_gdf, lv_mod = cugraph.louvain(G) 
        part_ids = lv_gdf["partition"].unique()
        lv_gdf['id'] = lv_gdf['vertex']
        cg_nodes_gdf = cg_nodes_gdf.merge(lv_gdf, on=['id'], how='left')
        cg_nodes_gdf = cg_nodes_gdf.drop(['vertex'], axis=1)

        # merge cuGraph results with original nodes dataframe
        cg_nodes_pd = cg_nodes_gdf.drop(["id"], axis=1).to_pandas()
        nodes_pd = nodes_pd.merge(cg_nodes_pd, on=['addr'], how='left')

        return (nodes_pd, batch_start_time, result_size)

    
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

    
    def top_n(self, df, n=5):
        sorted_df = df.nlargest(n, "conns").sort_values(by="conns", ascending=False)
        return pd.Series([list(sorted_df["port"]), list(sorted_df["service"]), list(sorted_df["conns"])], ["ports","services","conns"])
    
    def build_edges(self, wel_df):
        edges_gdf = None

        for log in edge_defs:

            for e in log["edges"]:
                
                eventsDF = wel_df
                
                # if 'filters' in e:
                #     for f in e['filters']:
                #         eventsDF = eventsDF.query(f)
                                
                evt_edges_gdf = cudf.DataFrame()
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
