import networkx as nx
import itertools as it
import os
import pickle
import logging
import multiprocessing

def _Edge_Subgraph_Generator(Graph):
#     Iterate through selecting 0, 1, 2, ... edges that will be selected from the edge set of the host graph
    for nEdges in range(Graph.size()+1):
#         For each given number of edges that will be selected, select each combination of that many edges from the host graph
        for Edges in it.combinations(Graph.edges(),nEdges):
#             Add the respective edge-induced subgraph to the list of all subgraphs
            yield nx.edge_subgraph(Graph, Edges).copy()
    
def _Unique_Edge_Subgraph_Generator(Graph):
#     Iterate through each edge-induced subgraph of the input graph
    UniqueSubgraphs = []
    for subgraph in _Edge_Subgraph_Generator(Graph):
        Unique = True
#         Check if the given subgraph is unique. We will assume it is until we find an isomorphic one
        for UniqueSubgraph in UniqueSubgraphs:
            if nx.is_isomorphic(UniqueSubgraph, subgraph):
                Unique = False
                break
#         Now that we've gone through the entire list (or short cirtuited) we can return this subgraph (if it's unique) or conginue to the next subgraph
        if Unique:
            UniqueSubgraphs.append(subgraph)
            yield subgraph
            
def _Return_Unique(Graphs):
    UniqueGraphs = []
#     Iterate through each of the input graphs
    for Graph in Graphs:
        Unique = True
#         Check if the graph is unique. We will assume it is until we find an isomorphic one
        for UniqueGraph in UniqueGraphs:
            if nx.is_isomorphic(UniqueGraph, Graph):
                Unique = False
                break
#         Now that we've gone through the entire list (or short circuited) we can return ass this graph to the list of unique graphs
        if Unique:
            UniqueGraphs.append(Graph)
    return UniqueGraphs

def _get_graph_from_graph_name(GraphName):
    if "K_" in GraphName:
        if "," in GraphName:
            n,m=GraphName.split(".",1)[0].split("K_",1)[1].split(",")
            n=int(n)
            m=int(m)
            return nx.complete_multipartite_graph(n,m)
        else:
            n=GraphName.split(".",1)[0].split("K_",1)[1]
            n=int(n)
            return nx.complete_graph(n)
    elif "C_" in GraphName:
            n=GraphName.split(".",1)[0].split("C_",1)[1]
            n=int(n)
            return nx.cycle_graph(n)
    elif "P_" in GraphName:
            n=GraphName.split(".",1)[0].split("P_",1)[1]
            n=int(n)
            return nx.path_graph(n)

def _decompose_graphs(Graphs):
    logging.basicConfig(filename=f"Default_Log.txt", level=logging.INFO, format=f'%(asctime)s [{multiprocessing.current_process().name}, {os.getpid()}] %(message)s')
    RootDir = os.getcwd()
    os.chdir("Graphs")
    BaseDir = os.getcwd()
    for GraphName in Graphs:
        if not os.path.exists(GraphName):
            os.mkdir(GraphName)
        os.chdir(GraphName)
        if os.path.exists(f"{GraphName}.UniqueGraphs.pickle"):
            logging.info(f"{GraphName} has already been decomposed, and all edge-induced subgraphs have been generated.")
            continue
        logging.info(f"Decomposing {GraphName}")
        HostGraph = _get_graph_from_graph_name(GraphName)
        UniqueGraphs = _Return_Unique(set(_Unique_Edge_Subgraph_Generator(HostGraph)))
        logging.info(f"There are {len(UniqueGraphs)} distinct, edge-induced subgraphs of {GraphName}")
        with open(f"{GraphName}.UniqueGraphs.pickle", "wb") as OutFile:
            pickle.dump(UniqueGraphs, OutFile)
        os.chdir(BaseDir)
    os.chdir(RootDir)
    return

def slice_per(source, step):
    return [source[i::step] for i in range(step)]

if __name__ == '__main__':
    logging.basicConfig(filename=f"Default_Log.txt", level=logging.INFO, format=f'%(asctime)s [{multiprocessing.current_process().name}, {os.getpid()}] %(message)s')
    logging.info("Starting...")
    Graphs = ["K_4,4", "K_4,5", "K_4,6", "K_5,4", "K_5,5", "K_5,6", "K_6,6", "K_6,7", "K_6,8", "K_7,7", "K_7,8", "K_7,9"]
    logging.info(f"Decomposing the graphs: {Graphs}")

    nWorkers = 4
    Workers = []
    Work = slice_per(Graphs, nWorkers)
    for ID in range(nWorkers):
        logging.info(f"Worker {ID} is being handed the list of graphs: {Work[ID]}")
        Worker = multiprocessing.Process(target=_decompose_graphs, args=(Work[ID],))
        Worker.start()
        Workers.append(Worker)
    for Worker in Workers:
        Worker.join()