import networkx as nx
import itertools as it
import matplotlib.pyplot as plt
import math
import scipy
import os
import logging
import multiprocessing

def _draw_graph(Graph):
    Dimension = 1
    fig,axes = plt.subplots(nrows=Dimension,ncols=Dimension)
    nx.draw_circular(Graph, ax=axes, with_labels=True, edge_color='black', node_color='black', font_color='white')
    axes.set_title("Graph 0")
    plt.show()
    plt.close()
    
def _draw_graph_list(GraphList, Names = None):
    if Names == None:
        Names = []
        for i in range(len(GraphList)):
            Names.append(f"Graph {i}")
    if len(GraphList) <= 0:
        return
    elif len(GraphList) == 1:
        Dimension = 1
        fig,axes = plt.subplots(nrows=Dimension,ncols=Dimension)
        nx.draw_circular(GraphList[0], ax=axes, with_labels=True, edge_color='black', node_color='black', font_color='silver')
        axes.set_title("Graph 0")
    else:
        Dimension = math.ceil(math.sqrt(len(GraphList)))
        fig,axes = plt.subplots(nrows=Dimension,ncols=Dimension, figsize=(25,25),dpi=250)
        GraphCounter=0
        for x in range(0,Dimension):
            for y in range(0,Dimension):
                if GraphCounter < len(GraphList):
                    axes[x,y].set_title(Names[GraphCounter])
                    nx.draw_circular(GraphList[GraphCounter], ax=axes[x,y], with_labels=True, edge_color='black', node_color='black', font_color='silver')
                    GraphCounter+=1
                else:
                    nx.draw(nx.null_graph(), ax=axes[x,y])
    plt.tight_layout()
    plt.show()
    plt.close()
    return

def _save_coloring(ColoringList, HostGraph = None, FileName = "Default"):
#     This function saves specifically a red blue coloring
    
    fig,axes = plt.subplots(nrows=1, ncols=3)
    nx.draw_circular(ColoringList[0], ax=axes[0], with_labels=True, edge_color="red", node_color='black', font_color='silver')
    axes[0].set_title("Red Subgraph")
    
    nx.draw_circular(ColoringList[1], ax=axes[1], with_labels=True, edge_color="blue", node_color='black', font_color='silver')
    axes[1].set_title("Blue Subgraph")
    
    if HostGraph == None:
        HostGraph = nx.compose(ColoringList[0], ColoringList[1])
    nx.draw_circular(HostGraph, ax=axes[2], with_labels=True, edge_color="black", node_color='black', font_color='silver')
    axes[2].set_title("Host")
    
    plt.tight_layout()
    plt.savefig(f"{FileName}.svg")
    plt.close()
    return

def _save_graph(Graph, FileName = "Default", Name = None):
    if Name == None:
        Name = "Graph"
    fig,axes = plt.subplots(nrows=1, ncols=1)
    nx.draw_circular(Graph, ax=axes, with_labels=True, edge_color="black", node_color="Black", font_color="silver")
    plt.tight_layout()
    plt.savefig(f"{FileName}.svg")
    plt.close()
    return

def _save_graph_list(GraphList, FileName = "Default", Names = None):
    GraphList = list(GraphList)
    if Names == None:
        Names = []
        for i in range(len(GraphList)):
            Names.append(f"Graph {i}")
    if len(GraphList) <= 0:
        return
    elif len(GraphList) == 1:
        Dimension = 1
        fig,axes = plt.subplots( nrows=Dimension, ncols=Dimension)
        nx.draw_circular(GraphList[0], ax=axes, with_labels=True, edge_color='black', node_color='black', font_color='silver')
        axes.set_title("Graph 0")
    else:
        Dimension = math.ceil(math.sqrt(len(GraphList)))
        fig,axes = plt.subplots( nrows=Dimension, ncols=Dimension, figsize=(25,25),dpi=250)
        GraphCounter=0
        for x in range(0,Dimension):
            for y in range(0,Dimension):
                if GraphCounter < len(GraphList):
                    axes[x,y].set_title(Names[GraphCounter])
                    nx.draw_circular(GraphList[GraphCounter], ax=axes[x,y], label=f"Graph {GraphCounter}", with_labels=True, edge_color='black', node_color='black', font_color='silver')
                    GraphCounter+=1
                else:
                    nx.draw(nx.null_graph(), ax=axes[x,y])
    plt.tight_layout()
    plt.savefig(f"{FileName}.svg")
    plt.close()
    return

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
            
def _Union(Graph1, Graph2):
    return _Return_Unique(set(_Unique_Edge_Subgraph_Generator(Graph1)).union(set(_Unique_Edge_Subgraph_Generator(Graph2))))

def _Intersection(GraphList1, GraphList2):
    Intersection = set()
    for Subgraph1 in GraphList1:
        Found = False
        for Subgraph2 in GraphList2:
            if nx.is_isomorphic(Subgraph1, Subgraph2):
                Intersection.add(Subgraph2)
                Found = True
                break
    return list(Intersection)

def _Complement(Graph, Host):
    return Host.edge_subgraph(set(Host.edges())-set(Graph.edges())).copy()

def _get_graph_from_file_name(GraphName):
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

def _inspect_graph(GraphName, ID, nWorkers):
    RootDir = os.getcwd()
    os.chdir("Graphs")
    os.chdir(GraphName)
    logging.basicConfig(filename=f"{GraphName}_Log.txt", level=logging.INFO, format=f'%(asctime)s [{multiprocessing.current_process().name}, {os.getpid()}] %(message)s')
    if os.path.exists(f"{GraphName}.Down.Arrow.Set.g6"):
        logging.info(f"{GraphName} has already been inspected")
        os.chdir(RootDir)
        return
    HostGraph = _get_graph_from_file_name(GraphName)
    UniqueSubgraphs = _work_generator(f"{GraphName}.UniqueGraphs.g6", ID, nWorkers)
    Intersections = set(nx.read_graph6(f"{GraphName}.UniqueGraphs.g6"))
    for count,Red in enumerate(UniqueSubgraphs):
        Blue = _Complement(Red, HostGraph)
        Unions = _Union(Red, Blue)
        Intersections = _Intersection(Unions, Intersections)
    logging.info(f"Worker {ID} is done determinging its part of the down arrow set of {GraphName}")
    with open(f"{GraphName}.Down.Arrow.Set.Part{ID}.g6", "wb") as OutputFile:
        for Graph in Intersections:
            OutputFile.write(nx.to_graph6_bytes(Graph, header=False))
    os.chdir(RootDir)
    return

def _finish_up(GraphName):
    RootDir = os.getcwd()
    os.chdir("Graphs")
    os.chdir(GraphName)
    logging.basicConfig(filename=f"{GraphName}_Log.txt", level=logging.INFO, format=f'%(asctime)s [{multiprocessing.current_process().name}, {os.getpid()}] %(message)s')
    if os.path.exists(f"{GraphName}.Down.Arrow.Set.g6"):
        logging.info(f"{GraphName} has already been inspected")
        os.chdir(RootDir)
        return
    logging.info(f"Intersecting the final graphs of the down arrow set of {GraphName} on a single thread...")
    Intersections = nx.read_graph6(f"{GraphName}.Down.Arrow.Set.Part1.g6")
    for FileName in os.listdir(os.getcwd()):
        if "Part" in FileName:
            if "Part1.g6" in FileName:
                continue
            else:
                Intersections = _Intersection(nx.read_graph6(FileName), Intersections)
            os.remove(FileName)
    logging.info(f"The down arrow set of {GraphName} has been concluded")
    with open(f"{GraphName}.Down.Arrow.Set.g6", "wb") as OutputFile:
        for Graph in Intersections:
            OutputFile.write(nx.to_graph6_bytes(Graph, header=False))
    _save_graph_list(Intersections, f"{GraphName}.Down.Arrow.Set")
    os.chdir(RootDir)
    return


def _find_graphs():
    Graphs = []
    RootDir = os.getcwd()
    os.chdir("Graphs")
    BaseDir = os.getcwd()
    for DirName in os.listdir(os.getcwd()):
        os.chdir(BaseDir)
        if os.path.isdir(DirName):
            os.chdir(DirName)
            for FileName in os.listdir(os.getcwd()):
                if ".UniqueGraphs.g6" in FileName:
                    Graphs.append(FileName.split(".",1)[0])
    os.chdir(RootDir)
    return Graphs

def _work_generator(FileName, WorkerID, nWorkers):
    # This function takes in a g6 file name in the working directory and parts out the graphs stored within based off of the number of workers tasked to completing the job
    with open(FileName, "rb") as InputFile:
        for LineID,Line in enumerate(InputFile.readlines()):
            if LineID < WorkerID:
                # This condition makes sure that the worker starts at the right place
                continue
            if (LineID % nWorkers) == WorkerID:
                # This condition gives the workers each their portions of the graphs
                yield nx.from_graph6_bytes(Line.strip())

if __name__ == '__main__':
    logging.basicConfig(filename=f"Default_Log.txt", level=logging.INFO, format=f'%(asctime)s [{multiprocessing.current_process().name}, {os.getpid()}] %(message)s')
    logging.info("Starting...")
    Graphs = _find_graphs()
    logging.info(f"Inspecting the graphs: {Graphs}")
    nWorkers = (multiprocessing.cpu_count()-1)
    Workers = []

    for GraphName in Graphs:
        logging.info(f"  Beginning work on {GraphName}")
        for ID in range(nWorkers):
            Worker = multiprocessing.Process(target=_inspect_graph, args=(GraphName, ID, nWorkers))
            Worker.start()
            Workers.append(Worker)
        for Worker in Workers:
            Worker.join()
        _finish_up(GraphName)