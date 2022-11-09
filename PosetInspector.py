import networkx as nx
import matplotlib.pyplot as plt
import shutil
import math
import multiprocessing
import os
import pickle
import logging

def _save_graph_list(GraphList, FileName = "Default", Names = None):
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

def _inspect_set(Graphs):
    logging.basicConfig(filename=f"Default_Log.txt", level=logging.INFO, format=f'%(asctime)s [{multiprocessing.current_process().name}, {os.getpid()}] %(message)s')
    RootDir = os.getcwd()
    os.chdir("Graphs")
    BaseDir = os.getcwd()
    for GraphName in Graphs:
        HostGraph = _get_graph_from_graph_name(GraphName)
        logging.info(f"Determining the poset structure of {GraphName}\'s down arrow set")
        os.chdir(BaseDir)
        os.chdir(GraphName)
        if os.path.exists(f"{GraphName}.Down.Arrow.Poset.pickle"):
            logging.info(f"  The poset structure already exists for {GraphName}...")
            continue
        if not os.path.exists(f"{GraphName}.Down.Arrow.Set.pickle"):
            logging.info(f"  The down arrow set for {GraphName} doesn't yet exist...")
            continue
        with open(f"{GraphName}.Down.Arrow.Set.pickle", "rb") as InputFile:
            DownArrowSet = pickle.load(InputFile)
        Poset = nx.empty_graph(create_using=nx.DiGraph)
        for SourceID, SourceGraph in enumerate(DownArrowSet):
            for TargetID, TargetGraph in enumerate(DownArrowSet):
                if SourceID == TargetID:
                    continue
                if nx.algorithms.isomorphism.GraphMatcher(TargetGraph, SourceGraph).subgraph_is_monomorphic():
                    Poset.add_edge(SourceID, TargetID)
        with open(f"{GraphName}.Down.Arrow.Poset.pickle", "wb") as OutputFile:
            pickle.dump(Poset, OutputFile)
        Maximals = [DownArrowSet[node] for node in Poset.nodes if Poset.out_degree(node) == 0]
        _save_graph_list(Maximals, f"{GraphName}.Down.Arrow.Ideals")
        logging.info(f"  Determined the poset structure of {GraphName}\'s down arrow set")
    os.chdir(RootDir)
    return
        
def _find_graphs():
    Graphs = []
    RootDir = os.getcwd()
    if "Graphs" not in os.getcwd():
        os.chdir("Graphs")
    BaseDir = os.getcwd()
    for DirName in os.listdir(os.getcwd()):
        os.chdir(BaseDir)
        if os.path.isdir(DirName):
            os.chdir(DirName)
            for FileName in os.listdir(os.getcwd()):
                if ".UniqueGraphs.pickle" in FileName:
                    Graphs.append(FileName.split(".",1)[0])
    os.chdir(RootDir)
    return Graphs

def _gather_results():
    RootDir = os.getcwd()
    if not os.path.exists("Results"):
        os.mkdir("Results")
    os.chdir("Results")
    TargetDir = os.getcwd()
    os.chdir(RootDir)
    os.chdir("Graphs")
    BaseDir = os.getcwd()
    for GraphName in _find_graphs():
        os.chdir(BaseDir)
        os.chdir(GraphName)
        if os.path.exists(f"{GraphName}.Down.Arrow.Ideals.svg"):
            shutil.copy(f"{GraphName}.Down.Arrow.Ideals.svg", TargetDir)
    os.chdir(RootDir)
    return

def slice_per(source, step):
    return [source[i::step] for i in range(step)]

if __name__ == '__main__':
    logging.basicConfig(filename=f"Default_Log.txt", level=logging.INFO, format=f'%(asctime)s [{multiprocessing.current_process().name}, {os.getpid()}] %(message)s')
    logging.info("Starting...")
    Graphs = _find_graphs()
    logging.info(f"Inspecting the graphs: {Graphs}")

    nWorkers = 12
    Workers = []
    Work = slice_per(Graphs, nWorkers)
    for ID in range(nWorkers):
        logging.info(f"Worker {ID} is being handed the list of graphs: {Work[ID]}")
        Worker = multiprocessing.Process(target=_inspect_set, args=(Work[ID],))
        Worker.start()
        Workers.append(Worker)
    for Worker in Workers:
        Worker.join()
    _gather_results()