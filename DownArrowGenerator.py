import networkx as nx
import itertools as it
import matplotlib.pyplot as plt
import multiprocessing
import logging
import os
import math

def _Complement(Graph, Host):
    return Host.edge_subgraph(set(Host.edges())-set(Graph.edges())).copy()

def _Union(Graph1, Graph2):
    for Graph in Graph1:
        yield Graph
    for Graph in Graph2:
        yield Graph

def _Intersection(Graph1, Graph2):
    Graph1 = list(Graph1)
    Graph2 = list(Graph2)
    Uniques = []
    if len(Graph1) >= len(Graph2):
        Smaller = Graph1
        Larger = Graph2
    else:
        Smaller = Graph2
        Larger = Graph1
    for First in Graph1:
        for Second in Graph2:
            if nx.is_isomorphic(First, Second):
                if not nx.to_graph6_bytes(First) in Uniques:
                    Uniques.append(nx.to_graph6_bytes(First))
                    yield First
    
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

def _get_graph_from_name(GraphName):
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

def _get_to_folder(GraphName):
    if not "Graphs" in os.getcwd():
        if not os.path.exists("Graphs"):
            os.mkdir("Graphs")
        os.chdir("Graphs")
    if not GraphName in os.getcwd():
        if not os.path.exists(GraphName):
            os.mkdir(GraphName)
        os.chdir(GraphName)
    return

def _build_directory(GraphName):
    if not os.path.exists(f"Graphs"):
        os.mkdir("Graphs")
    if not os.path.exists(f"Graphs/{GraphName}"):
        os.mkdir(f"Graphs/{GraphName}")
    return

def _red_coloring_generator(Graph):
    # As a note, we will only have to iterate through half of the number of edges in the graph
    for nEdges in range(math.ceil(Graph.number_of_edges()/2)+1):
        for EdgeSet in it.combinations(Graph.edges(),nEdges):
            yield nx.edge_subgraph(Graph,EdgeSet).copy()

def _work_generator(Generator, WorkerID, nWorkers):
    for JobID,Job in enumerate(Generator):
        if (JobID % nWorkers) == WorkerID:
            yield Job
        else:
            continue

def _subgraph_set_generator(Graph, HostName):
    RootDir = os.getcwd()
    _get_to_folder(HostName)
    Poset = nx.read_gml(f"{HostName}.Poset.gml")
    os.chdir(RootDir)
    Subgraphs = []
    for Source,Target in Poset.edges():
        TargetGraph = nx.from_graph6_bytes(bytes(Target[2:len(Target)-1], "utf-8").replace(b"\\\\",b"\\"))
        if nx.is_isomorphic(Graph, TargetGraph):
            yield nx.from_graph6_bytes(bytes(Source[2:len(Source)-1], "utf-8").replace(b"\\\\",b"\\"))

def _get_part_reds(GraphName, ID, nWorkers, nJobs):
    logging.basicConfig(filename=f"Default_Log.txt", level=logging.INFO, format=f'%(asctime)s [{multiprocessing.current_process().name}, {os.getpid()}] %(message)s')
    logging.info(f"Worker {ID} is starting...")
    _get_to_folder(GraphName)
    HostGraph = _get_graph_from_name(GraphName)
    UniqueReds = []
    for counter,Red in enumerate(_work_generator(_red_coloring_generator(HostGraph), ID, nWorkers)):
        if (counter % max(math.floor(nJobs/10),1)) == 0:
            logging.info(f"Worker {ID} is about {round((counter/nJobs)*100)}% done.")
        for UniqueRed in UniqueReds:
            if nx.is_isomorphic(Red, UniqueRed):
                break
        else:
            # If the previous for loop did not break, then...
            UniqueReds.append(Red)
    with open(f"{GraphName}.Reds.Part.{ID}.g6", "wb") as OutputFile:
        for Red in UniqueReds:
            OutputFile.write(nx.to_graph6_bytes(Red, header=False))
    logging.info(f"Worker {ID} is done making the red subgraphs")
    return

def _finish_reds(GraphName):
    RootDir = os.getcwd()
    logging.basicConfig(filename=f"Default_Log.txt", level=logging.INFO, format=f'%(asctime)s [{multiprocessing.current_process().name}, {os.getpid()}] %(message)s')
    logging.info(f"Finishing up the red subgraphs of {GraphName} by zipping together the seperate g6 files where needed...")
    _get_to_folder(GraphName)
    UniqueReds = None
    for FileName in os.listdir():
        if ".Reds.Part." in FileName:
            logging.info(f"Unpacking {FileName}")
            if UniqueReds == None:
                UniqueReds = list(nx.read_graph6(FileName))
            else:
                InputList = nx.read_graph6(FileName)
                if type(InputList) == type(nx.null_graph()):
                    InputList = (InputList,)
                for Red in InputList:
                    for UniqueRed in UniqueReds:
                        if type(Red) == type(UniqueRed):
                            if nx.is_isomorphic(Red, UniqueRed):
                                break
                    else:
                        # If the previous for loop did not break, then...
                        UniqueReds.append(Red)
            logging.info(f"Done with {FileName}")
            os.remove(FileName)
    with open(f"{GraphName}.Reds.g6", "wb") as OutputFile:
        for Red in UniqueReds:
            OutputFile.write(nx.to_graph6_bytes(Red, header=False))
    logging.info(f"Exporting the unique red subgraphs")
    # _save_graph_list(UniqueReds, f"{GraphName}.Reds")
    os.chdir(RootDir)
    return

def _get_reds(GraphName):
    nWorkers = max(multiprocessing.cpu_count()-1,1)
    Workers = []
    logging.basicConfig(filename=f"Default_Log.txt", level=logging.INFO, format=f'%(asctime)s [{multiprocessing.current_process().name}, {os.getpid()}] %(message)s')
    logging.info(f"Inspecting {GraphName} with {nWorkers} workers")
    if os.path.exists(f"Graphs/{GraphName}/{GraphName}.Reds.g6"):
        logging.info(f"{GraphName} has already been inspected...")
        return
    nJobs = sum(1 for _ in _work_generator(_red_coloring_generator(_get_graph_from_name(GraphName)), 0, nWorkers))
    logging.info(f"Each worker will get approximately {nJobs} jobs...")
    for ID in range(nWorkers):
        Worker = Worker = multiprocessing.Process(target=_get_part_reds, args=(GraphName, ID, nWorkers, nJobs))
        Worker.start()
        Workers.append(Worker)
    for Worker in Workers:
        Worker.join()
    _finish_reds(GraphName)
    return

def _finish_subgraphs(GraphName):
    logging.basicConfig(filename=f"Default_Log.txt", level=logging.INFO, format=f'%(asctime)s [{multiprocessing.current_process().name}, {os.getpid()}] %(message)s')
    logging.info(f"Finishing up the subgraphs of {GraphName} by zipping together the seperate g6 files where needed...")
    _get_to_folder(GraphName)
    UniqueSubgraphs = None
    for FileName in os.listdir():
        if ".Unique.Subgraphs.Part." in FileName:
            logging.info(f"Unpacking {FileName}")
            if UniqueSubgraphs == None:
                UniqueSubgraphs = nx.read_graph6(FileName)
                if type(UniqueSubgraphs) == type(nx.null_graph()):
                    UniqueSubgraphs = (UniqueSubgraphs,)
            else:
                InputList = nx.read_graph6(FileName)
                if type(InputList) == type(nx.null_graph()):
                    InputList = (InputList,)
                for Red in InputList:
                    for UniqueSubgraph in UniqueSubgraphs:
                        if nx.is_isomorphic(Red, UniqueSubgraph):
                            break
                    else:
                        # If the previous for loop did not break, then...
                        UniqueSubgraphs.append(Red)
            logging.info(f"Done with {FileName}")
            os.remove(FileName)
    with open(f"{GraphName}.Unique.Subgraphs.g6", "wb") as OutputFile:
        for Red in UniqueSubgraphs:
            OutputFile.write(nx.to_graph6_bytes(Red, header=False))
        OutputFile.write(nx.to_graph6_bytes(nx.empty_graph(), header=False))
        OutputFile.write(nx.to_graph6_bytes(nx.complete_graph(1), header=False))
    logging.info(f"Exporting the subgraphs of {GraphName}")
    # _save_graph_list(UniqueSubgraphs, f"{GraphName}.Unique.Subgraphs")
    return

def _get_part_subgraphs(GraphName, ID, nWorkers, nJobs):
    logging.basicConfig(filename=f"Default_Log.txt", level=logging.INFO, format=f'%(asctime)s [{multiprocessing.current_process().name}, {os.getpid()}] %(message)s')
    logging.info(f"Worker {ID} is starting subgraphs...")
    _get_to_folder(GraphName)
    HostGraph = _get_graph_from_name(GraphName)
    UniqueSubgraphs = []
    for counter,Red in enumerate(_work_generator(nx.read_graph6(f"{GraphName}.Reds.g6"),ID,nWorkers)):
        if type(Red) == type(counter):
            OnlyGraph = nx.read_graph6(f"{GraphName}.Reds.g6")
            UniqueSubgraphs.append(OnlyGraph.copy())
            UniqueSubgraphs.append(_Complement(OnlyGraph, HostGraph).copy())
            break
        if (counter % max(math.floor(nJobs/10),1)) == 0:
            logging.info(f"Worker {ID} is about {round((counter/nJobs)*100)}% done.")
        UniqueSubgraphs.append(Red.copy())
        UniqueSubgraphs.append(_Complement(Red, HostGraph).copy())
    with open(f"{GraphName}.Unique.Subgraphs.Part.{ID}.g6", "wb") as OutputFile:
        for Graph in UniqueSubgraphs:
            OutputFile.write(nx.to_graph6_bytes(Graph, header=False))
    logging.info(f"Worker {ID} is done extrapolating from the red subgraphs")
    return

def _make_subgraphs(GraphName):
    RootDir = os.getcwd()
    nWorkers = max(multiprocessing.cpu_count()-1,1)
    Workers = []
    logging.basicConfig(filename=f"Default_Log.txt", level=logging.INFO, format=f'%(asctime)s [{multiprocessing.current_process().name}, {os.getpid()}] %(message)s')
    logging.info(f"Extrapolating from the red subgraphs of {GraphName} with {nWorkers} workers")
    if os.path.exists(f"Graphs/{GraphName}/{GraphName}.Unique.Subgraphs.g6"):
        logging.info(f"{GraphName} has already been extrapolated...")
        return
    nJobs = sum(1 for _ in _work_generator(nx.read_graph6(f"Graphs/{GraphName}/{GraphName}.Reds.g6"),0,nWorkers))
    logging.info(f"Each worker will get approximately {nJobs} jobs...")
    for ID in range(nWorkers):
        Worker = Worker = multiprocessing.Process(target=_get_part_subgraphs, args=(GraphName, ID, nWorkers, nJobs))
        Worker.start()
        Workers.append(Worker)
    for Worker in Workers:
        Worker.join()
    _finish_subgraphs(GraphName)
    os.chdir(RootDir)
    return

def _finish_poset(GraphName):
    logging.basicConfig(filename=f"Default_Log.txt", level=logging.INFO, format=f'%(asctime)s [{multiprocessing.current_process().name}, {os.getpid()}] %(message)s')
    logging.info(f"Finishing up the subgraphs of {GraphName} by zipping together the seperate g6 files where needed...")
    _get_to_folder(GraphName)
    Poset = None
    for FileName in os.listdir():
        if ".Poset.Part." in FileName:
            logging.info(f"Unpacking {FileName}")
            if Poset == None:
                Poset = nx.read_gml(FileName)
            else:
                for Source,Target in nx.read_gml(FileName).edges():
                    Poset.add_edge(Source, Target)
            logging.info(f"Done with {FileName}")
            os.remove(FileName)
    nx.write_gml(Poset,f"{GraphName}.Poset.gml")
    return

def _make_poset_parts(GraphName, ID, nWorkers, nJobs):
    logging.basicConfig(filename=f"Default_Log.txt", level=logging.INFO, format=f'%(asctime)s [{multiprocessing.current_process().name}, {os.getpid()}] %(message)s')
    logging.info(f"Worker {ID} is starting poset decomposition...")
    _get_to_folder(GraphName)
    Poset = nx.empty_graph(create_using=nx.DiGraph)
    for counter,Source in enumerate(_work_generator(nx.read_graph6(f"{GraphName}.Unique.Subgraphs.g6"),ID,nWorkers)):
        if (counter % max(math.floor(nJobs/10),1)) == 0:
            logging.info(f"Worker {ID} is about {round((counter/nJobs)*100)}% done.")
        for Target in nx.read_graph6(f"{GraphName}.Unique.Subgraphs.g6"):
            if nx.algorithms.isomorphism.GraphMatcher(Target,Source).subgraph_is_monomorphic():
                Poset.add_edge(f"{nx.to_graph6_bytes(Source, header=False).strip()}", f"{nx.to_graph6_bytes(Target, header=False).strip()}")
    nx.write_gml(Poset, f"{GraphName}.Poset.Part.{ID}.gml")
    logging.info(f"Worker {ID} is done decomposing its part of the poset")
    return

def _make_poset(GraphName):
    RootDir = os.getcwd()
    nWorkers = max(multiprocessing.cpu_count()-1,1)
    Workers = []
    logging.basicConfig(filename=f"Default_Log.txt", level=logging.INFO, format=f'%(asctime)s [{multiprocessing.current_process().name}, {os.getpid()}] %(message)s')
    logging.info(f"Building the poset of {GraphName}")
    if os.path.exists(f"Graphs/{GraphName}/{GraphName}.Poset.gml"):
        logging.info(f"{GraphName}\'s poset already exists...")
        return
    nJobs = sum(1 for _ in _work_generator(nx.read_graph6(f"Graphs/{GraphName}/{GraphName}.Unique.Subgraphs.g6"),0,nWorkers))
    logging.info(f"Each worker will get approximately {nJobs} jobs...")
    for ID in range(nWorkers):
        Worker = Worker = multiprocessing.Process(target=_make_poset_parts, args=(GraphName, ID, nWorkers, nJobs))
        Worker.start()
        Workers.append(Worker)
    for Worker in Workers:
        Worker.join()
    _finish_poset(GraphName)
    os.chdir(RootDir)
    return

def _finish_down_set(GraphName):
    logging.basicConfig(filename=f"Default_Log.txt", level=logging.INFO, format=f'%(asctime)s [{multiprocessing.current_process().name}, {os.getpid()}] %(message)s')
    logging.info(f"Finishing up the down arrow set of {GraphName} by zipping together the seperate g6 files where needed...")
    _get_to_folder(GraphName)
    DownArrowSet = None
    for FileName in os.listdir():
        if ".Down.Arrow.Set.Part." in FileName:
            logging.info(f"Unpacking {FileName}")
            if DownArrowSet == None:
                DownArrowSet = list(nx.read_graph6(FileName))
            else:
                DownArrowSet = _Intersection(DownArrowSet, nx.read_graph6(FileName))
            logging.info(f"Done with {FileName}")
            os.remove(FileName)
    with open(f"{GraphName}.Down.Arrow.Set.g6", "wb") as OutputFile:
        for Graph in DownArrowSet:
            OutputFile.write(nx.to_graph6_bytes(Graph, header=False))
    logging.info(f"Exporting the down arrow set of {GraphName}")
    _save_graph_list(list(nx.read_graph6(f"{GraphName}.Down.Arrow.Set.g6")), f"{GraphName}.Down.Arrow.Set")
    return

def _get_part_down_set(GraphName, ID, nWorkers, nJobs):
    logging.basicConfig(filename=f"Default_Log.txt", level=logging.INFO, format=f'%(asctime)s [{multiprocessing.current_process().name}, {os.getpid()}] %(message)s')
    logging.info(f"Worker {ID} is starting to make the down arrow set...")
    _get_to_folder(GraphName)
    HostGraph = _get_graph_from_name(GraphName)
    DownArrowSet = None
    for counter,Red in enumerate(_work_generator(nx.read_graph6(f"{GraphName}.Reds.g6"),ID,nWorkers)):
        if type(Red) == type(counter):
            OnlyGraph = nx.read_graph6(f"{GraphName}.Reds.g6")
            DownArrowSet = _Union(_subgraph_set_generator(OnlyGraph, GraphName), _subgraph_set_generator(_Complement(OnlyGraph,HostGraph), GraphName))
            break
        if counter == 0:
            DownArrowSet = _Union(_subgraph_set_generator(Red, GraphName), _subgraph_set_generator(_Complement(Red,HostGraph), GraphName))
        else:
            if (counter % max(math.floor(nJobs/10),1)) == 0:
                logging.info(f"Worker {ID} is about {round((counter/nJobs)*100)}% done.")
            ColoringUnion = _Union(_subgraph_set_generator(Red, GraphName), _subgraph_set_generator(_Complement(Red,HostGraph), GraphName))
            DownArrowSet = _Intersection(ColoringUnion, DownArrowSet)
    logging.info(f"Worker {ID} is done determing its part of the down arrow set of {GraphName}")
    if not DownArrowSet is None:
        logging.info(f"Exporing the down arrow set part {ID}")
        with open(f"{GraphName}.Down.Arrow.Set.Part.{ID}.g6", "wb") as OutputFile:
            for Graph in DownArrowSet:
                OutputFile.write(nx.to_graph6_bytes(Graph, header=False))
    return

def _make_down_set(GraphName):
    RootDir = os.getcwd()
    nWorkers = max(multiprocessing.cpu_count()-1,1)
    Workers = []
    logging.basicConfig(filename=f"Default_Log.txt", level=logging.INFO, format=f'%(asctime)s [{multiprocessing.current_process().name}, {os.getpid()}] %(message)s')
    logging.info(f"Extrapolating from the red subgraphs of {GraphName} with {nWorkers} workers")
    if os.path.exists(f"Graphs/{GraphName}/{GraphName}.Down.Arrow.Set.g6"):
        logging.info(f"{GraphName}\'s down arrow set has already been made...")
        return
    nJobs = sum(1 for _ in _work_generator(nx.read_graph6(f"Graphs/{GraphName}/{GraphName}.Reds.g6"),0,nWorkers))
    logging.info(f"Each worker will get approximately {nJobs} jobs...")
    for ID in range(nWorkers):
        Worker = Worker = multiprocessing.Process(target=_get_part_down_set, args=(GraphName, ID, nWorkers, nJobs))
        Worker.start()
        Workers.append(Worker)
    for Worker in Workers:
        Worker.join()
    _finish_down_set(GraphName)
    os.chdir(RootDir)
    return

def _make_ideals(GraphName):
    RootDir = os.getcwd()
    logging.basicConfig(filename=f"Default_Log.txt", level=logging.INFO, format=f'%(asctime)s [{multiprocessing.current_process().name}, {os.getpid()}] %(message)s')
    logging.info(f"Building the ideals of the down arrow set of {GraphName}")
    if os.path.exists(f"Graphs/{GraphName}/{GraphName}.Down.Arrow.Ideals.g6"):
        logging.info(f"{GraphName}\'s ideals already exists...")
        return
    _get_to_folder(GraphName)
    Poset = nx.empty_graph(create_using=nx.DiGraph)
    DownArrowSet = list(nx.read_graph6(f"{GraphName}.Down.Arrow.Set.g6"))
    for SourceID, SourceGraph in enumerate(DownArrowSet):
        for TargetID, TargetGraph in enumerate(DownArrowSet):
            if SourceID == TargetID:
                continue
            if nx.algorithms.isomorphism.GraphMatcher(TargetGraph, SourceGraph).subgraph_is_monomorphic():
                Poset.add_edge(SourceID, TargetID)
    Maximals = [DownArrowSet[node] for node in Poset.nodes if Poset.out_degree(node) == 0]
    logging.info(f"  Determined the poset structure of {GraphName}\'s down arrow set")
    _save_graph_list(Maximals, f"{GraphName}.Down.Arrow.Ideals")
    with open(f"{GraphName}.Down.Arrow.Ideals.g6", "wb") as OutputFile:
        for Graph in Maximals:
            OutputFile.write(nx.to_graph6_bytes(Graph, header=False))
    os.chdir(RootDir)
    return

if __name__ == '__main__':
    # Graphs = ["K_3,4"]

    GraphName = "K_1,1"

    # _build_directory(GraphName)
    # _get_reds(GraphName)
    # _make_subgraphs(GraphName)
    # _make_poset(GraphName)
    # _make_down_set(GraphName)
    # # _make_ideals(GraphName)

    Graphs = ["K_1,1", "K_1,2", "K_1,3", "K_2", "K_3"]
    for GraphName in Graphs:
        print(GraphName)
        _build_directory(GraphName)
        _get_reds(GraphName)
        _make_subgraphs(GraphName)
        _make_poset(GraphName)
        _make_down_set(GraphName)
        _make_ideals(GraphName)