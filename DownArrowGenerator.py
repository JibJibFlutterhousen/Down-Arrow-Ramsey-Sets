import networkx as nx
import itertools as it
import matplotlib.pyplot as plt
import multiprocessing
import logging
import os
import shutil
import math

def _Complement(Graph, Host):
#     This function takes the input of a host graph (Host) and a set of edges contained in the host (Graph)
    
#     This returns the edge induced subgraph (as a networkx graph) built on the host graph, from the supplied edges
    return Host.edge_subgraph(set(Host.edges())-set(Graph.edges())).copy()

def _Union(Graph1, Graph2):
    # This generator takes the input of two iterables (Graph1 and Graph2) that should hold graphs
    # 
    # This returns, one by one, each graph (as a networkx graph) in each of the two input iterables
    for Graph in Graph1:
        yield Graph
    for Graph in Graph2:
        yield Graph

def _Intersection(Graph1, Graph2):
#     This generator takes the input of two iterables (Graph1 and Graph 2) that should hold graphs
    
#     This returns, one by one, graphs (as a networkx graph) that are in both iterables
    
#     First do the setup by making sure that each of the iterables are converted to lists and begin with an empty list of unique subgraphs that have been returned
    Graph1 = list(Graph1)
    Graph2 = list(Graph2)
    Uniques = []
#     For optimization purposes, iterate first over the shorter list to check if each of the graphs in the shorter list is in the longer list
    if len(Graph1) >= len(Graph2):
        Smaller = Graph1
        Larger = Graph2
    else:
        Smaller = Graph2
        Larger = Graph1
    for First in Smaller:
        for Second in Larger:
            if nx.to_graph6_bytes(First) == nx.to_graph6_bytes(Second):
                if not nx.to_graph6_bytes(First) in Uniques:
                    Uniques.append(nx.to_graph6_bytes(First))
                    yield First
    
def _save_graph_list(GraphList, FileName = "Default", Names = None):
#     This funciton takes the input of a list of graphs (GraphList), a name for the output file (FileName), and a list of names for the graphs

#     This saves, in svg format, a square matrix of the given graphs

#     First do the setup by making sure that the input list of graphs is actually a list of graphs and initialize the list of names if none are supplied
    GraphList = list(GraphList)
    if Names == None:
        Names = []
        for i in range(len(GraphList)):
            Names.append(f"Graph {i}")
#     If the input graph list is empty
    if len(GraphList) <= 0:
        return
#     If the graph list is a single graph
    elif len(GraphList) == 1:
        Dimension = 1
        fig,axes = plt.subplots( nrows=Dimension, ncols=Dimension)
        nx.draw_circular(GraphList[0], ax=axes, with_labels=True, edge_color='black', node_color='black', font_color='silver')
        axes.set_title("Graph 0")
#     If the graph list has more than a single graph
    else:
#         Determine the number of graphs on a given row based off of making the matrix square
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
#     This funciton takes the input of a graph name (GraphName) in conventional form
    
#     This returns a networkx graph of the given type with the correct parameters (nodes and edges)
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
#     This method is used to create, and navigate to, the correct output directory
    if not "Graphs" in os.getcwd():
        if not os.path.exists("Graphs"):
            os.mkdir("Graphs")
        os.chdir("Graphs")
    if not GraphName in os.getcwd():
        if not os.path.exists(GraphName):
            os.mkdir(GraphName)
        os.chdir(GraphName)
    return

def _setup_part_dir(PartName):
#     This method is used to create, but not navigate into, the correct directory where part files will be stored after completing a given step of the process
    RootDir = os.getcwd()
    if not os.path.exists("Parts"):
        os.mkdir("Parts")
    os.chdir("Parts")
    if not os.path.exists(PartName):
        os.mkdir(PartName)
    os.chdir(RootDir)
    return

def _build_directory(GraphName):
#     This method is used to create, but not navigate into, the correct directory for a graph itself
    if not os.path.exists(f"Graphs"):
        os.mkdir("Graphs")
    if not os.path.exists(f"Graphs/{GraphName}"):
        os.mkdir(f"Graphs/{GraphName}")
    return

def _red_coloring_generator(Graph):
#     This generator takes the input of a networkx graph (Graph)
    
#     This returns, one by one, and, up to half the total number of edges, edge-induced subgraphs (as networkx graphs) starting with subgraphs with 1 edge, then 2, then 3... and so on

#     As a note, we will only have to iterate through half of the number of edges in the graph
    for nEdges in range(math.ceil(Graph.number_of_edges()/2)+1):
        for EdgeSet in it.combinations(Graph.edges(),nEdges):
            yield nx.edge_subgraph(Graph,EdgeSet).copy()

def _work_generator(Generator, WorkerID, nWorkers):
#     This generator takes the input of an iterable (Generator), an integer representing which worker number to assign tasks to (WorkerID), and the total number of workers that will be tasked with jobs contained in the iterable
    
#     This returns, one by one, every nth (nWorkers) object in the iterable with an initial offset (WorkerID)
    
#     This generator is used in multiprocessing to ensure that even load-balancing is done without having to store any complete list in memory at any given time
    for JobID,Job in enumerate(Generator):
        if (JobID % nWorkers) == WorkerID:
            yield Job
        else:
            continue

def _subgraph_set_generator(Graph, HostName):
#     This generator takes the input of an input graph (Graph) and a graph for which the input graph is a subgraph of (HostName)
    
#     This generator returns, one by one, each of the edge-induced subgraphs (as networkx graphs) of the input graph with respect to the host graph. This assumes that the poset of edge-induced subgraphs exists as a gml
    RootDir = os.getcwd()
    _get_to_folder(HostName)
#     Once in the correct directory, there should be a gml that exists as a simple, directed graph with node labels that correspond to edge-induced subgraphs (as g6 strings) of the host graph
    Poset = nx.read_gml(f"{HostName}.Poset.gml")
    os.chdir(RootDir)
    Subgraphs = []
    for Source,Target in Poset.edges():
        TargetGraph = nx.from_graph6_bytes(bytes(Target[2:len(Target)-1], "utf-8").replace(b"\\\\",b"\\"))
        if nx.to_graph6_bytes(Graph) == nx.to_graph_6_bytes(TargetGraph):
#         if nx.is_isomorphic(Graph, TargetGraph):
            yield nx.from_graph6_bytes(bytes(Source[2:len(Source)-1], "utf-8").replace(b"\\\\",b"\\"))

def _finish_reds(GraphName):
#     This method takes the input of a graph name (GraphName) and finishes the generation of red colorings of it
    
#     This saves, in g6 format, the list of unique red colorings of a given graph (up to coloring half of the edges red). This assumes that there are part files from the multithreaded processes.
    
#     First the setup for logging is performed and a log entry is made for this part of the process
    RootDir = os.getcwd()
    logging.basicConfig(filename=f"Default_Log.txt", level=logging.INFO, format=f'%(asctime)s [{multiprocessing.current_process().name}, {os.getpid()}] %(message)s')
    logging.info(f"Finishing up the red subgraphs of {GraphName} by zipping together the seperate g6 files where needed...")
    _get_to_folder(GraphName)
    _setup_part_dir("Reds")
    UniqueReds = None
#     Look for the respective part files leftover from the multithreaded process
    for FileName in os.listdir():
        if ".Reds.Part." in FileName:
            logging.info(f"Unpacking {FileName}")
#             If the list of unique red colorings is empty, initialize it
            if UniqueReds == None:
                UniqueReds = nx.read_graph6(FileName)
#                 This is a bugfix for small graphs, when a part file contains a single part file. The iterable that read_graph6 returns is an iterable over the nodes in a graph instead of an iterable over a list of graphs
                if type(UniqueReds) == type(nx.null_graph()):
                    UniqueReds = [UniqueReds,]
            else:
                InputList = nx.read_graph6(FileName)
#                 Again applying the bugfix that a part file contains only a single graph
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
#             Instead of removing the file, place it in the respective parts folder for extensive record keeping
            shutil.move(FileName, f"Parts/Reds/{FileName}")
#             os.remove(FileName)
    logging.info(f"Exporting the unique red subgraphs")
    with open(f"{GraphName}.Reds.g6", "wb") as OutputFile:
        for Red in UniqueReds:
            OutputFile.write(nx.to_graph6_bytes(Red, header=False))
    os.chdir(RootDir)
    return

def _get_part_reds(GraphName, ID, nWorkers, nJobs):
#     This method takes the input of a graph name (GraphName), an integer ID (ID) that corresponds to which worker number the respective process will be, the total number of workers in the group (nWorkers), and the approximate number of total jobs a given worker will have (nJobs)
    
#     This method saves, in g6 format, the list of unique red colorings of a given graph (up to coloring half the edges red), in a multithreaded manner. Each thread will save it's given output to a separate g6 file, to be zipped together later

#     First the setup for logging is performed and a log entry is made for this part of the process
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

def _get_reds(GraphName):
#     This is the entry point for the multithreaded process for which red colorings are made
    
#     This method takes the input of a graph name (GraphName) as a string
    
#     This method will result in a list of graph (in g6 format) that contains, up to isomorphism, unique red colroings with up to half the number of edges colored red

#     First the setup for logging is performed and a log entry is made for this part of the process. A number of processes will be spawned up to one fewer threads as available, and a minimum of one thread
    nWorkers = max(multiprocessing.cpu_count()-1,1)
    Workers = []
    logging.basicConfig(filename=f"Default_Log.txt", level=logging.INFO, format=f'%(asctime)s [{multiprocessing.current_process().name}, {os.getpid()}] %(message)s')
    logging.info(f"Inspecting {GraphName} with {nWorkers} workers")
    if os.path.exists(f"Graphs/{GraphName}/{GraphName}.Reds.g6"):
        logging.info(f"{GraphName} has already been inspected...")
        return
    nJobs = sum(1 for _ in _work_generator(_red_coloring_generator(_get_graph_from_name(GraphName)), 0, nWorkers))
    logging.info(f"Each worker will get approximately {nJobs} jobs...")
#     This is where the split into multiple processes happens
    for ID in range(nWorkers):
        Worker = Worker = multiprocessing.Process(target=_get_part_reds, args=(GraphName, ID, nWorkers, nJobs))
        Worker.start()
        Workers.append(Worker)
#     Waiting for each thread to complete its work
    for Worker in Workers:
        Worker.join()
#     Running the finishing steps, cleaning up the remnants required for multiprocessing
    _finish_reds(GraphName)
    return

def _finish_subgraphs(GraphName):
#     This method takes the input of a graph name (GraphName) and finishes the generation of edge-induced subgraphs of it
    
#     This saves, in g6 format, the list of unique edge-induced subgraphs of a given graph. This assumes that there are part files from the multithreaded processes.
    
#     First the setup for logging is performed and a log entry is made for this part of the process
    logging.basicConfig(filename=f"Default_Log.txt", level=logging.INFO, format=f'%(asctime)s [{multiprocessing.current_process().name}, {os.getpid()}] %(message)s')
    logging.info(f"Finishing up the subgraphs of {GraphName} by zipping together the seperate g6 files where needed...")
    _get_to_folder(GraphName)
    _setup_part_dir("Subgraphs")
    UniqueSubgraphs = None
    for FileName in os.listdir():
        if ".Unique.Subgraphs.Part." in FileName:
            logging.info(f"Unpacking {FileName}")
            if UniqueSubgraphs == None:
                UniqueSubgraphs = nx.read_graph6(FileName)
#                 Again applying the bugfix that a part file contains only a single graph
                if type(UniqueSubgraphs) == type(nx.null_graph()):
                    UniqueSubgraphs = [UniqueSubgraphs,]
            else:
                InputList = nx.read_graph6(FileName)
#                 Again applying the bugfix that a part file contains only a single graph
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
#             Instead of removing the file, place it in the respective parts folder for extensive record keeping
            shutil.move(FileName, f"Parts/Subgraphs/{FileName}")
#             os.remove(FileName)
    logging.info(f"Exporting the subgraphs of {GraphName}")
    with open(f"{GraphName}.Unique.Subgraphs.g6", "wb") as OutputFile:
        for Red in UniqueSubgraphs:
            OutputFile.write(nx.to_graph6_bytes(Red, header=False))
        OutputFile.write(nx.to_graph6_bytes(nx.empty_graph(), header=False))
        OutputFile.write(nx.to_graph6_bytes(nx.complete_graph(1), header=False))
    # _save_graph_list(UniqueSubgraphs, f"{GraphName}.Unique.Subgraphs")
    return

def _get_part_subgraphs(GraphName, ID, nWorkers, nJobs):
#     This method takes the input of a graph name (GraphName), an integer ID (ID) that corresponds to which worker number the respective process will be, the total number of workers in the group (nWorkers), and the approximate number of total jobs a given worker will have (nJobs)
    
#     This method saves, in g6 format, the list of unique subgraphs of a given graph, in a multithreaded manner. Each thread will save it's given output to a separate g6 file, to be zipped together later

#     First the setup for logging is performed and a log entry is made for this part of the process
    logging.basicConfig(filename=f"Default_Log.txt", level=logging.INFO, format=f'%(asctime)s [{multiprocessing.current_process().name}, {os.getpid()}] %(message)s')
    logging.info(f"Worker {ID} is starting subgraphs...")
    _get_to_folder(GraphName)
    HostGraph = _get_graph_from_name(GraphName)
    UniqueSubgraphs = []
    for counter,Red in enumerate(_work_generator(nx.read_graph6(f"{GraphName}.Reds.g6"),ID,nWorkers)):
#         Again applying the bugfix that a part file contains only a single graph
        if type(Red) == type(counter):
            OnlyGraph = nx.read_graph6(f"{GraphName}.Reds.g6")
            UniqueSubgraphs.append(OnlyGraph.copy())
            UniqueSubgraphs.append(_Complement(OnlyGraph, HostGraph).copy())
            break
        UniqueSubgraphs.append(Red.copy())
        UniqueSubgraphs.append(_Complement(Red, HostGraph).copy())
    with open(f"{GraphName}.Unique.Subgraphs.Part.{ID}.g6", "wb") as OutputFile:
        for Graph in UniqueSubgraphs:
            OutputFile.write(nx.to_graph6_bytes(Graph, header=False))
    logging.info(f"Worker {ID} is done extrapolating from the red subgraphs")
    return

def _make_subgraphs(GraphName):
#     This is the entry point for the multithreaded process for which edge-induced subgraphs are made
    
#     This method takes the input of a graph name (GraphName) as a string
    
#     This method will result in a list of graph (in g6 format) that contains, up to isomorphism, unique edge-induced subgraphs

#     First the setup for logging is performed and a log entry is made for this part of the process. A number of processes will be spawned up to one fewer threads as available, and a minimum of one thread
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
#     This is the split into multiple processes happens
    for ID in range(nWorkers):
        Worker = Worker = multiprocessing.Process(target=_get_part_subgraphs, args=(GraphName, ID, nWorkers, nJobs))
        Worker.start()
        Workers.append(Worker)
#     Waiting for each thread to complete its work
    for Worker in Workers:
        Worker.join()
#     Running the finishing steps, cleaning up the remnants required for multiprocessing
    _finish_subgraphs(GraphName)
    os.chdir(RootDir)
    return

def _finish_poset(GraphName):
#     This method takes the input of a graph name (GraphName) and finishes the generation of the poset graph of edge-induced subgraphs
    
#     This saves, in gml format, the poset of unique edge-induced subgraphs of a given graph. This assumes that there are part files from the multithreaded processes.
    
#     First the setup for logging is performed and a log entry is made for this part of the process
    logging.basicConfig(filename=f"Default_Log.txt", level=logging.INFO, format=f'%(asctime)s [{multiprocessing.current_process().name}, {os.getpid()}] %(message)s')
    logging.info(f"Finishing up the subgraphs of {GraphName} by zipping together the seperate g6 files where needed...")
    _get_to_folder(GraphName)
    _setup_part_dir("Poset")
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
#             Instead of removing the file, place it in the respective parts folder for extensive record keeping
            shutil.move(FileName, f"Parts/Poset/{FileName}")
            # os.remove(FileName)
    nx.write_gml(Poset,f"{GraphName}.Poset.gml")
    return

def _make_poset_parts(GraphName, ID, nWorkers, nJobs):
#     This method takes the input of a graph name (GraphName), an integer ID (ID) that corresponds to which worker number the respective process will be, the total number of workers in the group (nWorkers), and the approximate number of total jobs a given worker will have (nJobs)
    
#     This method saves, in gml format, the poset of unique subgraphs of a given graph, in a multithreaded manner. Each thread will save it's given output to a separate gml file, to be zipped together later

#     First the setup for logging is performed and a log entry is made for this part of the process
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
#     This is the entry point for the multithreaded process for which the poset of edge-induced subgraphs is made
    
#     This method takes the input of a graph name (GraphName) as a string
    
#     This method will result in a directed graph (in gml format) that contains the poset of edge-induced subgraphs (with an edge from a graph to another graph if the first is a subgraph of the second)

#     First the setup for logging is performed and a log entry is made for this part of the process. A number of processes will be spawned up to one fewer threads as available, and a minimum of one thread
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
#     This is the split into multiple processes happens
    for ID in range(nWorkers):
        Worker = Worker = multiprocessing.Process(target=_make_poset_parts, args=(GraphName, ID, nWorkers, nJobs))
        Worker.start()
        Workers.append(Worker)
#     Waiting for each thread to complete its work
    for Worker in Workers:
        Worker.join()
#     Running the finishing steps, cleaning up the remnants required for multiprocessing
    _finish_poset(GraphName)
    os.chdir(RootDir)
    return

def _finish_down_set(GraphName):
#     This method takes the input of a graph name (GraphName) and finishes the generation of the down arrow ramset set
    
#     This saves, in 6 format, the down arrow set of a given graph. This assumes that there are part files from the multithreaded processes.
    
#     First the setup for logging is performed and a log entry is made for this part of the process
    logging.basicConfig(filename=f"Default_Log.txt", level=logging.INFO, format=f'%(asctime)s [{multiprocessing.current_process().name}, {os.getpid()}] %(message)s')
    logging.info(f"Finishing up the down arrow set of {GraphName} by zipping together the seperate g6 files where needed...")
    _get_to_folder(GraphName)
    _setup_part_dir("DownArrowSet")
    DownArrowSet = None
    for FileName in os.listdir():
        if ".Down.Arrow.Set.Part." in FileName:
            logging.info(f"Unpacking {FileName}")
#             Again applying the bugfix that a part file contains only a single graph
            if DownArrowSet == None:
                DownArrowSet = list(nx.read_graph6(FileName))
            else:
                DownArrowSet = _Intersection(DownArrowSet, nx.read_graph6(FileName))
            logging.info(f"Done with {FileName}")
#             Instead of removing the file, place it in the respective parts folder for extensive record keeping
            shutil.move(FileName, f"Parts/DownArrowSet/{FileName}")
            # os.remove(FileName)
    logging.info(f"Exporting the down arrow set of {GraphName}")
    with open(f"{GraphName}.Down.Arrow.Set.g6", "wb") as OutputFile:
        for Graph in DownArrowSet:
            OutputFile.write(nx.to_graph6_bytes(Graph, header=False))
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
            ColoringUnion = _Union(_subgraph_set_generator(Red, GraphName), _subgraph_set_generator(_Complement(Red,HostGraph), GraphName))
            DownArrowSet = _Intersection(ColoringUnion, DownArrowSet)
    if not DownArrowSet is None:
        with open(f"{GraphName}.Down.Arrow.Set.Part.{ID}.g6", "wb") as OutputFile:
            for Graph in DownArrowSet:
                OutputFile.write(nx.to_graph6_bytes(Graph, header=False))
        logging.info(f"Worker {ID} is done determing its part of the down arrow set of {GraphName}")
    else:
        logging.info(f"Worker {ID} has determined an empty down arrow set...")
    return

def _make_down_set(GraphName):
#     This is the entry point for the multithreaded process for which the down arrow set is made
    
#     This method takes the input of a graph name (GraphName) as a string
    
#     This method will result in a list of the down arrow set (in g6 format) that contains the down arrow set

#     First the setup for logging is performed and a log entry is made for this part of the process. A number of processes will be spawned up to one fewer threads as available, and a minimum of one thread
    RootDir = os.getcwd()
    nWorkers = max(multiprocessing.cpu_count()-1,1)
    Workers = []
    logging.basicConfig(filename=f"Default_Log.txt", level=logging.INFO, format=f'%(asctime)s [{multiprocessing.current_process().name}, {os.getpid()}] %(message)s')
    logging.info(f"Making the down arrow set of {GraphName} with {nWorkers} workers")
    if os.path.exists(f"Graphs/{GraphName}/{GraphName}.Down.Arrow.Set.g6"):
        logging.info(f"{GraphName}\'s down arrow set has already been made...")
        return
    nJobs = sum(1 for _ in _work_generator(nx.read_graph6(f"Graphs/{GraphName}/{GraphName}.Reds.g6"),0,nWorkers))
    logging.info(f"Each worker will get approximately {nJobs} jobs...")bs...")
#     This is the split into multiple processes happens
    for ID in range(nWorkers):
        Worker = Worker = multiprocessing.Process(target=_get_part_down_set, args=(GraphName, ID, nWorkers, nJobs))
        Worker.start()
        Workers.append(Worker)
#     Waiting for each thread to complete its work
    for Worker in Workers:
        Worker.join()
#     Running the finishing steps, cleaning up the remnants required for multiprocessing
    _finish_down_set(GraphName)
    os.chdir(RootDir)
    return

def _make_ideals(GraphName):
#     This method takes the input of a graph name (GraphName) as a string
    
#     This method will result in a list of the ideals (in g6 format) that generate the down arrow set

#     First the setup for logging is performed and a log entry is made for this part of the process
    RootDir = os.getcwd()
    logging.basicConfig(filename=f"Default_Log.txt", level=logging.INFO, format=f'%(asctime)s [{multiprocessing.current_process().name}, {os.getpid()}] %(message)s')
    logging.info(f"Building the ideals of the down arrow set of {GraphName}")
    if os.path.exists(f"Graphs/{GraphName}/{GraphName}.Down.Arrow.Ideals.g6"):
        logging.info(f"{GraphName}\'s ideals already exists...")
        return
    _get_to_folder(GraphName)
    Poset = nx.empty_graph(create_using=nx.DiGraph)
#     Basically we make the poset of the down arrow set
    DownArrowSet = list(nx.read_graph6(f"{GraphName}.Down.Arrow.Set.g6"))
    for SourceID, SourceGraph in enumerate(DownArrowSet):
        for TargetID, TargetGraph in enumerate(DownArrowSet):
            if SourceID == TargetID:
                continue
            if nx.algorithms.isomorphism.GraphMatcher(TargetGraph, SourceGraph).subgraph_is_monomorphic():
                Poset.add_edge(SourceID, TargetID)
#     Each graph that is not a subgraph of another is considered an ideal, since all of its subgraphs are in the down arrow set
    Maximals = [DownArrowSet[node] for node in Poset.nodes if Poset.out_degree(node) == 0]
    logging.info(f"  Determined the poset structure of {GraphName}\'s down arrow set")
    _save_graph_list(Maximals, f"{GraphName}.Down.Arrow.Ideals")
    with open(f"{GraphName}.Down.Arrow.Ideals.g6", "wb") as OutputFile:
        for Graph in Maximals:
            OutputFile.write(nx.to_graph6_bytes(Graph, header=False))
    os.chdir(RootDir)
    return

if __name__ == '__main__':
    Graphs = ["K_4"]
#     Graphs = ["K_2", "K_3", "K_4", "K_5", "K_6", "K_1,1", "K_1,2", "K_1,3", "K_1,4", "K_1,5", "K_1,6", "K_1,7", "K_1,8", "K_1,9", "K_1,10", "K_2,1", "K_2,2", "K_2,3", "K_2,4", "K_2,5", "K_2,6", "K_2,7", "K_2,8", "K_2,9", "K_2,10", "K_3,1", "K_3,2", "K_3,3", "K_3,4", "K_3,5", "K_3,6", "K_3,7", "K_3,8", "K_3,9", "K_3,10", "K_4,1", "K_4,2", "K_4,3", "K_4,4", "K_4,5", "K_4,6", "K_4,7", "K_4,8", "K_4,9", "K_4,10"]
#     Graphs = ["K_7","K_8","K_9","K_4,4","K_4,5","K_4,6","K_4,7","K_4,8","K_5,5","K_5,6","K_5,7","K_5,8","K_6,6","K_6,7","K_6,8","K_7,7","K_7,8","K_8,8"]
    for GraphName in Graphs:
        _build_directory(GraphName)
        _get_reds(GraphName)
        _make_subgraphs(GraphName)
        _make_poset(GraphName)
        _make_down_set(GraphName)
        _make_ideals(GraphName)