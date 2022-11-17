import networkx as nx
import itertools as it
import multiprocessing
import logging
import os
import math

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

def _get_part_reds(GraphName, ID, nWorkers, nJobs):
    logging.basicConfig(filename=f"Default_Log.txt", level=logging.INFO, format=f'%(asctime)s [{multiprocessing.current_process().name}, {os.getpid()}] %(message)s')
    logging.info(f"Worker {ID} is starting...")
    _get_to_folder(GraphName)
    HostGraph = _get_graph_from_name(GraphName)
    UniqueReds = []
    PercentDone = 0
    for counter,Red in enumerate(_work_generator(_red_coloring_generator(HostGraph), ID, nWorkers)):
        if (counter % math.floor(nJobs/10)) == 0:
            logging.info(f"Worker {ID} is about {PercentDone}% done.")
            PercentDone += 10
        for UniqueRed in UniqueReds:
            if nx.is_isomorphic(Red, UniqueRed):
                break
        else:
            # If the previous for loop did not break, then...
            UniqueReds.append(Red)
    with open(f"{GraphName}.Reds.Part.{ID}.g6", "wb") as OutputFile:
        for Red in UniqueReds:
            OutputFile.write(nx.to_graph6_bytes(Red))
    logging.info(f"Worker {ID} has completed its task...")
    return

def _finish_reds(GraphName):
    logging.basicConfig(filename=f"Default_Log.txt", level=logging.INFO, format=f'%(asctime)s [{multiprocessing.current_process().name}, {os.getpid()}] %(message)s')
    logging.info(f"Finishing up {GraphName} by zipping together the seperate g6 files where needed...")
    _get_to_folder(GraphName)
    UniqueReds = None
    for FileName in os.listdir():
        if ".Reds.Part." in FileName:
            if UniqueReds == None:
                UniqueReds = list(nx.read_graph6(FileName))
            else:
                for Red in nx.read_graph6(FileName):
                    for UniqueRed in UniqueReds:
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
    return

def _get_reds(GraphName):
    nWorkers = max(multiprocessing.cpu_count()-1,1)
    Workers = []
    logging.basicConfig(filename=f"Default_Log.txt", level=logging.INFO, format=f'%(asctime)s [{multiprocessing.current_process().name}, {os.getpid()}] %(message)s')
    logging.info(f"Inspecting {GraphName}")
    nJobs = sum(1 for x in _work_generator(_red_coloring_generator(_get_graph_from_name(GraphName)), 0, nWorkers))
    logging.info(f"Each worker will get approximately {nJobs} jobs...")
    for ID in range(nWorkers):
        Worker = Worker = multiprocessing.Process(target=_get_part_reds, args=(GraphName, ID, nWorkers, nJobs))
        Worker.start()
        Workers.append(Worker)
    for Worker in Workers:
        Worker.join()
    _finish_reds(GraphName)


if __name__ == '__main__':
    _get_reds("K_7")