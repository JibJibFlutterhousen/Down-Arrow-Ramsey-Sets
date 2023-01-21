import networkx as nx
import itertools as it
import matplotlib.pyplot as plt
import os
import math
import multiprocessing

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
    plt.savefig(f"{FileName}.png")
    plt.close()
    return

def _get_graph_from_name(Graph_name):
    if "K_" in Graph_name:
        if "," in Graph_name:
            n,m=Graph_name.split(".",1)[0].split("K_",1)[1].split(",")
            n=int(n)
            m=int(m)
            return nx.complete_multipartite_graph(n,m)
        else:
            n=Graph_name.split(".",1)[0].split("K_",1)[1]
            n=int(n)
            return nx.complete_graph(n)
    elif "C_" in Graph_name:
            n=Graph_name.split(".",1)[0].split("C_",1)[1]
            n=int(n)
            return nx.cycle_graph(n)
    elif "P_" in Graph_name:
            n=Graph_name.split(".",1)[0].split("P_",1)[1]
            n=int(n)
            return nx.path_graph(n)

def _make_graph_directory(Graph_name):
    folders_to_make = ["Graphs", f"Graphs/{Graph_name}", f"Graphs/{Graph_name}/Parts", f"Graphs/{Graph_name}/Parts/DownArrowSet", f"Graphs/{Graph_name}/Parts/Poset", f"Graphs/{Graph_name}/Parts/Poset", f"Graphs/{Graph_name}/Parts/Subgraphs", f"Graphs/{Graph_name}/Parts/UniqueSubgraphs"]
    [os.mkdir(folder_name) for folder_name in folders_to_make if not os.path.exists(folder_name)]
    return

def _find_in_poset(Target_graph6_string, Graph_name):
    if Target_graph6_string in list(nx.read_gml(f"Graphs/{Graph_name}/{Graph_name}.Poset.gml").nodes()):
        return Target_graph6_string
    else:
        looking_for = nx.from_graph6_bytes(Target_graph6_string)
        for graph6_string in list(nx.read_gml(f"Graphs/{Graph_name}/{Graph_name}.Poset.gml").nodes()):
            graph_in_poset = nx.from_graph6_bytes(bytes(graph6_string[2:len(graph6_string)-1], "utf-8").replace(b"\\\\",b"\\"))
            if nx.is_isomorphic(looking_for, graph_in_poset):
                return(graph6_string)
    raise KeyError(f"No graph isomorphic to {Target_graph6_string} resides in the poset for {Graph_name}")

def _subgraphs_of(Subgraph_name, Graph_name):
    poset_graph = nx.read_gml(f"Graphs/{Graph_name}/{Graph_name}.Poset.gml")
    return [source for source,target in poset_graph.edges() if target == Subgraph_name]


def _Complement(Graph, Host):
#     This function takes the input of a host graph (Host) and a set of edges contained in the host (Graph)
    
#     This returns the edge induced subgraph (as a networkx graph) built on the host graph, from the supplied edges
    return Host.edge_subgraph(set(Host.edges())-set(Graph.edges())).copy()

def _union(List1, List2):
    return list(set(List1).union(set(List2)))

def _intersection(List1, List2):
    return list(set(List1).intersection(set(List2)))

def _subgraph_generator(Graph):
    for num_edges in range(Graph.number_of_edges()+1):
        for edges in it.combinations(Graph.edges(), num_edges):
            yield Graph.edge_subgraph(edges)

def _unique_subgraph_generator(Graph):
    unique_subgraphs = []
    for subgraph in _subgraph_generator(Graph):
        for unique_subgraph in unique_subgraphs:
            if nx.is_isomorphic(subgraph, unique_subgraph):
                break
        else:
            unique_subgraphs.append(subgraph)
            yield subgraph

def _g6_filter(path):
    unique_subgraphs = []
    for subgraph in nx.read_graph6(path):
        for unique_subgraph in unique_subgraphs:
            if nx.is_isomorphic(subgraph, unique_subgraph):
                break
        else:
            unique_subgraphs.append(subgraph)
            yield subgraph

def _poset_iterator(Graph_name):
    poset_graph = nx.read_gml(f"Graphs/{Graph_name}/{Graph_name}.Poset.gml")
    for graph6_string in poset_graph.nodes():
        yield graph6_string

def _split_work(Generator, ID, Num_workers):
    for job_number,job in enumerate(Generator):
        if (job_number % Num_workers) == ID:
            yield job
        else:
            continue

def _make_subgraphs(Graph_name):
    _make_graph_directory(Graph_name)
    num_workers = max(1, multiprocessing.cpu_count()-1)
    workers = []
    for id in range(num_workers):
        job = multiprocessing.Process(target=_make_part_subgraphs, args=(Graph_name,id, num_workers))
        job.start()
        workers.append(job)
    for worker in workers:
        worker.join()
    workers = []
    for id in range(num_workers):
        job = multiprocessing.Process(target=_filter_subgraphs, args=(Graph_name,id, num_workers))
        job.start()
        workers.append(job)
    for worker in workers:
        worker.join()
    _finish_subgraphs(Graph_name)
    return

def _make_part_subgraphs(Graph_name, ID, Num_workers):
    graph = _get_graph_from_name(Graph_name)
    with open(f"Graphs/{Graph_name}/Parts/Subgraphs/{Graph_name}.Subgraphs.Part.{ID}.g6", "wb") as output_file:
        [output_file.write(nx.to_graph6_bytes(subgraph, header=False)) for subgraph in _split_work(_subgraph_generator(graph), ID, Num_workers)]
    return

def _filter_subgraphs(Graph_name, ID, Num_workers):
    with open(f"Graphs/{Graph_name}/Parts/UniqueSubgraphs/{Graph_name}.UniqueSubgraphs.Part.{ID}.g6", "wb") as output_file:
        [output_file.write(nx.to_graph6_bytes(subgraph, header=False)) for subgraph in _g6_filter(f"Graphs/{Graph_name}/Parts/Subgraphs/{Graph_name}.Subgraphs.Part.{ID}.g6")]
    return

def _finish_subgraphs(Graph_name):
    unique_subgraphs = []
    for file in os.listdir(f"Graphs/{Graph_name}/Parts/UniqueSubgraphs"):
        if file.endswith(".g6"):
            for subgraph in nx.read_graph6(f"Graphs/{Graph_name}/Parts/UniqueSubgraphs/{file}"):
                for unique_subgraph in unique_subgraphs:
                    if nx.is_isomorphic(subgraph, unique_subgraph):
                        break
                else:
                    unique_subgraphs.append(subgraph)
    with open(f"Graphs/{Graph_name}/{Graph_name}.Unique.Subgraphs.g6", "wb") as output_file:
        pass
    with open(f"Graphs/{Graph_name}/{Graph_name}.Unique.Subgraphs.g6", "wb") as output_file:
        [output_file.write(nx.to_graph6_bytes(subgraph, header=False)) for subgraph in unique_subgraphs]
    return

def _make_poset(Graph_name):
    if not os.path.exists(f"Graphs/{Graph_name}/{Graph_name}.Unique.Subgraphs.g6"):
        _make_subgraphs(Graph_name)
    num_workers = max(1, multiprocessing.cpu_count()-1)
    workers = []
    for id in range(num_workers):
        job = multiprocessing.Process(target=_make_part_poset, args=(Graph_name,id, num_workers))
        job.start()
        workers.append(job)
    for worker in workers:
        worker.join()
    _finish_poset(Graph_name)
    return

def _make_part_poset(Graph_name, ID, Num_workers):
    poset_graph = nx.empty_graph(create_using=nx.DiGraph)
    for target in _split_work(nx.read_graph6(f"Graphs/{Graph_name}/{Graph_name}.Unique.Subgraphs.g6"), ID, Num_workers):
        for source in nx.read_graph6(f"Graphs/{Graph_name}/{Graph_name}.Unique.Subgraphs.g6"):
            if nx.algorithms.isomorphism.GraphMatcher(target, source).subgraph_is_monomorphic():
                poset_graph.add_edge(f"{nx.to_graph6_bytes(source, header=False).strip()}", f"{nx.to_graph6_bytes(target, header=False).strip()}")
    nx.write_gml(poset_graph, f"Graphs/{Graph_name}/Parts/Poset/Poset.Part.{ID}.gml")
    return

def _finish_poset(Graph_name):
    poset_graph = nx.empty_graph(create_using=nx.DiGraph)
    for file in os.listdir(f"Graphs/{Graph_name}/Parts/Poset/"):
        if file.startswith("Poset.Part."):
            poset_graph.add_edges_from(nx.read_gml(f"Graphs/{Graph_name}/Parts/Poset/{file}").edges())
    nx.write_gml(poset_graph, f"Graphs/{Graph_name}/{Graph_name}.Poset.gml")
    return

def make_down_arrow_set(Graph_name):
    graph = _get_graph_from_name(Graph_name)
    if graph.size() < 1:
        return
    if not os.path.exists(f"Graphs/{Graph_name}/{Graph_name}.Poset.gml"):
        _make_poset(Graph_name)
    if os.path.exists(f"Graphs/{Graph_name}/{Graph_name}.DownArrowIdeals.png"):
        retun
    num_workers = max(1, multiprocessing.cpu_count()-1)
    workers = []
    for id in range(num_workers):
        job = multiprocessing.Process(target=_make_part_down_arrow_set, args=(Graph_name,id, num_workers))
        job.start()
        workers.append(job)
    for worker in workers:
        worker.join()
    _finish_down_arrow_set(Graph_name)
    return

def _make_part_down_arrow_set(Graph_name, ID, Num_workers):
    down_arrow_set = None
    for red_subgraph_graph6_string in _split_work(_poset_iterator(Graph_name), ID, Num_workers):
        red_subgraph = nx.from_graph6_bytes(bytes(red_subgraph_graph6_string[2:len(red_subgraph_graph6_string)-1], "utf-8").replace(b"\\\\",b"\\"))
        blue_subgraph = _Complement(red_subgraph, _get_graph_from_name(Graph_name))
        blue_subgraph_graph6_string = _find_in_poset(nx.to_graph6_bytes(blue_subgraph, header=False).strip(), Graph_name)
        red_subgraphs = _subgraphs_of(red_subgraph_graph6_string, Graph_name)
        blue_subgraphs = _subgraphs_of(blue_subgraph_graph6_string, Graph_name)
        coloring_union = _union(red_subgraphs, blue_subgraphs)
        if down_arrow_set == None:
            down_arrow_set = coloring_union
        else:
            down_arrow_set = _intersection(down_arrow_set, coloring_union)
    if not down_arrow_set == None:
        down_arrow_set = [nx.from_graph6_bytes(bytes(subgraph_graph6_string[2:len(subgraph_graph6_string)-1], "utf-8").replace(b"\\\\",b"\\")) for subgraph_graph6_string in down_arrow_set]
        with open(f"Graphs/{Graph_name}/Parts/DownArrowSet/{Graph_name}.Down.Arrow.Set.Part.{ID}.g6", "wb") as output_file:
            [output_file.write(nx.to_graph6_bytes(subgraph, header=False)) for subgraph in down_arrow_set]
    return

def _finish_down_arrow_set(Graph_name):
    down_arrow_set = None
    for file_name in os.listdir(f"Graphs/{Graph_name}/Parts/DownArrowSet"):
        if file_name.startswith(f"{Graph_name}.Down.Arrow.Set.Part."):
            if down_arrow_set == None:
                down_arrow_set = [nx.to_graph6_bytes(graph, header=False).strip() for graph in nx.read_graph6(f"Graphs/{Graph_name}/Parts/DownArrowSet/{file_name}")]
            else:
                down_arrow_set = _intersection(down_arrow_set, [nx.to_graph6_bytes(graph, header=False).strip() for graph in nx.read_graph6(f"Graphs/{Graph_name}/Parts/DownArrowSet/{file_name}")])
    if not down_arrow_set == None:
        down_arrow_set = [nx.from_graph6_bytes(subgraph_graph6_string) for subgraph_graph6_string in down_arrow_set]
        with open(f"Graphs/{Graph_name}/{Graph_name}.Down.Arrow.Set.g6", "wb") as output_file:
            [output_file.write(nx.to_graph6_bytes(subgraph, header=False)) for subgraph in down_arrow_set]
    _make_ideals(Graph_name)
    return

def _make_ideals(Graph_name):
    poset_graph = nx.empty_graph(create_using=nx.DiGraph)
    down_arrow_set = list(nx.read_graph6(f"Graphs/{Graph_name}/{Graph_name}.Down.Arrow.Set.g6"))
    for source_id,source in enumerate(down_arrow_set):
        for target_id,target in enumerate(down_arrow_set):
            if source_id == target_id:
                continue
            else:
                if nx.algorithms.isomorphism.GraphMatcher(target, source).subgraph_is_monomorphic():
                    poset_graph.add_edge(source_id,target_id)
    maximal_ideals = [down_arrow_set[maximal_node] for maximal_node in poset_graph.nodes if poset_graph.out_degree(maximal_node) == 0]
    with open(f"Graphs/{Graph_name}/{Graph_name}.Down.Arrow.Ideals.g6", "wb") as output_file:
        [output_file.write(nx.to_graph6_bytes(ideal, header=False)) for ideal in maximal_ideals]
    _save_graph_list(maximal_ideals, f"Graphs/{Graph_name}/{Graph_name}.Down.Arrow.Ideals")
    return