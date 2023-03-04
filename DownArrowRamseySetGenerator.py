import networkx as nx
import matplotlib.pyplot as plt
import itertools as it
import psutil
import sys
import multiprocessing
import os
import tempfile
import math

def ZimGraph():
    return nx.parse_adjlist(['1 2 5', '2 3 6', '3 7', '4 5', '5 6 9', '6 7 9', '7 8 9', '9 10 11', '10 11'])

def _allocate_work(iterator, worker_id, num_workers):
    for job_number,job in enumerate(iterator):
        if (job_number % num_workers) == worker_id:
            yield job
        else:
            continue

def _send_workers(target_function, argument):
    # num_workers = 1
    num_workers = max(1, multiprocessing.cpu_count()-1)
    workers = []
    for worker_id in range(num_workers):
        job = multiprocessing.Process(target=target_function, args=(argument, worker_id, num_workers))
        job.start()
        workers.append(job)
    for worker in workers:
        worker.join()
    return

def _read_graph6(path):
    for graph in nx.read_graph6(path):
        if type(graph) == type(nx.empty_graph()):
            yield graph 
        else:
            single_graph = nx.read_graph6(path)
            yield single_graph
            break

def _get_graph_from_name(graph_name):
    if graph_name == "Petersen":
        return nx.petersen_graph()
    if graph_name == "ZimGraph":
        return ZimGraph()
    if "K_" in graph_name:
        if "," in graph_name:
            n,m=graph_name.split(".",1)[0].split("K_",1)[1].split(",")
            n=int(n)
            m=int(m)
            return nx.complete_multipartite_graph(n,m)
        else:
            n=graph_name.split(".",1)[0].split("K_",1)[1]
            n=int(n)
            return nx.complete_graph(n)
    elif "C_" in graph_name:
            n=graph_name.split(".",1)[0].split("C_",1)[1]
            n=int(n)
            return nx.cycle_graph(n)
    elif "P_" in graph_name:
            n=graph_name.split(".",1)[0].split("P_",1)[1]
            n=int(n)
            return nx.path_graph(n)

def _make_graph_directory(graph_name):
    folders_to_make = ["Graphs", f"Graphs/{graph_name}", f"Graphs/{graph_name}/Subgraphs", f"Graphs/{graph_name}/Poset", f"Graphs/{graph_name}/Down-Arrow Ramsey Set", f"Graphs/{graph_name}/Red-Blue Colorings"]
    [os.mkdir(folder_name) for folder_name in folders_to_make if not os.path.exists(folder_name)]
    return

def _draw_graph(graph_iter, path=None):
    figure,axes = plt.subplots(figsize=(25,25), dpi=250)
    graph=next(graph_iter)
    axes.set_title(_graph6_bytes_to_file_name(nx.to_graph6_bytes(graph,header=False)))
    nx.draw_kamada_kawai(graph, ax=axes, with_labels=True, node_color="black", edge_color="black", font_color="lightgrey")
    plt.tight_layout()
    if path == None:
        plt.show()
    else:
        plt.savefig(f"{path}.png")
        plt.close()
    return

def _draw_graphs(graph_iter, path=None):
    graph_iter,tmp = it.tee(graph_iter)
    num_graphs = 0
    for graph in tmp:
        num_graphs+=1
    if num_graphs == 1:
        _draw_graph(graph_iter, path)
        return
    dimension = math.ceil(math.sqrt(num_graphs))
    figure,axes = plt.subplots(nrows=dimension, ncols=dimension, figsize=(25,25), dpi=250)
    graph_counter = 0
    for x in range(dimension):
        for y in range(dimension):
            if graph_counter < num_graphs:
                graph=next(graph_iter)
                axes[x,y].set_title(_graph6_bytes_to_file_name(nx.to_graph6_bytes(graph,header=False)))
                nx.draw_kamada_kawai(graph, ax=axes[x,y], with_labels=True, node_color="black", edge_color="black", font_color="lightgrey")
            else:
                nx.draw(nx.null_graph(), ax=axes[x,y])
            graph_counter+=1
    plt.tight_layout()
    if path == None:
        plt.show()
    else:
        plt.savefig(f"{path}.png")
        plt.close()
    return
            
def _file_name_to_graph6_bytes(file_name):
    """
        Ok, so hold onto your butts for this one, because windows caused a bug here.

        First, you cannot use the symbols "?", "\", or "|" in a windows file name but they can be encoded as graph6 byte strings... so we replace them with "1", "2", and "3" respectively.
        Next, windows cannot have files with the same letters, but different cases, in the same directory (i.e. "Bw.g6" and "BW.g6") but this CAN happen for non-isomorphic graphs. So we choose to make everything lowercase.
        If an upper-case letter is replaced with a lower-case by this function, it will append a "+" symbol in front so it can be decoded later.
    """
    return bytes(file_name.split(".g6")[0].replace("1","?").replace("2",chr(92)).replace("3","|").replace('+a','A').replace('+b','B').replace('+c','C').replace('+d','D').replace('+e','E').replace('+f','F').replace('+g','G').replace('+h','H').replace('+i','I').replace('+j','J').replace('+k','K').replace('+l','L').replace('+m','M').replace('+n','N').replace('+o','O').replace('+p','P').replace('+q','Q').replace('+r','R').replace('+s','S').replace('+t','T').replace('+u','U').replace('+v','V').replace('+w','W').replace('+x','X').replace('+y','Y').replace('+z','Z'), "utf-8")

def _graph6_bytes_to_file_name(graph6_bytes):
    """
        Ok, so hold onto your butts for this one, because windows caused a bug here.

        First, you cannot use the symbols "?", "\", or "|" in a windows file name but they can be encoded as graph6 byte strings... so we replace them with "1", "2", and "3" respectively.
        Next, windows cannot have files with the same letters, but different cases, in the same directory (i.e. "Bw.g6" and "BW.g6") but this CAN happen for non-isomorphic graphs. So we choose to make everything lowercase.
        If an upper-case letter is replaced with a lower-case by this function, it will append a "+" symbol in front so it can be decoded later.
    """
    return graph6_bytes.decode().strip().replace("?","1").replace(chr(92),"2").replace("|","3").replace('A','+a').replace('B','+b').replace('C','+c').replace('D','+d').replace('E','+e').replace('F','+f').replace('G','+g').replace('H','+h').replace('I','+i').replace('J','+j').replace('K','+k').replace('L','+l').replace('M','+m').replace('N','+n').replace('O','+o').replace('P','+p').replace('Q','+q').replace('R','+r').replace('S','+s').replace('T','+t').replace('U','+u').replace('V','+v').replace('W','+w').replace('X','+x').replace('Y','+y').replace('Z','+z')+".g6"

def _non_isomorphic_edge_induced_subgraph_generator(graph):
    for subset_size in range(graph.number_of_edges()+1):
        for subset in it.combinations(graph.edges(),subset_size):
            yield graph.edge_subgraph(subset)
            
def _distinct_edge_induced_subgraph_generator(graph):
    with tempfile.TemporaryFile() as tmp:
        for edge_induced_subgraph in _non_isomorphic_edge_induced_subgraph_generator(graph):
            tmp.seek(0)
            for distinct_edge_induced_subgraph in _read_graph6(tmp):
                if nx.is_isomorphic(edge_induced_subgraph,distinct_edge_induced_subgraph):
                    break
            else:
                tmp.write(nx.to_graph6_bytes(edge_induced_subgraph, header=False))
                yield edge_induced_subgraph

def _graph_iter_union_generator(graph_iter_1, graph_iter_2):
    for item in graph_iter_1:
        yield item
    for item in graph_iter_2:
        yield item

def _graph_complement(subgraph, host_graph_name):
    host_graph = _get_graph_from_name(host_graph_name)
    """
        This function should be much simpler, but there are some issues with the way that complete bipartite graphs were working that prevented simplicity.

        If a human were doing complementation inside of a host graph (by which I mean automatically ignoring isolated vertices), we would find a copy of the original graph in the host graph, and just remove those edges.
        Well, this is what the algorithm has to do as well, so we have to invoke SOMETHING NOT IN THE DOCUMENTATION BUT ONLY IN THE SOURCE OF NETWORKX.... subgraph_monomorphisms_iter() to find an edge-induced subgraph.

        Once we find this edge-induced subgraph, we need to relabel our nodes on the input subgraph to match that matching, so that we can properly remove the edges.

        I caught this annoying bug when trying to remove an edge from K_2,2.....
    """
    mapping = dict()
    for key,value in next(nx.algorithms.isomorphism.GraphMatcher(host_graph,subgraph).subgraph_monomorphisms_iter()).items():
        mapping[value]=key
    subgraph = nx.relabel_nodes(subgraph,mapping,copy=True)
    blue_subgraph = host_graph.edge_subgraph(set(host_graph.edges())-set(subgraph.edges()))
    for subgraph_file_name in os.scandir(f"Graphs/{host_graph_name}/Subgraphs"):
        if nx.is_isomorphic(blue_subgraph, nx.from_graph6_bytes(_file_name_to_graph6_bytes(subgraph_file_name.name))):
            return nx.from_graph6_bytes(_file_name_to_graph6_bytes(subgraph_file_name.name))
    raise KeyError(f"Re-check when the red subgraph is {nx.to_graph6_bytes(subgraph,header=False)} and the blue subgraph is {nx.to_graph6_bytes(blue_subgraph,header=False)}")
                
def _make_edge_induced_subgraphs(graph_name):
    _make_graph_directory(graph_name)
    graph = _get_graph_from_name(graph_name)
    if os.path.exists(f"Graphs/{graph_name}/Subgraphs/{_graph6_bytes_to_file_name(nx.to_graph6_bytes(graph,header=False))}"):
        print(f"The unique edge-induced subgraphs of {graph_name} have already been made")
    else:
        print(f"Making the unique edge-induced subgraphs of {graph_name}")
        _send_workers(_make_edge_induced_subgraphs_helper, graph_name)
    return

def _make_edge_induced_subgraphs_helper(graph_name, worker_id, num_workers):
    graph = _get_graph_from_name(graph_name)
    for edge_induced_subgraph in _allocate_work(_distinct_edge_induced_subgraph_generator(graph), worker_id, num_workers):
        with open(f"Graphs/{graph_name}/Subgraphs/{_graph6_bytes_to_file_name(nx.to_graph6_bytes(edge_induced_subgraph,header=False))}", "wb") as output_file:
            output_file.write(nx.to_graph6_bytes(edge_induced_subgraph,header=False))
    return

def _make_poset(graph_name):
    _make_graph_directory(graph_name)
    graph = _get_graph_from_name(graph_name)
    if not os.path.exists(f"Graphs/{graph_name}/Subgraphs/{_graph6_bytes_to_file_name(nx.to_graph6_bytes(graph,header=False))}"):
        _make_edge_induced_subgraphs(graph_name)
    if os.path.exists(f"Graphs/{graph_name}/Poset/{_graph6_bytes_to_file_name(nx.to_graph6_bytes(graph,header=False))}"):
        print(f"The poset of {graph_name} have already been made")
    else:
        print(f"Making the poset of {graph_name}")
        _send_workers(_make_poset_helper, graph_name)
    return

def _make_poset_helper(graph_name, worker_id, num_workers):
    for host_graph_file_name in _allocate_work(os.scandir(f"Graphs/{graph_name}/Subgraphs/"), worker_id, num_workers):
        with open(f"Graphs/{graph_name}/Poset/{host_graph_file_name.name}", "wb") as output_file:
            host_graph = nx.from_graph6_bytes(_file_name_to_graph6_bytes(host_graph_file_name.name))
            for potential_subgraph_file_name in os.scandir(f"Graphs/{graph_name}/Subgraphs/"):
                potential_subgraph = nx.from_graph6_bytes(_file_name_to_graph6_bytes(potential_subgraph_file_name.name))
                if nx.algorithms.isomorphism.GraphMatcher(host_graph, potential_subgraph).subgraph_is_monomorphic():
                    output_file.write(nx.to_graph6_bytes(potential_subgraph, header=False))
    return

def _make_colorings(graph_name):
    _make_graph_directory(graph_name)
    graph = _get_graph_from_name(graph_name)
    if not os.path.exists(f"Graphs/{graph_name}/Subgraphs/{_graph6_bytes_to_file_name(nx.to_graph6_bytes(graph,header=False))}"):
        _make_edge_induced_subgraphs(graph_name)
    if not os.path.exists(f"Graphs/{graph_name}/Poset/{_graph6_bytes_to_file_name(nx.to_graph6_bytes(graph,header=False))}"):
        _make_poset(graph_name)
    if os.path.exists(f"Graphs/{graph_name}/Red-Blue Colorings/Part.1.g6"):
        print(f"The unioning each of the red and blue subgraphs of {graph_name} already exists")
    else:
        print(f"Unioning each of the red and blue subgraphs of {graph_name}")
        _send_workers(_make_colorings_helper, graph_name)
    return

def _make_colorings_helper(graph_name, worker_id, num_workers):
    for red_subgraph_file_name in _allocate_work(os.scandir(f"Graphs/{graph_name}/Subgraphs/"), worker_id, num_workers):
        red_subgraph = nx.from_graph6_bytes(_file_name_to_graph6_bytes(red_subgraph_file_name.name))
        try:
            blue_subgraph = _graph_complement(red_subgraph,graph_name)
        except KeyError:
            with open(f"Graphs/{graph_name}/Logs.txt", "a") as output_file:
                output_file.write(f"Re-check when the red subgraph is {_file_name_to_graph6_bytes(red_subgraph_file_name.name).strip()}\n")
            continue
        red_blue_union = _graph_iter_union_generator(_read_graph6(f"Graphs/{graph_name}/Poset/{red_subgraph_file_name.name}"),_read_graph6(f"Graphs/{graph_name}/Poset/{_graph6_bytes_to_file_name(nx.to_graph6_bytes(blue_subgraph,header=False))}"))
        with open(f"Graphs/{graph_name}/Red-Blue colorings/{red_subgraph_file_name.name}", "wb") as output_file:
            for graph in red_blue_union:
                output_file.write(nx.to_graph6_bytes(graph,header=False))

def _intersect_colorings(graph_name):
    _make_graph_directory(graph_name)
    graph = _get_graph_from_name(graph_name)
    if not os.path.exists(f"Graphs/{graph_name}/Subgraphs/{_graph6_bytes_to_file_name(nx.to_graph6_bytes(graph,header=False))}"):
        _make_edge_induced_subgraphs(graph_name)
    if not os.path.exists(f"Graphs/{graph_name}/Poset/{_graph6_bytes_to_file_name(nx.to_graph6_bytes(graph,header=False))}"):
        _make_poset(graph_name)
    if not os.path.exists(f"Graphs/{graph_name}/Red-Blue Colorings/{_graph6_bytes_to_file_name(nx.to_graph6_bytes(graph,header=False))}"):
        _make_colorings(graph_name)
    if os.path.exists(f"Graphs/{graph_name}/{graph_name} down-arrow ramsey set.g6"):
        print(f"The colorings of {graph_name} have already been processed")
    else:
        print(f"Processing the colorings of {graph_name} to generate the down-arrow Ramsey set")
        _send_workers(_intersect_colorings_helper, graph_name)
        print(f"Parsing the work done by the different workers on the down-arrow Ramsey set")
        down_arrow_ramsey_set = None
        for file_name in os.scandir(f"Graphs/{graph_name}/Down-Arrow Ramsey Set/"):
            with open(f"Graphs/{graph_name}/Down-Arrow Ramsey Set/{file_name.name}", "rb") as input_file:
                if down_arrow_ramsey_set == None:
                    down_arrow_ramsey_set = set(input_file)
                else:
                    down_arrow_ramsey_set = down_arrow_ramsey_set.copy().intersection(set(input_file))
        with open(f"Graphs/{graph_name}/{graph_name} down-arrow ramsey set.g6", "wb") as output_file:
            for line in down_arrow_ramsey_set:
                output_file.write(line)
    return

def _intersect_colorings_helper(graph_name, worker_id, num_workers):
    graph = _get_graph_from_name(graph_name)
    mem_needed = num_workers * sys.getsizeof(nx.to_graph6_bytes(graph)) * graph.number_of_edges()
    running_intersection = None
    for file_name in _allocate_work(os.scandir(f"Graphs/{graph_name}/Red-Blue Colorings/"), worker_id, num_workers):
        if psutil.virtual_memory().available < mem_needed:
            with open(f"Graphs/{graph_name}/Down-Arrow Ramsey Set/{int(max(os.listdir(f'Graphs/{graph_name}/Down-Arrow Ramsey Set/'), default='0 ').split(' ')[0])+1} -Thread {worker_id} waypoint.g6", "wb") as output_file:
                for line in running_intersection:
                    output_file.write(line)
            running_intersection = None
        else:
            if running_intersection == None:
                running_intersection = set()
                with open(f"Graphs/{graph_name}/Red-Blue colorings/{file_name.name}", "rb") as input_file:
                    for line in input_file:
                        running_intersection.add(line)
            else:
                with open(f"Graphs/{graph_name}/Red-Blue colorings/{file_name.name}", "rb") as input_file:
                    running_intersection = running_intersection.copy().intersection(set(input_file))
    if not running_intersection == None:
        with open(f"Graphs/{graph_name}/Down-Arrow Ramsey Set/{int(max(os.listdir(f'Graphs/{graph_name}/Down-Arrow Ramsey Set/'), default='0 ').split(' ')[0])+1} -Thread {worker_id} waypoint.g6", "wb") as output_file:
            for line in running_intersection:
                output_file.write(line)
"""
    _intersect_colorings_single_threaded is still here, because I haven't ran into an issue with the newly written one yet, and I'm not quite confident that it works as expected, and don't know how to make a unit test for it..... oops.
"""
# def _intersect_colorings_single_threaded(graph_name):
#     _make_graph_directory(graph_name)
#     graph = _get_graph_from_name(graph_name)
#     if not os.path.exists(f"Graphs/{graph_name}/Subgraphs/{_graph6_bytes_to_file_name(nx.to_graph6_bytes(graph,header=False))}"):
#         _make_edge_induced_subgraphs(graph_name)
#     if not os.path.exists(f"Graphs/{graph_name}/Poset/{_graph6_bytes_to_file_name(nx.to_graph6_bytes(graph,header=False))}"):
#         _make_poset(graph_name)
#     if not os.path.exists(f"Graphs/{graph_name}/Red-Blue Colorings/{_graph6_bytes_to_file_name(nx.to_graph6_bytes(graph,header=False))}"):
#         _make_colorings(graph_name)
#     if os.path.exists(f"Graphs/{graph_name}/{graph_name} down-arrow ramsey set.g6"):
#         print(f"The colorings of {graph_name} have already been processed")
#     else:
#         print(f"Processing the colorings of {graph_name} to generate the down-arrow Ramsey set")
#         down_arrow_ramsey_set = _unique_lines_from_file(f"Graphs/{graph_name}/Red-Blue Colorings/{_graph6_bytes_to_file_name(nx.to_graph6_bytes(_get_graph_from_name(graph_name),header=False))}")
#         for file_name in os.scandir(f"Graphs/{graph_name}/Red-Blue Colorings/"):
#             if file_name.name == _graph6_bytes_to_file_name(nx.to_graph6_bytes(_get_graph_from_name(graph_name),header=False)):
#                 continue
#             else:
#                 down_arrow_ramsey_set = _lines_in_both(down_arrow_ramsey_set,f"Graphs/{graph_name}/Red-Blue Colorings/{file_name.name}")
#         with open(f"Graphs/{graph_name}/{graph_name} down-arrow ramsey set.g6", "w+") as output_file:
#             for graph in down_arrow_ramsey_set:
#                 output_file.seek(0)
#                 if graph not in output_file:
#                     output_file.write(graph)
#     return

def _make_down_arrow_ramsey_set_ideals(graph_name):
    _make_graph_directory(graph_name)
    graph = _get_graph_from_name(graph_name)
    if not os.path.exists(f"Graphs/{graph_name}/Subgraphs/{_graph6_bytes_to_file_name(nx.to_graph6_bytes(graph,header=False))}"):
        _make_edge_induced_subgraphs(graph_name)
    if not os.path.exists(f"Graphs/{graph_name}/Poset/{_graph6_bytes_to_file_name(nx.to_graph6_bytes(graph,header=False))}"):
        _make_poset(graph_name)
    if not os.path.exists(f"Graphs/{graph_name}/Red-Blue Colorings/{_graph6_bytes_to_file_name(nx.to_graph6_bytes(graph,header=False))}"):
        _make_colorings(graph_name)
    if not os.path.exists(f"Graphs/{graph_name}/{graph_name} down-arrow ramsey set.g6"):
        _intersect_colorings(graph_name)
    if os.path.exists(f"Graphs/{graph_name}/{graph_name} down-arrow ramsey set ideals.g6"):
        print(f"The ideals of the down-arrow Ramsey set of {graph_name} have already been made")
    else:
        poset_graph = nx.DiGraph()
        for graph in _read_graph6(f"Graphs/{graph_name}/{graph_name} down-arrow ramsey set.g6"):
            node_graph6_string = _graph6_bytes_to_file_name(nx.to_graph6_bytes(graph,header=False)).split(".")[0]
            for subgraph in _read_graph6(f"Graphs/{graph_name}/Poset/{node_graph6_string}.g6"):
                subgraph_graph6_string = _graph6_bytes_to_file_name(nx.to_graph6_bytes(subgraph,header=False)).split(".")[0]
                if node_graph6_string != subgraph_graph6_string:
                    poset_graph.add_edge(subgraph_graph6_string,node_graph6_string)
        with open(f"Graphs/{graph_name}/{graph_name} down-arrow ramsey set ideals.g6", "wb") as output_file:
            for node in poset_graph.nodes():
                if poset_graph.out_degree(node)==0:
                    output_file.write(_file_name_to_graph6_bytes(f"{node}.g6")+b"\n")
        _draw_graphs(_read_graph6(f"Graphs/{graph_name}/{graph_name} down-arrow ramsey set ideals.g6"),f"Graphs/{graph_name}/{graph_name} down-arrow ramsey set ideals")