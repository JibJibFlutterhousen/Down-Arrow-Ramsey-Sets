import networkx as nx
import itertools as it
import matplotlib.pyplot as plt

import os
import math
# import pickle
import tempfile
import multiprocessing

def _edge_induced_subgraph_generator(Graph):
    for subset_size in range(Graph.number_of_edges()+1):
        # print("Using", subset_size, "edges")
        for subset in it.combinations(Graph.edges(),subset_size):
            edge_subgraph = Graph.edge_subgraph(subset)
            yield edge_subgraph

def _distinct_subgraph_generator_list(Graph):
    distinct_edge_induced_subgraphs = list()
    for edge_induced_subgraph in _edge_induced_subgraph_generator(Graph):
        for distinct_edge_induced_subgraph in distinct_edge_induced_subgraphs:
            if nx.is_isomorphic(edge_induced_subgraph,distinct_edge_induced_subgraph):
                break
        else:
            distinct_edge_induced_subgraphs.append(edge_induced_subgraph)
            subgraph = edge_induced_subgraph.copy()
            while subgraph.number_of_nodes() < Graph.number_of_nodes():
                subgraph.add_node(max(subgraph.nodes(), default=0)+1)
            yield subgraph

# def _read_graph6(Path):
#     for graph in nx.read_graph6(Path):
#         if type(graph) == type(nx.empty_graph()):
#             yield graph 
#         else:
#             single_graph = nx.read_graph6(Path)
#             yield single_graph
#             break
            
# def _distinct_edge_induced_subgraph_generator_file(Graph):
#     with tempfile.TemporaryFile() as tmp:
#         for edge_induced_subgraph in _edge_induced_subgraph_generator(Graph):
#             tmp.seek(0)
#             for distinct_edge_induced_subgraph in _read_graph6(tmp):
#                 if nx.is_isomorphic(edge_induced_subgraph,distinct_edge_induced_subgraph):
#                     break
#             else:
#                 tmp.write(nx.to_graph6_bytes(edge_induced_subgraph, header=False))
#                 yield edge_induced_subgraph
                
def needs_edge(Poset_graph, Edge):
    source_node, target_node = Edge
    if (source_node, target_node) in Poset_graph.edges():
        return False, (None, None)
    elif (target_node, source_node) in Poset_graph.edges():
        return False, (None, None)
    else:
        source_graph = nx.from_graph6_bytes(Poset_graph.nodes[source_node]["graph6_bytes"])
        target_graph = nx.from_graph6_bytes(Poset_graph.nodes[target_node]["graph6_bytes"])
        if nx.algorithms.isomorphism.GraphMatcher(target_graph, source_graph).subgraph_is_monomorphic():
            return True, (source_node, target_node)
        elif nx.algorithms.isomorphism.GraphMatcher(source_graph, target_graph).subgraph_is_monomorphic():
            return True, (target_node, source_node)
        else:
            return False, (None, None)

def _seed_poset(Host_graph):
    print("Determining the unique subgraphs")
    poset_graph = nx.DiGraph()
    [poset_graph.add_node(poset_graph.number_of_nodes(), graph6_bytes = nx.to_graph6_bytes(graph, header=False).strip()) for graph in _distinct_subgraph_generator_list(Host_graph)]
    return poset_graph

def _add_edges_to_poset(Poset_graph):
    print("Determining the poset structure")
    with multiprocessing.Pool() as pool:
        for x in range(1,math.ceil(Poset_graph.number_of_nodes()/2),1):
            seed_graph = nx.circulant_graph(Poset_graph.number_of_nodes(), (x,))
            arguments = zip(it.repeat(Poset_graph), seed_graph.edges())
            for truth_value, edge in pool.starmap(needs_edge, arguments):
                if truth_value:
                    Poset_graph.add_edge(*edge)
            print(f"pass {x}", Poset_graph, nx.transitive_closure(Poset_graph))
            Poset_graph = nx.transitive_closure(Poset_graph)
    return Poset_graph

def make_poset(Host_graph):
    empty_poset = _seed_poset(Host_graph)
    poset = _add_edges_to_poset(empty_poset)
    return poset

def isomorphic_node(Poset_graph, Target_graph):
    for node in Poset_graph.nodes():
        if nx.is_isomorphic(Target_graph, nx.from_graph6_bytes(Poset_graph.nodes[node]["graph6_bytes"])):
            return node


def complement_node(Poset_graph, Source_node):
    source_graph = nx.from_graph6_bytes(Poset_graph.nodes[Source_node]["graph6_bytes"])
    target_graph = nx.complement(source_graph)
    target_node = isomorphic_node(Poset_graph, target_graph)
    return target_node

def make_colorings(Poset_graph):
    print("Determining the structure of all colorings")
    colorings = dict()
    duplicate_colorings = set()

    for red_node in Poset_graph.nodes():
        if red_node in duplicate_colorings:
            continue
        else:
            blue_node = complement_node(Poset_graph, red_node)
            colorings[len(colorings)] = {"red_node":red_node, "blue_node":blue_node, "coloring_nodes":set(node for node in Poset_graph.nodes() if (node, red_node) in Poset_graph.edges() or (node, blue_node) in Poset_graph.edges())}
            duplicate_colorings.add(blue_node)

    return colorings

def make_down_arrow_set(Colorings):
    print("Determining the down-arrow Ramsey set")
    down_arrow_set_nodes = set(Colorings)
    for coloring_id in Colorings:
        down_arrow_set_nodes.intersection_update(Colorings[coloring_id]["coloring_nodes"])
    return down_arrow_set_nodes

if __name__ == "__main__":
    k = 5

# Make the poset, sometimes because the subgraphs exist already
    host_graph = nx.complete_graph(k)
    file_name = f"graph{k}.g6"
    if os.path.exists(file_name):
        empty_poset = nx.DiGraph()
        [empty_poset.add_node(empty_poset.number_of_nodes(), graph6_bytes = nx.to_graph6_bytes(graph, header=False).strip()) for graph in nx.read_graph6(file_name)]
        poset_graph = _add_edges_to_poset(empty_poset)
    else:
        poset_graph = make_poset(host_graph)

# Make the colorings based off of the poset
    colorings = make_colorings(poset_graph)

# Determine the down-arrow Ramsey set
    down_arrow_set_nodes = make_down_arrow_set(colorings)

# Make the ideals of the down-arrow Ramsey set
    down_arrow_set_poset = nx.induced_subgraph(poset_graph, down_arrow_set_nodes)
    down_arrow_set_poset_ideals_nodes = set(node for node in down_arrow_set_poset.nodes() if down_arrow_set_poset.out_degree(node)==0)

# Draw each of the ideals of the down-arrow Ramsey set
    for node in down_arrow_set_poset_ideals_nodes:
        nx.draw_circular(nx.from_graph6_bytes(poset_graph.nodes[node]["graph6_bytes"]), with_labels=True, node_color="lightgrey", edge_color="lightgrey")
        plt.show()
