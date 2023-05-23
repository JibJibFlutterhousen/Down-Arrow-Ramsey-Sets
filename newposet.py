import networkx as nx
import itertools as it
import matplotlib.pyplot as plt

import tempfile
import multiprocessing

def _read_graph6(path):
    for graph in nx.read_graph6(path):
        if type(graph) == type(nx.empty_graph()):
            yield graph 
        else:
            single_graph = nx.read_graph6(path)
            yield single_graph
            break

def _edge_induced_subgraph_generator(graph):
    for subset_size in range(graph.number_of_edges()+1):
        print(f"  Using {subset_size} edges")
        for subset in it.combinations(graph.edges(),subset_size):
            yield graph.edge_subgraph(subset)

def _distinct_edge_induced_subgraph_generator_list(graph):
    distinct_edge_induced_subgraphs = list()
    for edge_induced_subgraph in _edge_induced_subgraph_generator(graph):
        for distinct_edge_induced_subgraph in distinct_edge_induced_subgraphs:
            if nx.is_isomorphic(edge_induced_subgraph,distinct_edge_induced_subgraph):
                break
        else:
            distinct_edge_induced_subgraphs.append(edge_induced_subgraph)
            yield edge_induced_subgraph
            
# def _distinct_edge_induced_subgraph_generator_file(graph):
#     with tempfile.TemporaryFile() as tmp:
#         for edge_induced_subgraph in _edge_induced_subgraph_generator(graph):
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
        False, (None, None)
    elif (target_node, source_node) in Poset_graph.edges():
        False, (None, None)
    else:
        source_graph = nx.from_graph6_bytes(Poset_graph.nodes[source_node]["graph6_bytes"])
        target_graph = nx.from_graph6_bytes(Poset_graph.nodes[target_node]["graph6_bytes"])
        if nx.algorithms.isomorphism.GraphMatcher(target_graph, source_graph).subgraph_is_monomorphic():
            return True, (source_node, target_node)
        elif nx.algorithms.isomorphism.GraphMatcher(source_graph, target_graph).subgraph_is_monomorphic():
            return True, (target_node, source_node)
    return False, (None, None)

def make_poset(Host_graph):
    poset_graph = nx.DiGraph()
    print("Making unique edge-induced subgraphs")
    for graph in _distinct_edge_induced_subgraph_generator_list(Host_graph):
        poset_graph.add_node(poset_graph.number_of_nodes(), graph6_bytes=nx.to_graph6_bytes(graph, header=False).strip())
    print("Done")
    with multiprocessing.Pool() as pool:
        for x in range(1,poset_graph.number_of_nodes(),1):
            seed_graph = nx.circulant_graph(poset_graph.number_of_nodes(), (x,))
            arguments = zip(it.repeat(poset_graph), seed_graph.edges())
            for truth_value, edge in pool.starmap(needs_edge, arguments):
                if truth_value:
                    print(edge)
                    poset_graph.add_edge(*edge)
            print(f"pass {x}", poset_graph, nx.transitive_closure(poset_graph))
            poset_graph = nx.transitive_closure(poset_graph)
    return poset_graph

if __name__ == "__main__":
    drawingparameters = {"with_labels":True, "node_color":"lightgrey", "edge_color":"lightgrey", "font_color":"black"}

    poset_graph = make_poset(nx.complete_graph(6))

    poset_drawingparameters = dict(drawingparameters)
    poset_drawingparameters["labels"] = {node:poset_graph.nodes[node]["graph6_bytes"] for node in poset_graph.nodes()}

    nx.draw_circular(poset_graph, **poset_drawingparameters)
    plt.show()