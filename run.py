import dag
import os
import networkx as nx

if __name__ == '__main__':
    graph_name = "K_4"
    dag._make_down_arrow_ramsey_set_ideals(graph_name)
    # for i in range(2,6,1):
    #     print(f"working on K_{i}")
    #     graph_name = f"K_{i}"
    #     dag._make_graph_directory(graph_name)
    #     if not os.path.exists(f"Graphs/{graph_name}/K_{i}.Unique.Edge.Induced.Subgraphs.g6"):
    #         print("Removing isolated edges from the downloaded file")
    #         with open(f"Graphs/{graph_name}/K_{i}.Unique.Edge.Induced.Subgraphs.g6","wb") as output_file:
    #             with open(f"graph{i}.g6", "rb") as input_file:
    #                 for graph6_byte_string in input_file:
    #                     graph = nx.from_graph6_bytes(graph6_byte_string.strip())
    #                     graph.remove_nodes_from(node for node,degree in list(graph.degree()) if degree==0)
    #                     output_file.write(nx.to_graph6_bytes(graph,header=False))
    #     for edge_induced_subgraph in nx.read_graph6(f"Graphs/{graph_name}/K_{i}.Unique.Edge.Induced.Subgraphs.g6"):
    #         with open(f"Graphs/{graph_name}/Subgraphs/{dag._graph6_bytes_to_file_name(nx.to_graph6_bytes(edge_induced_subgraph,header=False))}", "wb") as output_file:
    #             output_file.write(nx.to_graph6_bytes(edge_induced_subgraph,header=False))
    #     dag._make_down_arrow_ramsey_set_ideals(graph_name)
    # # graph_name = "K_2,3"
    # # # dag._draw_graphs(dag._distinct_edge_induced_subgraph_generator(dag._get_graph_from_name(graph_name)),graph_name)
    # # dag._intersect_colorings(graph_name)
    # # # dag._make_down_arrow_ramsey_set(graph_name)
    # # # [print(line) for line in dag._unique_lines_from_file("Graphs/K_2,2/Red-Blue Colorings/Bo.g6")]