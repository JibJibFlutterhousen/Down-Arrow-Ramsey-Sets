import DownArrow as da

if __name__ == '__main__':
    graphs = ["K_4", "K_5", "K_6", "K_7",
    "K_1,1", "K_1,2", "K_1,3", "K_1,4", "K_1,5", "K_1,6", 
    "K_2,1", "K_2,2", "K_2,3", "K_2,4", "K_2,5", "K_2,6", 
    "K_3,1", "K_3,2", "K_3,3", "K_3,4", "K_3,5", "K_3,6", 
    "K_4,1", "K_4,2", "K_4,3", "K_4,4", "K_4,5", "K_4,6", 
    ]
    for graph in graphs:
        da.make_down_arrow_set(graph)