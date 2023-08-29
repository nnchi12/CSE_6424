## Build Base Graph

def build_base_graph(actor_name, graph):
    movie_credits = get_movie_credits_for_person(actor_name, vote_average = 8.0)

#   [{'id': '97909' # the id of the movie
#                'title': 'Long, Stock and Two Smoking Barrels' # the title (not original title) of the credit
#                'vote_avg': 5.0 # the float value of the vote average value for the credit},{},{} ... ]


##  use 'id' as movie_id for get_movie_cast(movie_id, limit, exclude_ids) to get all casts
#   [{'id': '97909' # the id of the cast member
#                'character': 'John Doe' # the name of the character played
#                'credit_id': '52fe4249c3a36847f8012927' # id of the credit, ...}, ... ]

    #   get a list of movie_id and use the get_movie_cast() to get cast of the movie, limit to 3
    mov_list =[d['id'] for d in movie_credits]

    #   get cast member of each movie, limit to top3 co_actor
    for i in mov_list:
        cast_member = get_movie_cast(i, 3)  #i is str

        for co_actor in cast_member:
                co_id = co_actor['id']
                co_name = co_actor['name']
                if (co_id, co_name) not in graph.nodes():
                    graph.add_node(co_id, co_name)
                graph.add_edge(actor_name, co_name)


build_base_graph("Laurence Fishburne", graph)
## End Build Base Graph


## Loop 2 time:
# remember to exclude original node of Laurence Fishburne
    for _ in range(2):
        nodes = graph.nodes() if _ > 0 else graph.nodes()[1:]

        for node in nodes:
            build_base_graph(node[1], graph)       # node = (co_id, co_name)

