import http.client
import json
import csv
from sys import exit
from time import time

#############################################################################################################################
# cse6242 
# All instructions, code comments, etc. contained within this notebook are part of the assignment instructions.
# Portions of this file will auto-graded in Gradescope using different sets of parameters / data to ensure that values are not
# hard-coded.
#
# Instructions:  Implement all methods in this file that have a return
# value of 'NotImplemented'. See the documentation within each method for specific details, including
# the expected return value
#
# Helper Functions:
# You are permitted to write additional helper functions/methods or use additional instance variables within
# the `Graph` class or `TMDbAPIUtils` class so long as the originally included methods work as required.
#
# Use:
# The `Graph` class  is used to represent and store the data for the TMDb co-actor network graph.  This class must
# also provide some basic analytics, i.e., number of nodes, edges, and nodes with the highest degree.
#
# The `TMDbAPIUtils` class is used to retrieve Actor/Movie data using themoviedb.org API.  We have provided a few necessary methods
# to test your code w/ the API, e.g.: get_movie_cast(), get_movie_credits_for_person().  You may add additional
# methods and instance variables as desired (see Helper Functions).
#
# The data that you retrieve from the TMDb API is used to build your graph using the Graph class.  After you build your graph using the
# TMDb API data, use the Graph class write_edges_file & write_nodes_file methods to produce the separate nodes and edges
# .csv files for submission to Gradescope.
#
# While building the co-actor graph, you will be required to write code to expand the graph by iterating
# through a portion of the graph nodes and finding similar artists using the TMDb API. We will not grade this code directly
# but will grade the resulting graph data in your nodes and edges .csv files.
#
#############################################################################################################################


class Graph:

    # Do not modify
    def __init__(self, with_nodes_file=None, with_edges_file=None):
        """
        option 1:  init as an empty graph and add nodes
        option 2: init by specifying a path to nodes & edges files
        """
        self.nodes = [] #option1
        self.edges = [] #option1
        if with_nodes_file and with_edges_file: #option2
            nodes_CSV = csv.reader(open(with_nodes_file))
            nodes_CSV = list(nodes_CSV)[1:]
            self.nodes = [(n[0], n[1]) for n in nodes_CSV]
            edges_CSV = csv.reader(open(with_edges_file))
            edges_CSV = list(edges_CSV)[1:]
            self.edges = [(e[0], e[1]) for e in edges_CSV]

    def add_node(self, id: str, name: str) -> None:
        """
        add a tuple (id, name) representing a node to self.nodes if it does not already exist
        The graph should not contain any duplicate nodes
        """
        # self.nodeDict={}
        tpl1=(id,name) #set tuple
        if tpl1 not in self.nodes:
            self.nodes.append(tpl1)
            # self.nodeDict[tpl1[0]]=tpl1[1]

    def add_edge(self, source: str, target: str) -> None:
        """
        Add an edge between two nodes if it does not already exist.
        An edge is represented by a tuple containing two strings: e.g.: ('source', 'target').
        Where 'source' is the id of the source node and 'target' is the id of the target node
        e.g., for two nodes with ids 'a' and 'b' respectively, add the tuple ('a', 'b') to self.edges
        """
        tpl1 = (source,target)
        tpl2 = (target,source)
        if tpl1 not in self.edges and tpl2 not in self.edges:
            self.edges.append(tpl1)

    def total_nodes(self) -> int:
        """
        Returns an integer value for the total number of nodes in the graph
        """
        return len(self.nodes) # [(a,b),(c,d)] ->2

    def total_edges(self) -> int:
        """
        Returns an integer value for the total number of edges in the graph
        """
        return len(self.edges) # [(a,b),(c,d)] ->2

    def max_degree_nodes(self) -> dict:
        """
        Return the node(s) with the highest degree
        Return multiple nodes in the event of a tie
        Format is a dict where the key is the node_id and the value is an integer for the node degree
        e.g. {'a': 8}
        or {'a': 22, 'b': 22}
        """
        self.dict0={}

        for n1, n2 in self.edges:
            if n1 in self.dict0.keys():
                self.dict0[n1] += 1
            else:
                self.dict0[n1] = 1
            if n2 in self.dict0.keys():
                self.dict0[n2] += 1
            else:
                self.dict0[n2] = 1

        # max degree find
        max_value = max(self.dict0.values())
        max_keys = [key for key, value in self.dict0.items() if value == max_value]

        d={}
        if len(max_keys) == 1:
            d = {max_keys[0]:max_value}
        else:
            for key in max_keys:
                d[key] = max_value
        return d


    def print_nodes(self):
        """
        No further implementation required
        May be used for de-bugging if necessary
        """
        print(self.nodes)

    def print_edges(self):
        """
        No further implementation required
        May be used for de-bugging if necessary
        """
        print(self.edges)

    # Do not modify
    def write_edges_file(self, path="edges.csv")->None:
        """
        write all edges out as .csv
        :param path: string
        :return: None
        """
        edges_path = path
        edges_file = open(edges_path, 'w', encoding='utf-8')

        edges_file.write("source" + "," + "target" + "\n")

        for e in self.edges:
            edges_file.write(e[0] + "," + e[1] + "\n")

        edges_file.close()
        print("finished writing edges to csv")

    # Do not modify
    def write_nodes_file(self, path="nodes.csv")->None:
        """
        write all nodes out as .csv
        :param path: string
        :return: None
        """
        nodes_path = path
        nodes_file = open(nodes_path, 'w', encoding='utf-8')

        nodes_file.write("id,name" + "\n")
        for n in self.nodes:
            nodes_file.write(n[0] + "," + n[1] + "\n")
        nodes_file.close()
        print("finished writing nodes to csv")

class  TMDBAPIUtils:

    # Do not modify
    def __init__(self, api_key:str):
        self.api_key = api_key

        # print('print API:', self.api_key)

    def get_movie_cast(self, movie_id:str, limit:int=None, exclude_ids:list=[]) -> list:
        """
        Get the movie cast for a given movie id, with optional parameters to exclude an cast member
        from being returned and/or to limit the number of returned cast members
        documentation url: https://developers.themoviedb.org/3/movies/get-movie-credits

        :param string movie_id: a movie_id
        :param list exclude_ids: a list of ints containing ids (not cast_ids) of cast members  that should be excluded from the returned result
            e.g., if exclude_ids are [353, 455] then exclude these from any result.
        :param integer limit: maximum number of returned cast members by their 'order' attribute
            e.g., limit=5 will attempt to return the 5 cast members having 'order' attribute values between 0-4
            If after excluding, there are fewer cast members than the specified limit, then return the remaining members (excluding the ones whose order values are outside the limit range). 
            If cast members with 'order' attribute in the specified limit range have been excluded, do not include more cast members to reach the limit.
            If after excluding, the limit is not specified, then return all remaining cast members."
            e.g., if limit=5 and the actor whose id corresponds to cast member with order=1 is to be excluded,
            return cast members with order values [0, 2, 3, 4], not [0, 2, 3, 4, 5]
        :rtype: list
            return a list of dicts, one dict per cast member with the following structure:
                [{'id': '97909' # the id of the cast member
                'character': 'John Doe' # the name of the character played
                'credit_id': '52fe4249c3a36847f8012927' # id of the credit, ...}, ... ]
                Note that this is an example of the structure of the list and some of the fields returned by the API.
                The result of the API call will include many more fields for each cast member.
        """
        # e.g. movie_id = '329'
        movie_id = int(movie_id)

        # get data via api
        import urllib.request
        import json
        url = f"https://api.themoviedb.org/3/movie/{movie_id}/credits?language=en-US&api_key={self.api_key}"
        # url = f"https://api.themoviedb.org/3/movie/{movie_id}/credits?language=en-US"
        headers = {"accept": "application/json"
                   # "Authorization": self.api_key
        }

        req = urllib.request.Request(url,headers=headers)
        with urllib.request.urlopen(req) as response:
            data = response.read()
            data = json.loads(data.decode('utf-8'))
        # print("Data:", data)

        new_list=[]
        # filter: exclude

        for element in data['cast']:
            cast_id = element['id']     #not cast_id
            if cast_id not in exclude_ids:
                if limit:
                    if element['order'] >= limit:
                        continue
                element['id']=str(cast_id)
                new_list.append(element) # element: dict
        return new_list

    def get_movie_credits_for_person(self, person_id:str, vote_avg_threshold:float=None)->list:
        """
        Using the TMDb API, get the movie credits for a person serving in a cast role
        documentation url: https://developers.themoviedb.org/3/people/get-person-movie-credits

        :param string person_id: the id of a person
        :param vote_avg_threshold: optional parameter to return the movie credit if it is >=
            the specified threshold.
            e.g., if the vote_avg_threshold is 5.0, then only return credits with a vote_avg >= 5.0
        :rtype: list
            return a list of dicts, with each dict having 'id', 'title', and 'vote_avg' keys, 
            one dict per movie credit with the following structure:
                [{'id': '97909' # the id of the movie
                'title': 'Long, Stock and Two Smoking Barrels' # the title (not original title) of the credit
                'vote_avg': 5.0 # the float value of the vote average value for the credit}, ... ]
        """

        # e.g person_id '4785'
        person_id = int(person_id)

        # get data via api
        import urllib.request
        import json

        url = f"https://api.themoviedb.org/3/person/{person_id}/movie_credits?language=en-US&api_key={self.api_key}"
        headers = {
            "accept": "application/json"
        }

        req = urllib.request.Request(url, headers=headers)

        with urllib.request.urlopen(req) as response:
            data = response.read()
            data = json.loads(data.decode('utf-8'))

        # return a list of dicts having the 3 keys if vote_avg_threshold is true
        new_list = []
        if vote_avg_threshold:
            for element in data['cast']:    #list of movies the person in
                if element['vote_average'] >= vote_avg_threshold:
                    d = {'id': str(element['id']),  #movie_id
                         'title': element['title'],
                         'vote_avg': element['vote_average']}
                    new_list.append(d)
        else:
            for element in data['cast']:  # list of movies the person in
                d = {'id': str(element['id']),  # movie_id
                     'title': element['title'],
                     'vote_avg': element['vote_average']}
                new_list.append(d)
        return new_list #only return 3 key-values

#############################################################################################################################
#
# BUILDING YOUR GRAPH
#
# Working with the API:  See use of http.request: https://docs.python.org/3/library/http.client.html#examples
#
# Using TMDb's API, build a co-actor network for the actor's/actress' highest rated movies
# In this graph, each node represents an actor
# An edge between any two nodes indicates that the two actors/actresses acted in a movie together
# i.e., they share a movie credit.
# e.g., An edge between Samuel L. Jackson and Robert Downey Jr. indicates that they have acted in one
# or more movies together.
#
# For this assignment, we are interested in a co-actor network of highly rated movies; specifically,
# we only want the top 3 co-actors in each movie credit of an actor having a vote average >= 8.0.
# Build your co-actor graph on the actor 'Laurence Fishburne' w/ person_id 2975.
#
# You will need to add extra functions or code to accomplish this.  We will not directly call or explicitly grade your
# algorithm. We will instead measure the correctness of your output by evaluating the data in your nodes.csv and edges.csv files.
#
# GRAPH SIZE
# With each iteration of your graph build, the number of nodes and edges grows approximately at an exponential rate.
# Our testing indicates growth approximately equal to e^2x.
# Since the TMDB API is a live database, the number of nodes / edges in the final graph will vary slightly depending on when
# you execute your graph building code. We take this into account by rebuilding the solution graph every few days and
# updating the auto-grader.  We establish a bound for lowest & highest encountered numbers of nodes and edges with a
# margin of +/- 100 for nodes and +/- 150 for edges.  e.g., The allowable range of nodes is set to:
#
# Min allowable nodes = min encountered nodes - 100
# Max allowable nodes = max allowable nodes + 100
#
# e.g., if the minimum encountered nodes = 507 and the max encountered nodes = 526, then the min/max range is 407-626
# The same method is used to calculate the edges with the exception of using the aforementioned edge margin.
# ----------------------------------------------------------------------------------------------------------------------
# BEGIN BUILD CO-ACTOR NETWORK
#
# INITIALIZE GRAPH
#   Initialize a Graph object with a single node representing Laurence Fishburne
#
# BEGIN BUILD BASE GRAPH:
#   Find all of Laurence Fishburne's movie credits that have a vote average >= 8.0
#   FOR each movie credit:
#   |   get the movie cast members having an 'order' value between 0-2 (these are the co-actors)
#   |
#   |   FOR each movie cast member:
#   |   |   using graph.add_node(), add the movie cast member as a node (keep track of all new nodes added to the graph)
#   |   |   using graph.add_edge(), add an edge between the Laurence Fishburne (actor) node
#   |   |   and each new node (co-actor/co-actress)
#   |   END FOR
#   END FOR
# END BUILD BASE GRAPH
#
#
# BEGIN LOOP - DO 2 TIMES:
#   IF first iteration of loop:
#   |   nodes = The nodes added in the BUILD BASE GRAPH (this excludes the original node of Laurence Fishburne!)
#   ELSE
#   |    nodes = The nodes added in the previous iteration:
#   ENDIF
#
#   FOR each node in nodes:
#   |  get the movie credits for the actor that have a vote average >= 8.0
#   |
#   |   FOR each movie credit:
#   |   |   try to get the 3 movie cast members having an 'order' value between 0-2
#   |   |
#   |   |   FOR each movie cast member:
#   |   |   |   IF the node doesn't already exist:
#   |   |   |   |    add the node to the graph (track all new nodes added to the graph)
#   |   |   |   ENDIF
#   |   |   |
#   |   |   |   IF the edge does not exist:
#   |   |   |   |   add an edge between the node (actor) and the new node (co-actor/co-actress)
#   |   |   |   ENDIF
#   |   |   END FOR
#   |   END FOR
#   END FOR
# END LOOP
#
# Your graph should not have any duplicate edges or nodes
# Write out your finished graph as a nodes file and an edges file using:
#   graph.write_edges_file()
#   graph.write_nodes_file()
#
# END BUILD CO-ACTOR NETWORK
# ----------------------------------------------------------------------------------------------------------------------

# Exception handling and best practices
# - You should use the param 'language=en-US' in all API calls to avoid encoding issues when writing data to file.
# - If the actor name has a comma char ',' it should be removed to prevent extra columns from being inserted into the .csv file
# - Some movie_credits do not return cast data. Handle this situation by skipping these instances.
# - While The TMDb API does not have a rate-limiting scheme in place, consider that making hundreds / thousands of calls
#   can occasionally result in timeout errors. If you continue to experience 'ConnectionRefusedError : [Errno 61] Connection refused',
#   - wait a while and then try again.  It may be necessary to insert periodic sleeps when you are building your graph.


def return_name()->str:
    """
    Return a string containing your GT Username
    e.g., gburdell3
    Do not return your 9 digit GTId
    """
    return 'nchi7'
## Build Base Graph

def build_base_graph(actor_id,actor_name, graph,tmdb_obj)->Graph:
    # actor_name=self.nodeDict[actor_id]
    # tmdb_obj=TMDBAPIUtils(api_key="e69b69740a64b56e2e80aa82fda13638")
    movie_credits = tmdb_obj.get_movie_credits_for_person(actor_id, vote_avg_threshold = 8.0)

#   [{'id': '97909' # the id of the movie
#                'title': 'Long, Stock and Two Smoking Barrels' # the title (not original title) of the credit
#                'vote_avg': 5.0 # the float value of the vote average value for the credit},{},{} ... ]


##  use 'id' as movie_id for get_movie_cast(movie_id, limit, exclude_ids) to get all casts
#   [{'id': '97909' # the id of the cast member
#                'character': 'John Doe' # the name of the character played
#                'credit_id': '52fe4249c3a36847f8012927' # id of the credit, ...}, {}... ]

    #   get a list of movie_id and use the get_movie_cast() to get cast of the movie, limit to 3
    mov_list =[d['id'] for d in movie_credits]

    #   get cast member of each movie, limit to top3 co_actor
    for i in mov_list: #['1','2','3']
        cast_member = tmdb_obj.get_movie_cast(movie_id=i,limit=3,exclude_ids=[actor_id])

        for co_actor in cast_member:
            co_id = co_actor['id']
            co_name = co_actor['name']
            graph.add_node(co_id, co_name)
            graph.add_edge(actor_id, co_id)
            #TODO: assume passed in by ref.
    ## End Build Base Graph

    # remember to exclude original node of Laurence Fishburne



# You should modify __main__ as you see fit to build/test your graph using  the TMDBAPIUtils & Graph classes.
# Some boilerplate/sample code is provided for demonstration. We will not call __main__ during grading.
if __name__ == "__main__":
    t0=time()
    graph = Graph()
    graph.add_node(id='2975', name='Laurence Fishburne')
    tmdb_api_utils = TMDBAPIUtils(api_key='e69b69740a64b56e2e80aa82fda13638')

    # call functions or place code here to build graph (graph building code not graded)
    # Suggestion: code should contain steps outlined above in BUILD CO-ACTOR NETWORK


    # Function to build the base graph


    # Initial build of base graph
    t1=time()
    print("Time elapsed before build graph.",t1-t0)
    build_base_graph('2975','Laurence Fishburne', graph,tmdb_api_utils)
    t2=time()
    print("Time elapsed after build graph.", t2 - t1)
    ## Loop 2 time:

    # nodes = graph.nodes[1:]  # Exclude the original node of Laurence Fishburne

    for run in range(2):  # Loop twice as described
        if run == 0:
            nodes = graph.nodes[1:]
        # else:
        #     nodes = graph.nodes
        for id,name in nodes:
            movie_credits = tmdb_api_utils.get_movie_credits_for_person(id, vote_avg_threshold=8.0)
            print(f"iter{run},node num:",len(graph.nodes))
            for movie_credit in movie_credits:
                movie_id=movie_credit['id']
                cast_list=tmdb_api_utils.get_movie_cast(movie_id=movie_id, limit=3)

                for ind,cast in enumerate(cast_list):
                    graph.add_node(cast['id'],cast['name'])
                    for i in range(ind+1,len(cast_list)):
                        oth_cast=cast_list[i]
                        graph.add_edge(cast['id'],oth_cast['id'])
    t3=time()
    print("Time for massive shit",t3-t2)
    #   FOR each node in nodes:
    #   |  get the movie credits for the actor that have a vote average >= 8.0
    #   |
    #   |   FOR each movie credit:
    #   |   |   try to get the 3 movie cast members having an 'order' value between 0-2
    #   |   |
    #   |   |   FOR each movie cast member:
    #   |   |   |   IF the node doesn't already exist:
    #   |   |   |   |    add the node to the graph (track all new nodes added to the graph)
    #   |   |   |   ENDIF
    #   |   |   |
    #   |   |   |   IF the edge does not exist:
    #   |   |   |   |   add an edge between the node (actor) and the new node (co-actor/co-actress)
    #   |   |   |   ENDIF
    #   |   |   END FOR
    #   |   END FOR
    #   END FOR
    # END LOOP

    graph.write_edges_file()
    graph.write_nodes_file()

    # If you have already built & written out your graph, you could read in your nodes & edges files
    # to perform testing on your graph.
    # graph = Graph(with_edges_file="edges.csv", with_nodes_file="nodes.csv")
