import graphviz

from doge_datagen import DataOnlineGenerator


class DataOnlineGeneratorGrapher:
    def __init__(self,
                 generator: DataOnlineGenerator):
        self.generator = generator

    def render(self):
        graph = self.generate_graph()
        graph.render('doctest-output/out.gv', view=True)

    def generate_graph(self) -> graphviz.Digraph:
        graph = graphviz.Digraph()
        for state in self.generator.states:
            graph.node(state)
        for state in self.generator.transition_matrix.keys():
            for trigger in self.generator.transition_matrix[state].keys():
                if trigger != DataOnlineGenerator.STAY_TRIGGER:
                    transition = self.generator.transition_matrix[state][trigger]
                    graph.edge(transition.from_state, transition.to_state,
                               label=f"{transition.trigger}(P={transition.probability}%)")
        return graph
