from graphviz import Digraph

def generate_data_flow_diagram(output_path="./docs/data_flow_diagram.png"):
    dot = Digraph(comment="Data Flow Diagram", format="png")
    
    # Nodes
    dot.node("A", "Airbnb Data (CSV)")
    dot.node("B", "Kamernet Data (JSON)")
    dot.node("C", "GeoJSON Data")
    dot.node("D", "Ingestion")
    dot.node("E", "Cleaning")
    dot.node("F", "Enrichment")
    dot.node("G", "Transformations")
    dot.node("H", "Processed Output")

    # Edges
    dot.edges([("A", "D"), ("B", "D"), ("C", "F")])
    dot.edges([("D", "E"), ("E", "F"), ("F", "G"), ("G", "H")])

    # Save
    dot.render(output_path)
    print(f"Data flow diagram saved to {output_path}")

generate_data_flow_diagram()

def generate_cicd_diagram(output_path="./docs/cicd_diagram.png"):
    dot = Digraph(comment="CI/CD Pipeline Diagram", format="png")

    # Nodes
    dot.node("A", "Developer Commit")
    dot.node("B", "Pre-Commit Hooks")
    dot.node("C", "CI/CD Pipeline")
    dot.node("D", "Unit Tests")
    dot.node("E", "Integration Tests")
    dot.node("F", "Deployment")

    # Edges
    dot.edges([("A", "B"), ("B", "C"), ("C", "D"), ("D", "E"), ("E", "F")])

    # Save
    dot.render(output_path)
    print(f"CI/CD diagram saved to {output_path}")

generate_cicd_diagram()
