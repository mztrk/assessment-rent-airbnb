from graphviz import Digraph


def generate_data_flow_diagram(output_path="./docs/data_flow_diagram.png"):
    """
    Generates a data flow diagram for the pipeline and saves it as a PNG file.

    Args:
        output_path (str): Path to save the generated data flow diagram.

    Diagram Details:
        - Airbnb Data (CSV), Kamernet Data (JSON), and GeoJSON Data are the input sources.
        - Data undergoes Ingestion, Cleaning, Enrichment, and Transformations steps.
        - Processed output is generated at the end of the pipeline.
    """
    dot = Digraph(comment="Data Flow Diagram", format="png")

    # Nodes representing different stages of the pipeline
    dot.node("A", "Airbnb Data (CSV)")
    dot.node("B", "Kamernet Data (JSON)")
    dot.node("C", "GeoJSON Data")
    dot.node("D", "Ingestion")
    dot.node("E", "Cleaning")
    dot.node("F", "Enrichment")
    dot.node("G", "Transformations")
    dot.node("H", "Processed Output")

    # Edges representing the flow between nodes
    dot.edges([("A", "D"), ("B", "D"), ("C", "F")])
    dot.edges([("D", "E"), ("E", "F"), ("F", "G"), ("G", "H")])

    # Save the diagram as a PNG file
    dot.render(output_path)
    print(f"Data flow diagram saved to {output_path}")


def generate_cicd_diagram(output_path="./docs/cicd_diagram.png"):
    """
    Generates a CI/CD pipeline diagram and saves it as a PNG file.

    Args:
        output_path (str): Path to save the generated CI/CD pipeline diagram.

    Diagram Details:
        - Starts with a developer commit.
        - Pre-commit hooks ensure code quality.
        - CI/CD pipeline runs unit tests, integration tests, and deployment.
    """
    dot = Digraph(comment="CI/CD Pipeline Diagram", format="png")

    # Nodes representing different stages of the CI/CD pipeline
    dot.node("A", "Developer Commit")
    dot.node("B", "Pre-Commit Hooks")
    dot.node("C", "CI/CD Pipeline")
    dot.node("D", "Unit Tests")
    dot.node("E", "Integration Tests")
    dot.node("F", "Deployment")

    # Edges representing the flow between nodes
    dot.edges([("A", "B"), ("B", "C"), ("C", "D"), ("D", "E"), ("E", "F")])

    # Save the diagram as a PNG file
    dot.render(output_path)
    print(f"CI/CD diagram saved to {output_path}")


# Generate both diagrams
generate_data_flow_diagram()
generate_cicd_diagram()
