from giraffe.exceptions.logical import ParseModelFailure
import giraffe.transformation.model_constants as mc


def get_vertex_label_by_id(model: dict, id: dict) -> dict:
    src_id_as_string = str(id)
    for vertex in model["vertices"]:
        trg_id = vertex["id"]
        if str(trg_id) == src_id_as_string:
            return vertex["label"]
    return None


def validate_has_attribute(model_part: dict, attribute: str, part_name) -> None:
    if attribute not in model_part:
        raise ParseModelFailure(f"expected attribute '{attribute}' not exist in {part_name}: {model_part}")


def get_edge_id(edge_model: dict):
    validate_has_attribute(edge_model, mc.ID)
    if isinstance(edge_model[mc.ID], list) and len(edge_model[mc.ID]) == 3:
        return edge_model[mc.ID][1]
    raise ParseModelFailure(f"edge has no valid id: {edge_model}")


def validate_vertex_properties(vertex_model):
    validate_has_attribute(vertex_model, mc.PROPERTIES)
    if isinstance(vertex_model[mc.PROPERTIES], dict):
        return
    raise ParseModelFailure(f"vertex properties should be dict")


def extract_identifier_as_vertex(edge_model: dict) -> dict:
    validate_has_attribute(edge_model, mc.VERTEX)
    validate_has_attribute(edge_model[mc.VERTEX], mc.LABEL)
    validate_has_attribute(edge_model[mc.VERTEX], mc.ID)
    identifier_as_vertex_model = {
        mc.LABEL: mc.IDENTIFIER,
        mc.PROPERTIES: {
            mc.LABEL: edge_model[mc.VERTEX][mc.LABEL],
            mc.ID: edge_model[mc.VERTEX][mc.ID]
        },
        mc.ID: {
            mc.FIELDS: [
                edge_model[mc.VERTEX][mc.LABEL],
                edge_model[mc.VERTEX][mc.ID]
            ],
            mc.DELIMITER: mc.DELIMITER_VALUE,
            mc.READONLY_FIELDS: []
        }
    }
    return identifier_as_vertex_model

def validate_array_property(array_property_model):
    if not isinstance(array_property_model, list):
        raise ParseModelFailure(f"array property - supports only '@<column>' values, got: {array_property_model}")