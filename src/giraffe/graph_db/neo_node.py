import json


class NeoNode:

    def __init__(self, string_representation: str):
        self.string_representation = string_representation

    @classmethod
    def from_json(cls, node_as_json: str):
        _ = json.loads(node_as_json, object_hook=NeoNode.decode_json_object)
        # TODO

    @classmethod
    def from_dict(cls, node_as_dict: dict):
        pass

    def validate(self):
        pass

    def __repr__(self):
        return self.string_representation

    @classmethod
    def decode_json_object(cls, dct):
        # TODO: handle special cases of date/time/geo
        pass
