"""Resume one owner assessment from its generation-bound native route selections."""

from uuid import UUID, uuid4

from . import storage
from .contract import RecoveryRequired

RULE_VERSION = 2


class OccupationReview:
    def __init__(self, root, group, public_id):
        if len(public_id) != 64 or any(c not in "0123456789abcdef" for c in public_id):
            raise RecoveryRequired("invalid occupation review identity")
        self.root = storage.path_at(root, ".occupation-reviews/" + public_id)
        self.path = storage.path_at(self.root, "selected.json")
        self.input_binding = {
            "public_id": public_id,
            "group": group["locator"],
            "decision": group["decision_generation"],
            "references": group["references"],
        }
        previous = storage.read_document(self.path) if self.path.exists() else None
        if previous and (
            previous.get("version") != 1 or previous.get("boundary") != "alpaca_dated_occupation_review"
        ):
            raise RecoveryRequired("unsupported dated occupation review journal")
        self.state = (
            previous
            if previous and previous["inputs"] == self.input_binding
            else {
                "version": 1,
                "boundary": "alpaca_dated_occupation_review",
                "inputs": self.input_binding,
                "routes": {},
            }
        )
        if self.state.get("rule_version") != RULE_VERSION:
            self.state.pop("outcome", None)
            self.state.pop("decision", None)
            self.state["rule_version"] = RULE_VERSION
        self.rows = {}

    def save(self):
        storage.atomic_json(self.path, self.state, compare=False)

    def observe(self, key, fetch):
        # Unique physical names keep a previous generation's selector coherent
        # across interruption between a new payload and its selector.

        if key in self.rows:
            return self.rows[key]
        if key in self.state["routes"]:
            name = self.state["routes"][key]
            try:
                prefix, token, extension = name.split(".")
                if prefix != "observation" or extension != "json" or UUID(hex=token).hex != token:
                    raise ValueError
            except (AttributeError, ValueError) as err:
                raise RecoveryRequired("invalid dated occupation observation selector") from err
            data = storage.read_document(storage.path_at(self.root, name, published=True))
        else:
            data = fetch()
            name = "observation." + uuid4().hex + ".json"
            storage.atomic_json(storage.path_at(self.root, name), data, compare=False)
            self.state["routes"][key] = name
            self.save()
        self.rows[key] = data
        return data

    def decide(self, infer):
        if "decision" not in self.state:
            self.state["decision"] = infer()
            self.save()
        return self.state["decision"]

    def finish(self, outcome):
        self.state["outcome"] = outcome
        self.save()
