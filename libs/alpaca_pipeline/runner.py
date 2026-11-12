"""Sequence the actual serialized stages under one continuous store gate."""

from .execution import controller
from .histories import produce_histories
from .prepare import prepare, request
from .serve import serve


def run(data, *, pricing_client=None, identity_client=None, progress=None):
    configuration = request(data)
    with controller(configuration["store"]):
        data = prepare(data, pricing_client=pricing_client, identity_client=identity_client, progress=progress)
        data = produce_histories(
            data, pricing_client=pricing_client, identity_client=identity_client, progress=progress
        )
        return serve(data, progress=progress)
