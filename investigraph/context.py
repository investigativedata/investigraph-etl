from prefect.runtime import flow_run

from investigraph.model import Config, Context, Source
from investigraph.settings import DATA_ROOT
from investigraph.util import ensure_path


def init_context(config: Config, source: Source) -> Context:
    run_id = flow_run.get_id()
    if config.fragments_uri is None:
        path = ensure_path(DATA_ROOT / config.dataset / run_id)
        config.fragments_uri = (path / "fragments.json").as_uri()
    if config.entities_uri is None:
        path = ensure_path(DATA_ROOT / config.dataset / run_id)
        config.entities_uri = (path / "entities.ftm.json").as_uri()

    return Context(
        dataset=config.dataset,
        config=config,
        source=source,
        run_id=flow_run.get_id(),
    )
