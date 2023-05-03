from prefect.runtime import flow_run

from investigraph.model import Config, Context, Source


def init_context(config: Config, source: Source) -> Context:
    return Context(
        dataset=config.dataset,
        config=config,
        source=source,
        run_id=flow_run.get_id(),
    )
