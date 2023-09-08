from investigraph.logic.seed import handle
from investigraph.model import Config, Dataset, Source
from investigraph.model.context import BaseContext
from investigraph.model.stage import SeedStage


def test_seed(fixtures_path):
    dataset = Dataset(name="test")
    glob = str(fixtures_path) + "/*.json"
    seed = SeedStage(glob=glob)
    config = Config(dataset=dataset, seed=seed)
    ctx = BaseContext.from_config(config)
    res = [x for x in handle(ctx)]
    assert len(res) == 1
    assert isinstance(res[0], Source)

    # empty
    config = Config(dataset=dataset)
    ctx = BaseContext.from_config(config)
    res = [x for x in handle(ctx)]
    assert len(res) == 0
